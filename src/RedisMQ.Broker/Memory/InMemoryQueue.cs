namespace RedisMQ.Broker.Memory;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

/// <summary>
/// The per-queue delivery state M0 keeps next to each declared queue: the ready buffer
/// (FIFO; exactly what the publish path appended or a requeue returned), the consumer
/// registry in registration order, the round-robin cursor, and the single wake-driven
/// delivery pump task. The pump runs OUTSIDE the engine's structure lock whenever it
/// awaits — it only takes the gate briefly around each claim — so the lock is never
/// held across a frame write, and it exits when nothing is deliverable (consumers
/// empty, queue empty, or every consumer's channel gated) and is re-armed by the next
/// wake (enqueue, requeue, register, credit-freeing nudge).
/// </summary>
internal sealed class InMemoryQueue
{
    private readonly object _gate;
    private readonly List<InMemoryMessage> _ready = [];
    private readonly List<IQueueConsumer> _consumers = []; // registration order
    private int _rrIndex;
    private bool _pumpRunning;
    private bool _pumpWake;

    internal InMemoryQueue(object gate)
    {
        _gate = gate;
    }

    /// <summary>Stored messages visible to <see cref="PeekQueue"/>-style callers
    /// (claimed/unacked messages are NOT here — the pump removed them at admit).</summary>
    internal int ReadyCount => _ready.Count;

    internal bool HasConsumers => _consumers.Count > 0;

    /// <summary>Whether any consumer currently registered claimed the queue exclusively.</summary>
    internal bool HasExclusiveConsumer
    {
        get
        {
            foreach (IQueueConsumer consumer in _consumers)
            {
                if (consumer.IsExclusive)
                {
                    return true;
                }
            }

            return false;
        }
    }

    /// <summary>Appends one message to the ready tail (caller holds the engine gate).</summary>
    internal void AppendAlreadyLocked(InMemoryMessage message) => _ready.Add(message);

    internal void SnapshotReadyAlreadyLocked(List<InMemoryMessage> into)
    {
        foreach (InMemoryMessage message in _ready)
        {
            into.Add(message);
        }
    }

    internal bool TryPopReadyAlreadyLocked(out InMemoryMessage message)
    {
        if (_ready.Count == 0)
        {
            message = null!;
            return false;
        }

        message = _ready[0];
        _ready.RemoveAt(0);
        return true;
    }

    /// <summary>Removes expired messages from the ready buffer. Messages whose
    /// <see cref="InMemoryMessage.Expiration"/> is in the past are dropped.</summary>
    internal void RemoveExpiredMessages(DateTimeOffset now)
    {
        for (int i = _ready.Count - 1; i >= 0; i--)
        {
            if (_ready[i].Expiration is DateTimeOffset exp && exp <= now)
            {
                _ready.RemoveAt(i);
            }
        }
    }

    /// <summary>Registers a consumer at the registry tail (registration order = the
    /// round-robin cycle) and arms the pump. Caller holds the engine gate.</summary>
    internal void RegisterConsumerAlreadyLocked(IQueueConsumer consumer)
    {
        _consumers.Add(consumer);
        WakeAlreadyLocked();
    }

    /// <summary>Removes a consumer by tag; unknown tags are silent. A cursor left out
    /// of bounds is wrapped so the cycle stays deterministic. Caller holds the gate.</summary>
    internal void UnregisterConsumerAlreadyLocked(string consumerTag)
    {
        int index = _consumers.FindIndex(consumer => consumer.ConsumerTag == consumerTag);
        if (index < 0)
        {
            return;
        }

        _consumers.RemoveAt(index);
        if (_rrIndex >= _consumers.Count)
        {
            _rrIndex = 0;
        }

        WakeAlreadyLocked();
    }

    /// <summary>Arms (respawns) the pump when there is deliverable work and no pump is
    /// running; coalesces wakes through <c>_pumpWake</c> while one is. Caller holds
    /// the engine gate — no work is done inside the pump's claims except claiming.</summary>
    internal void WakeAlreadyLocked()
    {
        if (_ready.Count == 0 || _consumers.Count == 0)
        {
            return; // nothing deliverable: an idle exit needs no wake
        }

        if (_pumpRunning)
        {
            _pumpWake = true; // the running pump re-checks under the gate before exiting
            return;
        }

        _pumpRunning = true;
        _pumpWake = false;
        _ = Task.Run(PumpLoopAsync);
    }

    /// <summary>
    /// The delivery loop: claim one ready message for an un-gated consumer under the
    /// gate, flush that consumer's channel writer chain OUTSIDE the gate, repeat.
    /// Every exit (and transport-death crash exit) clears <c>_pumpRunning</c> under
    /// the gate so a later wake respawns a fresh pump.
    /// </summary>
    private async Task PumpLoopAsync()
    {
        try
        {
            while (true)
            {
                IQueueConsumer? admitted = null;
                bool wokeAgain = false;
                lock (_gate)
                {
                    admitted = TryClaimOneAlreadyLocked();
                    if (admitted is null)
                    {
                        if (_pumpWake)
                        {
                            _pumpWake = false;
                            wokeAgain = true;
                        }
                        else
                        {
                            _pumpRunning = false;
                            return;
                        }
                    }
                }

                if (wokeAgain)
                {
                    continue;
                }

                // Never hold the engine gate across an await (frame writes included).
                await admitted!.DrainDeliveryWritesAsync();
            }
        }
        catch (Exception)
        {
            // The transport died or the connection was cancelled mid-drain. The pump
            // stops quietly; the bookkeeping stays consistent for a later respawn.
            lock (_gate)
            {
                _pumpRunning = false;
                _pumpWake = false;
            }
        }
    }

    /// <summary>One claim pass: round-robin from the cursor over the registered
    /// consumers, handing the head message to the first consumer that admits it
    /// (credit-gated channels refuse). The message stays at the head when every
    /// consumer is gated. Caller holds the engine gate; the claim — including the
    /// consumer's channel bookkeeping and its writer-job scheduling — completes
    /// synchronously.</summary>
    private IQueueConsumer? TryClaimOneAlreadyLocked()
    {
        if (_ready.Count == 0 || _consumers.Count == 0)
        {
            return null;
        }

        InMemoryMessage message = _ready[0];
        for (int attempt = 0; attempt < _consumers.Count; attempt++)
        {
            int index = (_rrIndex + attempt) % _consumers.Count;
            IQueueConsumer consumer = _consumers[index];
            if (consumer.TryAdmitForDelivery(message))
            {
                _ready.RemoveAt(0);
                // The cycle pointer is the POSITION after the claimer, not modulo
                // today's registry size: a claim made while only one consumer was
                // registered must still round-robin to the consumer that joins
                // after it (registration order), so the modulo happens at use.
                _rrIndex = index + 1;
                return consumer;
            }
        }

        return null;
    }
}
