namespace RedisMQ.Broker.Server;

/// <summary>
/// The per-server connection/channel/unacked counters shared by every
/// <see cref="WireConnection"/> and read by <see cref="AmqpBrokerServer.GetHealth"/>.
/// Connection and channel counts use <see cref="Interlocked"/>; unacked counts live
/// behind one private lock and entries disappear at zero, so a deleted queue can never
/// leave a stale series behind (health also prunes against the live queue snapshot).
/// </summary>
internal sealed class BrokerRuntimeState
{
    private readonly object _unackedLock = new();
    private readonly Dictionary<(string Vhost, string Queue), long> _unacked = [];
    private int _activeConnections;
    private int _activeChannels;

    internal int ActiveConnections => Volatile.Read(ref _activeConnections);

    internal int ActiveChannels => Volatile.Read(ref _activeChannels);

    internal void ConnectionStarted() => Interlocked.Increment(ref _activeConnections);

    internal void ConnectionClosed() => Interlocked.Decrement(ref _activeConnections);

    internal void ChannelOpened() => Interlocked.Increment(ref _activeChannels);

    internal void ChannelClosed() => Interlocked.Decrement(ref _activeChannels);

    internal void DeliveryClaimed(string vhost, string queue)
    {
        lock (_unackedLock)
        {
            (string Vhost, string Queue) key = (vhost, queue);
            _unacked.TryGetValue(key, out long count);
            _unacked[key] = count + 1;
        }
    }

    internal void DeliverySettled(string vhost, string queue)
    {
        lock (_unackedLock)
        {
            (string Vhost, string Queue) key = (vhost, queue);
            if (!_unacked.TryGetValue(key, out long count))
            {
                return;
            }

            if (count <= 1)
            {
                _unacked.Remove(key);
            }
            else
            {
                _unacked[key] = count - 1;
            }
        }
    }

    internal IReadOnlyList<KeyValuePair<(string Vhost, string Queue), long>> UnackedSnapshot()
    {
        lock (_unackedLock)
        {
            return [.. _unacked];
        }
    }

    internal void PruneUnacked(IReadOnlySet<(string Vhost, string Queue)> liveQueues)
    {
        lock (_unackedLock)
        {
            foreach ((string Vhost, string Queue) key in _unacked.Keys.ToArray())
            {
                if (!liveQueues.Contains(key))
                {
                    _unacked.Remove(key);
                }
            }
        }
    }
}
