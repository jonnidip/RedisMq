namespace RedisMQ.Benchmarks;

internal static class Stats
{
    /// <summary>Nearest-rank percentile over a sorted copy: 0 for an empty sample.
    /// Percentile([1..100], 50) == 50 and Percentile([1..100], 99) == 99.</summary>
    public static double Percentile(IReadOnlyList<double> values, double percent)
    {
        if (values.Count == 0)
        {
            return 0;
        }

        double[] sorted = [.. values];
        Array.Sort(sorted);
        int rank = Math.Clamp(
            (int)Math.Ceiling(percent / 100.0 * sorted.Length) - 1,
            0,
            sorted.Length - 1);
        return sorted[rank];
    }
}
