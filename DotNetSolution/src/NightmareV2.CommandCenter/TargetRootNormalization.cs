namespace NightmareV2.CommandCenter;

internal static class TargetRootNormalization
{
    public static bool TryNormalize(string input, out string root)
    {
        root = input.Trim().TrimEnd('.').ToLowerInvariant();
        return !string.IsNullOrWhiteSpace(root);
    }

    public static IEnumerable<string> SplitLines(string text) =>
        text.Split(["\r\n", "\r", "\n"], StringSplitOptions.None);
}
