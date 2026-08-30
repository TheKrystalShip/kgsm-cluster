using System.Text.Json;

namespace TheKrystalShip.KGSM.Cluster.Membership;

/// <summary>
/// Reads and writes <see cref="MemberRow.Published"/> — what a member states about itself, stored as it
/// arrives and never interpreted here. What any given key means belongs to whoever publishes it.
/// </summary>
public static class PublishedFacts
{
    /// <summary>An empty set. A member that states nothing is the normal case.</summary>
    public static readonly IReadOnlyDictionary<string, string> None =
        new Dictionary<string, string>(StringComparer.Ordinal);

    /// <summary>Decode a stored set. A blank or unparseable value reads as empty: a member that has stated
    /// nothing and a member whose statement did not survive are the same to a reader, and neither is an
    /// error worth failing a gossip round over.</summary>
    public static IReadOnlyDictionary<string, string> Decode(string? stored)
    {
        if (string.IsNullOrWhiteSpace(stored)) return None;
        try
        {
            return JsonSerializer.Deserialize(stored, ClusterJsonContext.Default.DictionaryStringString)
                   ?? None;
        }
        catch (JsonException)
        {
            return None;
        }
    }

    /// <summary>Encode a set for storage, dropping anything oversized so one member's bug cannot make
    /// another member's roster unreadable.</summary>
    public static string Encode(IReadOnlyDictionary<string, string>? facts)
    {
        if (facts is null || facts.Count == 0) return "";
        var kept = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach ((string key, string value) in facts)
        {
            if (string.IsNullOrWhiteSpace(key) || value is null) continue;
            if (System.Text.Encoding.UTF8.GetByteCount(value) > SelfPublications.MaxValueBytes) continue;
            if (kept.Count >= SelfPublications.MaxFacts) break;
            kept[key] = value;
        }
        return kept.Count == 0 ? "" : JsonSerializer.Serialize(kept, ClusterJsonContext.Default.DictionaryStringString);
    }

    /// <summary>One fact a member states, or <see langword="null"/> when it states nothing under that key.</summary>
    public static string? Read(this MemberRow row, string key) =>
        Decode(row.Published).TryGetValue(key, out string? value) ? value : null;
}
