using System.Text.Json;

namespace TheKrystalShip.KGSM.Cluster.Membership;

/// <summary>
/// Reads and writes <see cref="MemberRow.Candidates"/> — the encoded list of addresses a member says it
/// answers at — and picks which of them this member calls.
/// </summary>
public static class MemberCandidates
{
    /// <summary>Decode a stored list. A blank or unparseable value reads as empty: a row written before the
    /// member offered any address is a gap, not a failure.</summary>
    public static IReadOnlyList<MemberCandidate> Decode(string? stored)
    {
        if (string.IsNullOrWhiteSpace(stored)) return [];
        try
        {
            return JsonSerializer.Deserialize(stored, ClusterJsonContext.Default.ListMemberCandidate) ?? [];
        }
        catch (JsonException)
        {
            return [];
        }
    }

    /// <summary>Encode a list for storage, dropping anything that is not an absolute <c>http(s)</c> address
    /// and de-duplicating by normalised URL while keeping the offered order.</summary>
    public static string Encode(IEnumerable<MemberCandidate>? candidates)
    {
        if (candidates is null) return "";
        var seen = new Dictionary<string, MemberCandidate>(StringComparer.OrdinalIgnoreCase);
        foreach (MemberCandidate candidate in candidates)
        {
            string? url = SelfIdentityStore.Normalize(candidate.Url);
            if (url is null || seen.ContainsKey(url)) continue;
            seen[url] = new MemberCandidate(url, candidate.Client);
        }
        return seen.Count == 0
            ? ""
            : JsonSerializer.Serialize(seen.Values.ToList(), ClusterJsonContext.Default.ListMemberCandidate);
    }

    /// <summary>
    /// Merge a freshly-offered list into what is already stored: the offer leads, because a member is the
    /// authority on its own addresses, and anything previously known that the offer omits is kept behind it
    /// — so an address this member has proven works is not dropped because one gossip round arrived with a
    /// shorter list.
    /// </summary>
    public static string Merge(string? stored, IEnumerable<MemberCandidate>? offered) =>
        Encode([.. offered ?? [], .. Decode(stored)]);

    /// <summary>The address to call a member on: the first candidate a browser can also use, else the first
    /// of any kind, else empty. Member-to-member accepts either, since an address no browser can use is
    /// still an address.</summary>
    public static string Best(IReadOnlyList<MemberCandidate> candidates) =>
        candidates.Count == 0 ? "" : (candidates.FirstOrDefault(c => c.Client) ?? candidates[0]).Url;

    /// <summary>The address a browser is given for a member: the first candidate a browser can use. Empty
    /// when the member offers none — the gap is reported rather than handing a browser an address it cannot
    /// reach.</summary>
    public static string ClientUrl(IReadOnlyList<MemberCandidate> candidates) =>
        candidates.FirstOrDefault(c => c.Client)?.Url ?? "";
}
