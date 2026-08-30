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

    /// <summary>
    /// The candidates fit to tell another member about — everything except a loopback address.
    /// </summary>
    /// <remarks>
    /// <b>A loopback address means "me" to whoever reads it.</b> Told to a member on another machine it
    /// does not fail: it connects to whatever is on that machine's own port, which in a cluster running
    /// the same components is plausibly a different member of the same kind. The result is a member
    /// talking to itself while believing it reached somebody else, and nothing errors — which is worse
    /// than an address that simply does not answer.
    /// <para>
    /// It stays in the store and stays usable. Two members on one machine reach each other over loopback
    /// and that is a real topology, so the address is kept and the local poller still walks it. What it
    /// is not is something to advertise, because it is true only for whoever already holds it. That
    /// applies equally to a member's own address and to a candidate it holds for somebody else: gossip
    /// carries a member's whole roster, so an unfiltered loopback pinned for a neighbour reaches every
    /// member in the cluster.
    /// </para>
    /// <para>
    /// The consequence, stated because it is a real limit rather than an oversight: a member reachable
    /// <em>only</em> over loopback cannot be learned from the mesh at all. It is reachable by whoever was
    /// told directly, and if that row is ever reaped the address has to be given again. Nothing on the
    /// wire can express "the loopback of the machine we happen to share".
    /// </para>
    /// </remarks>
    public static IReadOnlyList<MemberCandidate> Advertisable(IEnumerable<MemberCandidate>? candidates)
        => [.. (candidates ?? []).Where(c => !IsLoopback(c.Url))];

    /// <summary>Whether an address points back at whoever reads it.</summary>
    private static bool IsLoopback(string? url)
    {
        if (!Uri.TryCreate(url, UriKind.Absolute, out Uri? uri)) return false;
        if (string.Equals(uri.Host, "localhost", StringComparison.OrdinalIgnoreCase)) return true;
        return System.Net.IPAddress.TryParse(uri.Host, out System.Net.IPAddress? ip)
               && System.Net.IPAddress.IsLoopback(ip);
    }

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
