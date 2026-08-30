using System.Collections.Concurrent;

namespace TheKrystalShip.KGSM.Cluster.Membership;

/// <summary>
/// What this member states about itself for other members to read — small, durable facts that are not
/// liveness and not addresses. The archetype is a public key: a member that signs something the others
/// verify has to hand them the key to verify it with, and it is the member itself that knows it.
/// </summary>
/// <remarks>
/// <para>
/// <b>Self-asserted, so the ordering is free.</b> A published fact travels with the member's card and its
/// gossip entry, and is adopted under the same rule as everything else that member says about itself: a
/// strictly higher incarnation wins. Nobody publishes on anybody else's behalf, so there is no conflict to
/// resolve and no new merge rule.
/// </para>
/// <para>
/// <b>Changing the set raises this member's incarnation, and that is what delivers it.</b> A fact is
/// carried by the self-entry, and an entry at an incarnation another member already holds supersedes
/// nothing — so without the raise a healthy member's first published values are the only ones it can ever
/// have. Rotating a key, correcting an address and withdrawing a fact all depend on it. Republishing an
/// identical value raises nothing, so a caller re-stating its facts on a timer costs no rounds.
/// </para>
/// <para>
/// <b>Trusted because the publisher holds the cluster secret, not because of who they claim to be.</b>
/// The secret buys attribution, not isolation — any member holding it can present itself as another — so a
/// published fact is exactly as trustworthy as anything else a member says. That is the accepted boundary
/// for a single-owner cluster, and it is the reason this carries facts a member is willing to have
/// forged by a peer that already holds the secret, rather than anything that would grant authority on its
/// own.
/// </para>
/// <para>
/// <b>Keep them small.</b> Every fact rides every gossip round with every other member's, so this is not a
/// place to put a document. <see cref="MaxValueBytes"/> is the cap, and a value over it is refused at the
/// point of publishing rather than silently truncated on the wire.
/// </para>
/// </remarks>
public sealed class SelfPublications(SelfIncarnation incarnation)
{
    /// <summary>The largest a single published value may be. Comfortably above any public key encoding and
    /// far below anything that would make a gossip round expensive.</summary>
    public const int MaxValueBytes = 8 * 1024;

    /// <summary>The most facts one member may publish, so a bug cannot grow a round without bound.</summary>
    public const int MaxFacts = 16;

    private readonly ConcurrentDictionary<string, string> _facts = new(StringComparer.Ordinal);

    /// <summary>
    /// State a fact about this member, replacing any previous value for the same key. A value that differs
    /// from the one already published raises this member's incarnation, so the next gossip round carries it
    /// to members already holding the old one instead of tying with them and being dropped.
    /// </summary>
    /// <exception cref="ArgumentException">The key is blank, the value exceeds
    /// <see cref="MaxValueBytes"/>, or this member already publishes <see cref="MaxFacts"/> other keys.</exception>
    public void Publish(string key, string value)
    {
        if (string.IsNullOrWhiteSpace(key))
            throw new ArgumentException("A published fact needs a key.", nameof(key));
        ArgumentNullException.ThrowIfNull(value);

        int size = System.Text.Encoding.UTF8.GetByteCount(value);
        if (size > MaxValueBytes)
        {
            throw new ArgumentException(
                $"'{key}' is {size} bytes; a published fact may be at most {MaxValueBytes}. Every fact rides " +
                "every gossip round, so this carries keys and identifiers, never documents.",
                nameof(value));
        }

        if (!_facts.ContainsKey(key) && _facts.Count >= MaxFacts)
        {
            throw new ArgumentException(
                $"This member already publishes {MaxFacts} facts, which is the cap.", nameof(key));
        }

        bool changed = !_facts.TryGetValue(key, out string? previous)
            || !string.Equals(previous, value, StringComparison.Ordinal);
        _facts[key] = value;
        if (changed)
            incarnation.Advance();
    }

    /// <summary>Stop stating a fact. Other members drop it when they next take this member's entry, which
    /// the raised incarnation is what makes them do.</summary>
    public void Withdraw(string key)
    {
        if (_facts.TryRemove(key, out _))
            incarnation.Advance();
    }

    /// <summary>Everything this member currently states about itself, as the wire carries it.</summary>
    public IReadOnlyDictionary<string, string> Current => new Dictionary<string, string>(_facts, StringComparer.Ordinal);
}
