using System.Security.Cryptography;
using System.Text;

namespace TheKrystalShip.KGSM.Cluster;

/// <summary>
/// Whether this machine founded the cluster a member is in, and the fingerprint a secret is recognised
/// by.
/// </summary>
/// <remarks>
/// <para>
/// A machine whose secret was blank at first install founds a cluster of its own, and its install
/// records the fingerprint of the secret it generated in <see cref="DefaultPath"/>. Taking another
/// cluster's secret leaves that record naming a secret the machine no longer holds, so "founded here"
/// is a comparison made every time rather than the file's presence: it is true only while the machine
/// still holds the secret it founded with.
/// </para>
/// <para>
/// The fingerprint is the SHA-256 of the secret and a newline, in lowercase hex — what
/// <c>printf '%s\n' "$secret" | sha256sum</c> prints, which is how the install writes it.
/// </para>
/// </remarks>
public static class ClusterFounding
{
    /// <summary>Where a machine's founding record lives.</summary>
    public const string DefaultPath = "/etc/kgsm/cluster-founded";

    /// <summary>The fingerprint a secret is recorded and recognised by.</summary>
    public static string Fingerprint(string secret) =>
        Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(secret.Trim() + "\n")));

    /// <summary>
    /// Whether the machine this member runs on founded the cluster it is in: its founding record names
    /// the secret the member holds.
    /// </summary>
    /// <remarks>
    /// False for a member in no cluster, for a machine with no record, and for one whose record cannot
    /// be read. Each of those is a machine that did not found this cluster as far as anything here can
    /// prove, and every caller acts on "founded here" by taking something a joining machine must not.
    /// </remarks>
    public static bool IsFoundedHere(ClusterOptions options)
    {
        if (!options.Enabled || string.IsNullOrWhiteSpace(options.FoundedPath))
            return false;

        try
        {
            string recorded = File.ReadAllText(options.FoundedPath).Trim();
            return string.Equals(recorded, Fingerprint(options.Secret), StringComparison.OrdinalIgnoreCase);
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException)
        {
            return false;
        }
    }
}
