using Microsoft.Extensions.Configuration;

namespace TheKrystalShip.KGSM.Cluster;

/// <summary>
/// Reads the cluster secret from configuration, under key names this package owns.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why the package owns the names.</b> The secret is one value every member of a cluster must hold
/// identically, and on a machine running more than one member it comes from one shared file that each
/// member's unit loads before its own. Two members spelling the key differently would each read a blank
/// from the other's file and conclude, separately and quietly, that they are not clustered.
/// </para>
/// <para>
/// Read by explicit key rather than bound onto a type: configuration binding reflects over the target,
/// which no Native-AOT member can do.
/// </para>
/// </remarks>
public static class ClusterConfiguration
{
    /// <summary>The configuration section every cluster key sits under. Spelled with a double underscore
    /// in the environment: <c>Cluster__Secret</c>.</summary>
    public const string Section = "Cluster";

    /// <summary>The shared cluster secret's key.</summary>
    public const string SecretKey = Section + ":Secret";

    /// <summary>The previous secret's key, accepted alongside the current one during a rotation overlap.</summary>
    public const string SecretPreviousKey = Section + ":SecretPrevious";

    /// <summary>
    /// The shared cluster secret, or an empty string when this member holds none — which is a member
    /// that is simply not part of a cluster, not a misconfiguration.
    /// </summary>
    public static string Secret(IConfiguration configuration) => Read(configuration, SecretKey);

    /// <summary>
    /// The previous cluster secret, accepted alongside the current one while a rotation is rolling
    /// through the members. Empty when no rotation is in progress.
    /// </summary>
    public static string SecretPrevious(IConfiguration configuration) => Read(configuration, SecretPreviousKey);

    private static string Read(IConfiguration configuration, string key)
    {
        string? value = configuration[key];
        return string.IsNullOrWhiteSpace(value) ? "" : value.Trim();
    }
}
