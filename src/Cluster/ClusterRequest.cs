using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using TheKrystalShip.KGSM.Cluster.Identity;

namespace TheKrystalShip.KGSM.Cluster;

/// <summary>
/// Authenticating a member-to-member request, for a member serving a route of its own beside the ones
/// this package maps.
/// </summary>
/// <remarks>
/// A member with its own member-to-member surface — an account snapshot, a resource read — has to make
/// exactly the checks the package's own endpoints make: a valid service token, and a caller the roster
/// has not disabled. Two implementations of that is two members able to disagree about what the protocol
/// is, which is the drift this package exists to remove. So there is one, and it is public.
/// </remarks>
public static class ClusterRequest
{
    /// <summary>
    /// Validate the caller's service token and check the member gate.
    /// </summary>
    /// <returns>
    /// The calling member when both pass, and <see langword="null"/> when either does not — in which case
    /// <b>the refusal has already been written to the response</b> and the handler must return without
    /// writing anything further. The refusal is the same status and the same code the package's own
    /// endpoints answer with, which is the point of calling this rather than repeating it.
    /// </returns>
    public static async Task<ClusterPrincipal?> AuthenticateAsync(HttpContext context)
    {
        IServiceProvider services = context.RequestServices;
        var tokens = services.GetRequiredService<IClusterTokenService>();
        var gate = services.GetRequiredService<IClusterMemberGate>();

        string? token = ExtractBearerToken(context.Request);
        if (token is null)
        {
            await ClusterResponse.ErrorAsync(context, StatusCodes.Status401Unauthorized,
                "invalid_cluster_token", "missing bearer token").ConfigureAwait(false);
            return null;
        }

        ClusterPrincipal? principal = await tokens.ValidateAsync(token).ConfigureAwait(false);
        if (principal is null)
        {
            await ClusterResponse.ErrorAsync(context, StatusCodes.Status401Unauthorized,
                "invalid_cluster_token", "invalid, expired, or unsigned cluster service token")
                .ConfigureAwait(false);
            return null;
        }

        if (!await gate.IsEnabledAsync(principal.MemberId).ConfigureAwait(false))
        {
            await ClusterResponse.ErrorAsync(context, StatusCodes.Status403Forbidden, "member_disabled",
                $"member \'{principal.MemberId}\' is not an enabled member of this cluster")
                .ConfigureAwait(false);
            return null;
        }

        return principal;
    }

    /// <summary>A bearer token from the Authorization header, or <see langword="null"/> when absent or not
    /// a bearer. A member-to-member call has no other credential form.</summary>
    public static string? ExtractBearerToken(HttpRequest request)
    {
        string? header = request.Headers.Authorization.ToString();
        if (string.IsNullOrWhiteSpace(header)) return null;
        const string prefix = "Bearer ";
        return header.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)
            ? header[prefix.Length..].Trim()
            : null;
    }
}

/// <summary>Writing the refusal shape every cluster route answers with.</summary>
public static class ClusterResponse
{
    /// <summary>Write the frozen error envelope with a status and a code.</summary>
    public static async Task ErrorAsync(
        HttpContext context, int status, string code, string message, ClusterErrorDetails? details = null)
    {
        context.Response.StatusCode = status;
        context.Response.ContentType = "application/json; charset=utf-8";
        await System.Text.Json.JsonSerializer.SerializeAsync(
            context.Response.Body, ClusterError.Of(code, message, details),
            ClusterJsonContext.Default.ClusterError, context.RequestAborted).ConfigureAwait(false);
    }
}
