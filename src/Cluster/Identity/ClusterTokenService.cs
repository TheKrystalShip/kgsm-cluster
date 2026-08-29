using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;

namespace TheKrystalShip.KGSM.Cluster.Identity;

/// <summary>
/// HMAC-SHA256 member service tokens. The signing key is derived from the cluster secret with SHA-256,
/// so a secret of any length works, and it is deliberately independent of whatever key a member signs
/// its own users' sessions with: one proves membership of a cluster, the other proves who a person is.
/// <para>
/// A blank secret means this member is simply not part of a cluster. <see cref="Mint"/> throws and
/// <see cref="ValidateAsync"/> always answers <see langword="null"/> — the unconfigured-by-default
/// posture, not a misconfiguration, which is why construction logs at information level.
/// </para>
/// </summary>
public sealed class ClusterTokenService : IClusterTokenService
{
    /// <summary>Every member token carries this audience and validation rejects any other. Fixed,
    /// unlike the token's issuer.</summary>
    private const string ClusterAudience = "cluster";

    /// <summary>Deliberately short. A service token is minted fresh per outbound call and never cached
    /// across requests, so a tight lifetime bounds a leaked one to a minute.</summary>
    private static readonly TimeSpan ServiceTokenTtl = TimeSpan.FromSeconds(60);

    private readonly string _memberId;
    private readonly bool _enabled;
    private readonly SigningCredentials? _signing;
    private readonly TokenValidationParameters? _currentValidation;
    private readonly TokenValidationParameters? _previousValidation;
    private readonly JsonWebTokenHandler _handler = new();

    public ClusterTokenService(ClusterOptions options, ILogger<ClusterTokenService> logger)
    {
        _memberId = options.MemberId;
        _enabled = options.Enabled;

        if (!_enabled)
        {
            logger.LogInformation(
                "No cluster secret is set — this member is not part of a cluster. The service-token seam " +
                "stays dormant.");
            return;
        }

        SymmetricSecurityKey currentKey = DeriveKey(options.Secret);
        _signing = new SigningCredentials(currentKey, SecurityAlgorithms.HmacSha256);
        _currentValidation = BuildValidationParameters(currentKey);

        bool rotating = !string.IsNullOrWhiteSpace(options.SecretPrevious);
        if (rotating)
            _previousValidation = BuildValidationParameters(DeriveKey(options.SecretPrevious));

        logger.LogInformation(
            "Cluster enabled as member {MemberId}{Rotating}.",
            _memberId, rotating ? " (accepting the previous secret during a rotation overlap window)" : "");
    }

    /// <inheritdoc/>
    public MintedClusterToken Mint()
    {
        if (!_enabled || _signing is null)
            throw new InvalidOperationException(
                "This member is not clustered (no cluster secret) — cannot mint a service token.");

        DateTime expires = DateTime.UtcNow.Add(ServiceTokenTtl);
        var descriptor = new SecurityTokenDescriptor
        {
            // The issuer is this member's id rather than a fixed string: the receiving member reads it
            // back to learn who is calling.
            Issuer = _memberId,
            Audience = ClusterAudience,
            Subject = new ClaimsIdentity([new Claim("sub", $"member:{_memberId}")]),
            Expires = expires,
            IssuedAt = DateTime.UtcNow,
            SigningCredentials = _signing,
        };
        return new MintedClusterToken(_handler.CreateToken(descriptor), new DateTimeOffset(expires, TimeSpan.Zero));
    }

    /// <inheritdoc/>
    public async Task<ClusterPrincipal?> ValidateAsync(string token)
    {
        if (!_enabled || _currentValidation is null)
            return null;

        ClusterPrincipal? principal = await TryValidateAsync(token, _currentValidation).ConfigureAwait(false);
        if (principal is not null)
            return principal;

        // During a rotation overlap a member that has not rolled to the new secret still signs with the
        // previous one, so accept that too until every member has rotated.
        if (_previousValidation is not null)
            principal = await TryValidateAsync(token, _previousValidation).ConfigureAwait(false);

        return principal;
    }

    private async Task<ClusterPrincipal?> TryValidateAsync(string token, TokenValidationParameters parameters)
    {
        // Fail-closed: any exception collapses to null rather than propagating. A bad token must never
        // crash the caller into an ambiguous state.
        try
        {
            TokenValidationResult result = await _handler.ValidateTokenAsync(token, parameters).ConfigureAwait(false);
            if (!result.IsValid || result.SecurityToken is not JsonWebToken jwt)
                return null;

            // Read the issuer off the parsed token rather than searching the resulting claims: this is
            // robust regardless of inbound claim-type mapping, and it is the member id the mint side set.
            string memberId = jwt.Issuer;
            return string.IsNullOrWhiteSpace(memberId) ? null : new ClusterPrincipal(memberId);
        }
        catch
        {
            return null;
        }
    }

    private static SymmetricSecurityKey DeriveKey(string secret)
        => new(SHA256.HashData(Encoding.UTF8.GetBytes(secret)));

    // Issuer validation is deliberately off: a member token's issuer IS the caller's member id, so
    // there is no single valid issuer to check it against. It is read back after validation instead.
    private static TokenValidationParameters BuildValidationParameters(SymmetricSecurityKey key) => new()
    {
        ValidateIssuer = false,
        ValidateAudience = true,
        ValidAudience = ClusterAudience,
        ValidateIssuerSigningKey = true,
        IssuerSigningKey = key,
        ValidateLifetime = true,
        ClockSkew = TimeSpan.FromSeconds(30),
    };
}
