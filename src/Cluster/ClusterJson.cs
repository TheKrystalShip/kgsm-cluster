using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using TheKrystalShip.KGSM.Cluster.Membership;
using TheKrystalShip.KGSM.Cluster.Messaging;

namespace TheKrystalShip.KGSM.Cluster;

/// <summary>
/// The one place the member-to-member wire shape is configured: camelCase names and ISO-8601 UTC
/// <c>Z</c> timestamps, serialized through a source-generated context so an AOT member carries no
/// reflection. A payload written by one member must round-trip identically on every other, so both
/// the sender's outbox and the receiver's inbox read these same options.
/// </summary>
public static class ClusterJson
{
    /// <summary>The serializer options every cluster wire path uses.</summary>
    public static JsonSerializerOptions Options { get; } = Build();

    private static JsonSerializerOptions Build()
    {
        var options = new JsonSerializerOptions(ClusterJsonContext.Default.Options);
        options.Converters.Add(new Iso8601UtcDateTimeOffsetConverter());
        return options;
    }
}

/// <summary>
/// Writes a <see cref="DateTimeOffset"/> as ISO-8601 in UTC with a <c>Z</c> suffix. The serializer's
/// own default emits the local-offset form, which is valid ISO-8601 but a different shape on the wire;
/// this pins the one shape. Reads accept either form.
/// </summary>
public sealed class Iso8601UtcDateTimeOffsetConverter : JsonConverter<DateTimeOffset>
{
    public override DateTimeOffset Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        => reader.GetDateTimeOffset();

    public override void Write(Utf8JsonWriter writer, DateTimeOffset value, JsonSerializerOptions options)
        // UtcDateTime has Kind=Utc, so the "O" round-trip format ends in 'Z' rather than the degenerate
        // ".Z" a hand-rolled ".FFFZ" mask produces on a whole second.
        => writer.WriteStringValue(value.ToUniversalTime().UtcDateTime.ToString("O", CultureInfo.InvariantCulture));
}

/// <summary>
/// The source-generated serializer context for every shape that crosses the wire between members.
/// A type deserialized by this package must be registered here or it throws at runtime: there is no
/// reflection fallback, which is what lets an AOT member embed the package.
/// </summary>
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(ClusterEnvelope))]
[JsonSerializable(typeof(MemberCard))]
[JsonSerializable(typeof(MemberCandidate))]
[JsonSerializable(typeof(List<MemberCandidate>))]
[JsonSerializable(typeof(IntroduceExchange))]
[JsonSerializable(typeof(SyncRequest))]
[JsonSerializable(typeof(SyncResponse))]
[JsonSerializable(typeof(InboxAck))]
[JsonSerializable(typeof(ClusterError))]
[JsonSerializable(typeof(JsonElement))]
public sealed partial class ClusterJsonContext : JsonSerializerContext;

/// <summary>The inbox's success body. Every accepted envelope answers with this, whether it was freshly
/// applied, a de-duplicated replay, or an unknown type that was dropped.</summary>
public sealed record InboxAck(string Status);

/// <summary>
/// The error body every cluster endpoint answers a non-2xx with, matching the frozen
/// <c>{error:{code,message,details?}}</c> envelope the rest of the ecosystem's HTTP surfaces use, so a
/// member hosting these endpoints beside its own does not serve two error shapes.
/// </summary>
public sealed record ClusterError(ClusterErrorBody Error)
{
    public static ClusterError Of(string code, string message, ClusterErrorDetails? details = null)
        => new(new ClusterErrorBody(code, message, details));
}

/// <inheritdoc cref="ClusterError"/>
public sealed record ClusterErrorBody(
    string Code,
    string Message,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] ClusterErrorDetails? Details = null);

/// <summary>
/// What a refusal needs to say beyond naming itself. A member told only that its versions do not match
/// knows something is wrong and nothing about what to change, so the two values that disagreed travel with
/// the refusal.
/// </summary>
/// <param name="Remote">The refusing member's own value.</param>
/// <param name="Local">The value it was given.</param>
public sealed record ClusterErrorDetails(
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] string? Remote = null,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] string? Local = null);
