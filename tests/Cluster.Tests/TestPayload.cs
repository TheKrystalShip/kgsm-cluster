using System.Text.Json.Serialization;

namespace TheKrystalShip.KGSM.Cluster.Tests;

/// <summary>A caller-owned payload shape, with its own source-generated metadata — the shape the bus's
/// typed overload expects a member to bring.</summary>
public sealed record TestPayload(string Scope, int Count);

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(TestPayload))]
public sealed partial class TestPayloadContext : JsonSerializerContext;
