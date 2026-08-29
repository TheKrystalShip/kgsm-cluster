using System.Globalization;
using Microsoft.Data.Sqlite;

namespace TheKrystalShip.KGSM.Cluster.Storage;

/// <summary>
/// Reads and writes the two column shapes the package's schema uses that SQLite has no native type
/// for. Timestamps are ISO-8601 UTC strings so they sort chronologically as text; nullable columns
/// come back as <see cref="DBNull"/> and are read through the nullable overloads.
/// </summary>
internal static class SqliteValues
{
    /// <summary>Format a timestamp for storage: UTC, round-trip "O", which sorts lexically.</summary>
    public static string Stamp(DateTimeOffset value)
        => value.ToUniversalTime().UtcDateTime.ToString("O", CultureInfo.InvariantCulture);

    /// <summary>Read a non-null timestamp column.</summary>
    public static DateTimeOffset ReadStamp(SqliteDataReader reader, int ordinal)
        => DateTimeOffset.Parse(
            reader.GetString(ordinal), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);

    /// <summary>Read a nullable timestamp column.</summary>
    public static DateTimeOffset? ReadStampOrNull(SqliteDataReader reader, int ordinal)
        => reader.IsDBNull(ordinal) ? null : ReadStamp(reader, ordinal);

    /// <summary>Read a nullable text column.</summary>
    public static string? ReadStringOrNull(SqliteDataReader reader, int ordinal)
        => reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

    /// <summary>Bind a parameter, mapping a null to <see cref="DBNull"/>.</summary>
    public static void Bind(SqliteCommand command, string name, object? value)
        => command.Parameters.AddWithValue(name, value ?? DBNull.Value);
}
