namespace Similarweb.LinqToDB.Firebolt.Tests.Options;

public class FireboltCoreSettings
{
    /// <summary>
    /// Docker image used by Testcontainers when <see cref="ExternalUrl"/> is not set.
    /// </summary>
    public required string Image { get; init; }

    /// <summary>
    /// Container HTTP query port (mapped to a random host port).
    /// </summary>
    public ushort HttpPort { get; init; } = 3473;

    /// <summary>
    /// Secondary port exposed by Firebolt Core (mapped to a random host port).
    /// </summary>
    public ushort SecondaryPort { get; init; } = 8122;

    /// <summary>
    /// When set, skip Testcontainers and use this pre-started (and pre-seeded) Core URL.
    /// </summary>
    public string? ExternalUrl { get; init; }
}
