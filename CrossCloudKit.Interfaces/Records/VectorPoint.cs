// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Newtonsoft.Json.Linq;

namespace CrossCloudKit.Interfaces.Records;

/// <summary>
/// A point stored in a vector collection.
/// </summary>
/// <example>
/// <code>
/// var point = new VectorPoint
/// {
///     Id = "doc-42",
///     Vector = embedding,
///     Metadata = new JObject { ["title"] = "My Document", ["page"] = 3 }
/// };
/// await vectorService.UpsertAsync("documents", point);
/// </code>
/// </example>
public sealed record VectorPoint
{
    /// <summary>Unique string identifier for this point.</summary>
    public string Id { get; init; } = string.Empty;

    /// <summary>The embedding vector associated with this point.</summary>
    public float[] Vector { get; init; } = [];

    /// <summary>
    /// Arbitrary JSON metadata attached to this point.
    /// When <c>null</c> no payload is stored or returned.
    /// </summary>
    public JObject? Metadata { get; init; }
}
