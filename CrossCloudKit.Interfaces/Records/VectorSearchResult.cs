// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Newtonsoft.Json.Linq;

namespace CrossCloudKit.Interfaces.Records;

/// <summary>
/// A single result returned by a vector similarity search.
/// </summary>
/// <example>
/// <code>
/// var results = await vectorService.QueryAsync("docs", queryVector, topK: 5);
/// if (results.IsSuccessful)
///     foreach (var r in results.Data)
///         Console.WriteLine($"ID={r.Id}, Score={r.Score}, Title={r.Metadata?["title"]}");
/// </code>
/// </example>
public sealed record VectorSearchResult
{
    /// <summary>The identifier of the matching point.</summary>
    public string Id { get; init; } = string.Empty;

    /// <summary>
    /// Similarity score. Higher values indicate closer matches for Cosine and
    /// DotProduct metrics; lower values for Euclidean distance.
    /// </summary>
    public float Score { get; init; }

    /// <summary>
    /// Metadata payload attached to the point, or <c>null</c> when not requested.
    /// </summary>
    public JObject? Metadata { get; init; }
}
