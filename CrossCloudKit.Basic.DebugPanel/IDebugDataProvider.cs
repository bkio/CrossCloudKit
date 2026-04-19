// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Basic.DebugPanel;

/// <summary>
/// Provides browsable data from a Basic service instance to the debug panel.
/// Each Basic service type implements this to expose its entities for inspection.
/// </summary>
public interface IDebugDataProvider
{
    /// <summary>
    /// Lists top-level containers (tables, buckets, collections, scopes).
    /// </summary>
    Task<List<DebugContainer>> ListContainersAsync();

    /// <summary>
    /// Lists items within a container (rows, files, points, keys).
    /// </summary>
    /// <param name="container">Container name (table, bucket, collection, scope).</param>
    /// <param name="maxItems">Maximum items to return (for large containers).</param>
    Task<List<DebugItem>> ListItemsAsync(string container, int maxItems = 200);

    /// <summary>
    /// Gets the full detail of a single item. Returns null if not found.
    /// </summary>
    /// <param name="container">Container name.</param>
    /// <param name="itemId">Item identifier.</param>
    Task<DebugItemDetail?> GetItemDetailAsync(string container, string itemId);
}

/// <summary>
/// A top-level container in a Basic service (table, bucket, collection, scope).
/// </summary>
public sealed record DebugContainer
{
    /// <summary>Container name.</summary>
    public required string Name { get; init; }

    /// <summary>Number of items in the container (-1 if unknown/expensive to count).</summary>
    public long ItemCount { get; init; } = -1;

    /// <summary>Optional extra properties for display (e.g., "Dimensions: 1536", "Distance: Cosine").</summary>
    public Dictionary<string, string>? Properties { get; init; }
}

/// <summary>
/// A single item within a container. Shown in the item list view.
/// </summary>
public sealed record DebugItem
{
    /// <summary>Unique item identifier within its container.</summary>
    public required string Id { get; init; }

    /// <summary>Optional human-readable display label.</summary>
    public string? Label { get; init; }

    /// <summary>Key-value properties shown in the list view (e.g., size, modified date).</summary>
    public Dictionary<string, string>? Properties { get; init; }

    /// <summary>Whether clicking this item shows a detail popup.</summary>
    public bool HasDetail { get; init; } = true;
}

/// <summary>
/// Full detail of a single item, shown in the popup modal.
/// </summary>
public sealed record DebugItemDetail
{
    /// <summary>Item identifier.</summary>
    public required string Id { get; init; }

    /// <summary>Content as a JSON string (pretty-printed) for display.</summary>
    public required string ContentJson { get; init; }

    /// <summary>Optional summary line shown at the top of the popup.</summary>
    public string? Summary { get; init; }
}
