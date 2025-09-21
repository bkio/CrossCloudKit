// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Interfaces.Records;

/// <summary>
/// Result of a paginated file listing operation.
/// </summary>
public sealed record FileListResult
{
    /// <summary>
    /// Gets the list of file keys.
    /// </summary>
    public IReadOnlyList<string> FileKeys { get; init; } = [];

    /// <summary>
    /// Gets the continuation token for the next page, if any.
    /// </summary>
    public string? NextContinuationToken { get; init; }
}
