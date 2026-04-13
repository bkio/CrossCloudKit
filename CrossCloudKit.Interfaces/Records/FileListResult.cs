// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Interfaces.Records;

/// <summary>
/// Result of a paginated file listing operation.
/// </summary>
/// <example>
/// <code>
/// var result = await fileService.ListFilesAsync("bucket");
/// if (result.IsSuccessful)
/// {
///     foreach (var key in result.Data.FileKeys)
///         Console.WriteLine(key);
///     if (result.Data.NextContinuationToken != null)
///         // fetch next page
/// }
/// </code>
/// </example>
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
