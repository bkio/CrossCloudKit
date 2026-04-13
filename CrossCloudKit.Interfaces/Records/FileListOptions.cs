// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Interfaces.Records;

/// <summary>
/// Options for listing files in a bucket.
/// </summary>
/// <example>
/// <code>
/// var opts = new FileListOptions { Prefix = "reports/2024/", MaxResults = 50 };
/// var result = await fileService.ListFilesAsync("bucket", opts);
/// </code>
/// </example>
public sealed record FileListOptions
{
    /// <summary>
    /// Gets or sets the prefix to filter file keys.
    /// </summary>
    public string? Prefix { get; init; }

    /// <summary>
    /// Gets or sets the maximum number of files to return.
    /// </summary>
    public int? MaxResults { get; init; }

    /// <summary>
    /// Gets or sets the continuation token for paginated results.
    /// </summary>
    public string? ContinuationToken { get; init; }
}
