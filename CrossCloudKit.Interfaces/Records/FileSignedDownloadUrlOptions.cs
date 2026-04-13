// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Interfaces.Records;

/// <summary>
/// Options for creating signed URLs for file downloads.
/// </summary>
/// <example>
/// <code>
/// var opts = new FileSignedDownloadUrlOptions { ValidFor = TimeSpan.FromMinutes(15) };
/// var url = await fileService.CreateSignedDownloadUrlAsync("bucket", "file.pdf", opts);
/// </code>
/// </example>
public sealed record FileSignedDownloadUrlOptions
{
    /// <summary>
    /// Gets or sets the validity duration for the signed URL.
    /// </summary>
    public TimeSpan ValidFor { get; init; } = TimeSpan.FromMinutes(1);
}
