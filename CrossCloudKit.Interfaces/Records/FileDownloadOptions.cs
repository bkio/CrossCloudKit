// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Interfaces.Records;

/// <summary>
/// Options for downloading files with range support.
/// </summary>
/// <example>
/// <code>
/// // Download bytes 1000-2000
/// var opts = new FileDownloadOptions { StartIndex = 1000, Size = 1000 };
/// await fileService.DownloadFileAsync("bucket", "file.bin", "/tmp/part.bin", opts);
/// </code>
/// </example>
public sealed record FileDownloadOptions
{
    /// <summary>
    /// Gets or sets the starting byte index for partial downloads.
    /// </summary>
    public long StartIndex { get; init; }

    /// <summary>
    /// Gets or sets the number of bytes to download (0 for an entire file).
    /// </summary>
    public long Size { get; init; }
}
