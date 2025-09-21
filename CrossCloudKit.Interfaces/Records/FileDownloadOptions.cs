// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Interfaces.Records;

/// <summary>
/// Options for downloading files with range support.
/// </summary>
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
