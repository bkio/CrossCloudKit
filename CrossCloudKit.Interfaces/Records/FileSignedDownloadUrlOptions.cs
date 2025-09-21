// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Interfaces.Records;

/// <summary>
/// Options for creating signed URLs for file downloads.
/// </summary>
public sealed record FileSignedDownloadUrlOptions
{
    /// <summary>
    /// Gets or sets the validity duration for the signed URL.
    /// </summary>
    public TimeSpan ValidFor { get; init; } = TimeSpan.FromMinutes(1);
}
