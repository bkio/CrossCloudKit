// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Interfaces.Records;

/// <summary>
/// Options for creating signed URLs for file uploads.
/// </summary>
/// <example>
/// <code>
/// var opts = new FileSignedUploadUrlOptions
/// {
///     ContentType = "application/pdf",
///     ValidFor = TimeSpan.FromMinutes(30),
///     SupportResumable = true
/// };
/// var url = await fileService.CreateSignedUploadUrlAsync("bucket", "upload.pdf", opts);
/// </code>
/// </example>
public sealed record FileSignedUploadUrlOptions
{
    /// <summary>
    /// Gets or sets the content type of the file to be uploaded.
    /// </summary>
    public string? ContentType { get; init; }

    /// <summary>
    /// Gets or sets the validity duration for the signed URL.
    /// </summary>
    public TimeSpan ValidFor { get; init; } = TimeSpan.FromMinutes(60);

    /// <summary>
    /// Gets or sets whether to support resumable uploads.
    /// </summary>
    public bool SupportResumable { get; init; }
}
