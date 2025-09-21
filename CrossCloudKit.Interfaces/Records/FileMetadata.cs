// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Interfaces.Records;

/// <summary>
/// Represents file metadata information.
/// </summary>
public sealed record FileMetadata
{
    /// <summary>
    /// Gets the size of the file in bytes.
    /// </summary>
    public long Size { get; init; }

    /// <summary>
    /// Gets the MD5 checksum of the file.
    /// </summary>
    public string? Checksum { get; init; }

    /// <summary>
    /// Gets the content type of the file.
    /// </summary>
    public string? ContentType { get; init; }

    /// <summary>
    /// Gets the creation timestamp of the file.
    /// </summary>
    public DateTimeOffset? CreatedAt { get; init; }

    /// <summary>
    /// Gets the last modified timestamp of the file.
    /// </summary>
    public DateTimeOffset? LastModified { get; init; }

    /// <summary>
    /// Gets additional metadata properties.
    /// </summary>
    public IReadOnlyDictionary<string, string> Properties { get; init; } = new Dictionary<string, string>();

    /// <summary>
    /// Gets the file tags/labels.
    /// </summary>
    public IReadOnlyDictionary<string, string> Tags { get; init; } = new Dictionary<string, string>();
}
