// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Interfaces.Records;

/// <summary>
/// Represents a signed URL for file operations.
/// </summary>
/// <param name="Url">The signed URL</param>
/// <param name="ExpiresAt">When the URL expires</param>
// ReSharper disable once NotAccessedPositionalProperty.Global
public sealed record FileSignedUrl(string Url, DateTimeOffset ExpiresAt);
