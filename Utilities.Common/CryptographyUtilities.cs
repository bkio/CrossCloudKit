// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Security.Cryptography;
using System.Text;
// ReSharper disable MemberCanBePrivate.Global

namespace Utilities.Common;

/// <summary>
/// Provides utilities for cryptographic operations and hashing.
/// </summary>
public static class CryptographyUtilities
{
    /// <summary>
    /// Calculates the SHA-256 hash of a file.
    /// </summary>
    /// <param name="filePath">The path to the file</param>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>The SHA-256 hash as a lowercase hex string</returns>
    public static async Task<string> CalculateFileSha256Async(string filePath, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

        await using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 4096, useAsync: true);
        return await CalculateStreamSha256Async(fileStream, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Calculates the SHA-256 hash of a stream.
    /// </summary>
    /// <param name="stream">The stream to hash</param>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>The SHA-256 hash as a lowercase hex string</returns>
    public static async Task<string> CalculateStreamSha256Async(Stream stream, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);

        var originalPosition = stream.CanSeek ? stream.Position : -1;

        try
        {
            if (stream.CanSeek)
            {
                stream.Position = 0;
            }

            using var sha256 = SHA256.Create();
            var hashBytes = await sha256.ComputeHashAsync(stream, cancellationToken).ConfigureAwait(false);
            return Convert.ToHexString(hashBytes).ToLowerInvariant();
        }
        finally
        {
            if (originalPosition >= 0 && stream.CanSeek)
            {
                stream.Position = originalPosition;
            }
        }
    }

    /// <summary>
    /// Calculates the SHA-256 hash of a string using UTF-8 encoding.
    /// </summary>
    /// <param name="input">The string to hash</param>
    /// <returns>The SHA-256 hash as a lowercase hex string</returns>
    public static string CalculateStringSha256(string input)
    {
        ArgumentNullException.ThrowIfNull(input);

        var bytes = Encoding.UTF8.GetBytes(input);
        using var sha256 = SHA256.Create();
        var hashBytes = sha256.ComputeHash(bytes);
        return Convert.ToHexString(hashBytes).ToLowerInvariant();
    }

    /// <summary>
    /// Calculates the MD5 hash of a file (legacy support - consider using SHA-256).
    /// </summary>
    /// <param name="filePath">The path to the file</param>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>The MD5 hash as a lowercase hex string</returns>
    [Obsolete("MD5 is cryptographically broken. Use SHA-256 instead.")]
    public static async Task<string> CalculateFileMd5Async(string filePath, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

        await using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 4096, useAsync: true);
        return await CalculateStreamMd5Async(fileStream, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Calculates the MD5 hash of a stream (legacy support - consider using SHA-256).
    /// </summary>
    /// <param name="stream">The stream to hash</param>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>The MD5 hash as a lowercase hex string</returns>
    [Obsolete("MD5 is cryptographically broken. Use SHA-256 instead.")]
    public static async Task<string> CalculateStreamMd5Async(Stream stream, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);

        var originalPosition = stream.CanSeek ? stream.Position : -1;

        try
        {
            if (stream.CanSeek)
            {
                stream.Position = 0;
            }

            using var md5 = MD5.Create();
            var hashBytes = await md5.ComputeHashAsync(stream, cancellationToken).ConfigureAwait(false);
            return Convert.ToHexString(hashBytes).ToLowerInvariant();
        }
        finally
        {
            if (originalPosition >= 0 && stream.CanSeek)
            {
                stream.Position = originalPosition;
            }
        }
    }

    /// <summary>
    /// Calculates the MD5 hash of a string (legacy support - consider using SHA-256).
    /// </summary>
    /// <param name="input">The string to hash</param>
    /// <returns>The MD5 hash as a lowercase hex string</returns>
    [Obsolete("MD5 is cryptographically broken. Use SHA-256 instead.")]
    public static string CalculateStringMd5(string input)
    {
        ArgumentNullException.ThrowIfNull(input);

        var bytes = Encoding.UTF8.GetBytes(input);
        using var md5 = MD5.Create();
        var hashBytes = md5.ComputeHash(bytes);
        return Convert.ToHexString(hashBytes).ToLowerInvariant();
    }
}
