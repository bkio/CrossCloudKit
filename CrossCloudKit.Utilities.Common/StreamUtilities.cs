// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Utilities.Common;

/// <summary>
/// Provides utilities for working with streams and I/O operations.
/// </summary>
public static class StreamUtilities
{
    /// <summary>
    /// Reads all bytes from a stream efficiently using modern .NET APIs.
    /// </summary>
    /// <param name="stream">The stream to read from</param>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>All bytes from the stream</returns>
    public static async Task<byte[]> ReadAllBytesAsync(Stream stream, CancellationToken cancellationToken = default)
    {
        if (stream is MemoryStream memoryStream)
        {
            return memoryStream.ToArray();
        }

        await using var buffer = new MemoryTributary();
        await stream.CopyToAsync(buffer, cancellationToken).ConfigureAwait(false);
        return buffer.ToArray();
    }

    /// <summary>
    /// Reads all bytes from a stream synchronously, preserving the original position.
    /// </summary>
    /// <param name="stream">The stream to read from</param>
    /// <returns>All bytes from the stream</returns>
    public static byte[] ReadAllBytes(Stream stream)
    {
        var originalPosition = stream.CanSeek ? stream.Position : -1;

        try
        {
            if (stream.CanSeek)
            {
                stream.Position = 0;
            }

            using var buffer = new MemoryTributary();
            stream.CopyTo(buffer);
            return buffer.ToArray();
        }
        finally
        {
            if (originalPosition >= 0 && stream.CanSeek)
            {
                stream.Position = originalPosition;
            }
        }
    }
}
