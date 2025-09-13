// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Text;

namespace CrossCloudKit.Utilities.Common;

/// <summary>
/// Provides utilities for encoding and decoding operations.
/// </summary>
public static class EncodingUtilities
{
    /// <summary>
    /// Checks if a string contains only hexadecimal characters.
    /// </summary>
    /// <param name="input">The string to check</param>
    /// <returns>True if the string contains only hex characters</returns>
    public static bool IsHexString(string input)
    {
        return input.Length > 0 && input.All(char.IsAsciiHexDigit);
    }

    /// <summary>
    /// Decodes a hexadecimal string to its ASCII representation.
    /// </summary>
    /// <param name="hexInput">The hexadecimal string to decode</param>
    /// <returns>The decoded ASCII string</returns>
    public static string HexDecode(string hexInput)
    {
        ArgumentOutOfRangeException.ThrowIfNotEqual(hexInput.Length % 2, 0);

        var bytes = new byte[hexInput.Length / 2];
        for (var i = 0; i < bytes.Length; i++)
        {
            bytes[i] = Convert.ToByte(hexInput.Substring(i * 2, 2), 16);
        }

        return Encoding.ASCII.GetString(bytes);
    }

    /// <summary>
    /// Decodes a Base64 string to its UTF-8 representation.
    /// </summary>
    /// <param name="base64Input">The Base64 string to decode</param>
    /// <returns>The decoded UTF-8 string</returns>
    public static string Base64Decode(string base64Input)
    {
        var bytes = Convert.FromBase64String(base64Input);
        return Encoding.UTF8.GetString(bytes);
    }

    /// <summary>
    /// Encodes a string to Base64 using UTF-8 encoding.
    /// </summary>
    /// <param name="input">The string to encode</param>
    /// <returns>The Base64 encoded string</returns>
    public static string Base64Encode(string input)
    {
        var bytes = Encoding.UTF8.GetBytes(input);
        return Convert.ToBase64String(bytes);
    }

    /// <summary>
    /// Encodes a topic name to what cloud providers expect.
    /// </summary>
    /// <param name="topic">The string to encode</param>
    /// <returns>Encoded string</returns>
    public static string? EncodeTopic(string? topic) => topic == null ? null : Uri.EscapeDataString(topic);

    /// <summary>
    /// Decodes a cloud-compatible topic name to original.
    /// </summary>
    /// <param name="encodedTopic">The string to decode</param>
    /// <returns>Decoded string</returns>
    public static string? DecodeTopic(string? encodedTopic) => encodedTopic == null ? null : Uri.UnescapeDataString(encodedTopic);

    /// <summary>
    /// Encodes a string into a filename-safe token using UTF-8 bytes and Base64 URL alphabet (unpadded).
    /// Safe for filenames on Windows, Linux, and macOS.
    /// </summary>
    /// <param name="input">The string to encode.</param>
    /// <returns>A filename-safe encoded string.</returns>
    public static string Base64EncodeNoPadding(string input)
    {
        ArgumentNullException.ThrowIfNull(input);
        if (input.Length == 0) return string.Empty;

        var utf8 = Encoding.UTF8.GetBytes(input);
        return ToBase64UrlNoPadding(utf8);
    }

    /// <summary>
    /// Decodes a string produced by <see cref="Base64EncodeNoPadding"/> back to its original value.
    /// </summary>
    /// <param name="token">The encoded string.</param>
    /// <returns>The original decoded string.</returns>
    public static string Base64DecodeNoPadding(string token)
    {
        ArgumentNullException.ThrowIfNull(token);
        if (token.Length == 0) return string.Empty;

        var bytes = FromBase64UrlNoPadding(token);
        return Encoding.UTF8.GetString(bytes);
    }
    private static string ToBase64UrlNoPadding(byte[] bytes)
    {
        var s = Convert.ToBase64String(bytes); // standard base64
        s = s.TrimEnd('=');                       // remove padding
        s = s.Replace('+', '-').Replace('/', '_'); // URL-safe
        return s;
    }
    private static byte[] FromBase64UrlNoPadding(string s)
    {
        var incoming = s.Replace('-', '+').Replace('_', '/');

        // Add padding to make the length a multiple of 4
        var mod = incoming.Length % 4;
        if (mod != 0)
        {
            incoming += new string('=', 4 - mod);
        }

        return Convert.FromBase64String(incoming);
    }
}
