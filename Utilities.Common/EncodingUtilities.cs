// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Text;

namespace Utilities.Common;

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
        for (int i = 0; i < bytes.Length; i++)
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
}
