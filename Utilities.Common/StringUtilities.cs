// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;
using System.Text;
using System.Text.RegularExpressions;
// ReSharper disable MemberCanBePrivate.Global

namespace Utilities.Common;

/// <summary>
/// Provides utilities for working with strings and text operations.
/// </summary>
public static class StringUtilities
{
    /// <summary>
    /// Trims the specified string from the start of the target string.
    /// </summary>
    /// <param name="target">The string to trim</param>
    /// <param name="trimString">The string to remove from the start</param>
    /// <returns>The trimmed string</returns>
    public static string TrimStart(this string target, string trimString)
    {
        if (string.IsNullOrEmpty(trimString))
            return target;

        var result = target.AsSpan();
        while (result.StartsWith(trimString))
        {
            result = result[trimString.Length..];
        }

        return result.ToString();
    }

    /// <summary>
    /// Trims the specified string from the end of the target string.
    /// </summary>
    /// <param name="target">The string to trim</param>
    /// <param name="trimString">The string to remove from the end</param>
    /// <returns>The trimmed string</returns>
    public static string TrimEnd(this string target, string trimString)
    {
        if (string.IsNullOrEmpty(trimString))
            return target;

        var result = target.AsSpan();
        while (result.EndsWith(trimString))
        {
            result = result[..^trimString.Length];
        }

        return result.ToString();
    }

    /// <summary>
    /// Trims the specified string from both the start and end of the target string.
    /// </summary>
    /// <param name="target">The string to trim</param>
    /// <param name="trimString">The string to remove from both ends</param>
    /// <returns>The trimmed string</returns>
    public static string Trim(this string target, string trimString)
    {
        return target.TrimStart(trimString).TrimEnd(trimString);
    }

    /// <summary>
    /// Generates a random string of the specified length.
    /// </summary>
    /// <param name="length">The length of the string to generate</param>
    /// <param name="lowercase">Whether to return lowercase letters</param>
    /// <returns>A random string</returns>
    public static string GenerateRandomString(int length, bool lowercase = false)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(length);

        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        var random = Random.Shared;
        var result = new StringBuilder(length);

        for (var i = 0; i < length; i++)
        {
            result.Append(chars[random.Next(chars.Length)]);
        }

        return lowercase ? result.ToString().ToLowerInvariant() : result.ToString();
    }

    /// <summary>
    /// Converts a wildcard pattern to a regular expression pattern.
    /// </summary>
    /// <param name="wildcardPattern">The wildcard pattern (using * and ?)</param>
    /// <returns>A regular expression pattern</returns>
    public static string WildcardToRegex(string wildcardPattern)
    {
        return $"^{Regex.Escape(wildcardPattern).Replace("\\*", ".*").Replace("\\?", ".")}$";
    }

    /// <summary>
    /// Encodes a string for safe use in URL-like contexts by replacing % with a custom sequence.
    /// </summary>
    /// <param name="input">The string to encode</param>
    /// <returns>The encoded string</returns>
    public static string EncodeForTagging(string input)
    {
        return WebUtility.UrlEncode(input).Replace("%", "@pPp@");
    }

    /// <summary>
    /// Decodes a string that was encoded with EncodeForTagging.
    /// </summary>
    /// <param name="input">The string to decode</param>
    /// <returns>The decoded string</returns>
    public static string DecodeFromTagging(string input)
    {
        return WebUtility.UrlDecode(input.Replace("@pPp@", "%"));
    }

    /// <summary>
    /// Sanitizes a string to be a valid Elasticsearch index name.
    /// </summary>
    /// <param name="indexName">The index name to sanitize</param>
    /// <returns>A valid Elasticsearch index name</returns>
    public static string SanitizeElasticsearchIndexName(string indexName)
    {
        ArgumentNullException.ThrowIfNull(indexName);

        // Convert to lowercase
        indexName = indexName.ToLowerInvariant();

        // Remove illegal characters
        indexName = Regex.Replace(indexName, @"[\\/ *?""<>|,#]", "");

        // Ensure it starts with a letter or number
        if (!Regex.IsMatch(indexName, @"^[a-z0-9]"))
        {
            indexName = "log_" + indexName;
        }

        // Limit length (max 255 chars)
        if (indexName.Length > 255)
        {
            indexName = indexName[..255];
        }

        return indexName;
    }
}
