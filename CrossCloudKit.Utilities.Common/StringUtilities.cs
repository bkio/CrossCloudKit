// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Globalization;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
// ReSharper disable MemberCanBePrivate.Global

namespace CrossCloudKit.Utilities.Common;

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
        ArgumentNullException.ThrowIfNull(target);

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
        ArgumentNullException.ThrowIfNull(target);

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
    /// <param name="includeDigits">Whether to include digits in the random string</param>
    /// <returns>A random string</returns>
    public static string GenerateRandomString(int length, bool lowercase = false, bool includeDigits = false)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(length);

        var chars = includeDigits ? RandomStringChars : RandomStringCharsOnlyCharacters;
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
    public static string WildcardToRegex(string? wildcardPattern)
    {
        ArgumentNullException.ThrowIfNull(wildcardPattern);
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
    public static string SanitizeElasticsearchIndexName(string? indexName)
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

    /// <summary>
    /// Character set containing uppercase letters and digits for random string generation.
    /// </summary>
    private const string RandomStringChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    /// <summary>
    /// Character set containing only uppercase letters for random string generation.
    /// </summary>
    private const string RandomStringCharsOnlyCharacters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";


    /// <summary>
    /// Validates whether a string is a properly formatted email address.
    /// </summary>
    /// <param name="email">The email address to validate</param>
    /// <returns>True if the email address is valid; otherwise, false</returns>
    public static bool IsValidEmail(string? email)
    {
        if (string.IsNullOrWhiteSpace(email))
            return false;

        try
        {
            // Normalize the domain
            email = Regex.Replace(email, @"(@)(.+)$", DomainMapper, RegexOptions.None);

            // Examines the domain part of the email and normalizes it.
            string DomainMapper(Match match)
            {
                // Use IdnMapping class to convert Unicode domain names.
                var idn = new IdnMapping();

                // Pull out and process domain name (throws ArgumentException on invalid)
                var dName = idn.GetAscii(match.Groups[2].Value);

                return match.Groups[1].Value + dName;
            }
        }
        catch (RegexMatchTimeoutException)
        {
            return false;
        }
        catch (ArgumentException)
        {
            return false;
        }

        try
        {
            return Regex.IsMatch(email,
                @"^[^@\s]+@[^@\s]+\.[^@\s]+$",
                RegexOptions.IgnoreCase);
        }
        catch (RegexMatchTimeoutException)
        {
            return false;
        }
    }

    /// <summary>
    /// Validates whether a string is a properly formatted HTTP or HTTPS URL.
    /// </summary>
    /// <param name="url">The URL to validate</param>
    /// <returns>True if the URL is valid and uses HTTP or HTTPS scheme; otherwise, false</returns>
    public static bool IsValidUrl(string url)
    {
        return Uri.TryCreate(url, UriKind.Absolute, out var uri) && (uri.Scheme == Uri.UriSchemeHttp || uri.Scheme == Uri.UriSchemeHttps);
    }

    /// <summary>
    /// Retrieves a parameter value from URL parameters dictionary, handling both normal and amp-prefixed parameters.
    /// </summary>
    /// <param name="urlParameters">Dictionary containing URL parameters</param>
    /// <param name="parameter">The parameter name to retrieve</param>
    /// <param name="action">The parameter value if found; otherwise, null</param>
    /// <returns>True if the parameter was found; otherwise, false</returns>
    public static bool GetParameterFromUrlParameters(Dictionary<string, string> urlParameters, string parameter, out string? action)
    {
        action = null;

        if (!urlParameters.TryGetValue(parameter, out var urlParameter))
        {
            if (!urlParameters.ContainsKey($"amp;{parameter}"))
            {
                return false;
            }
            else
            {
                action = urlParameters[$"amp;{parameter}"];
            }
        }
        else
        {
            action = urlParameter;
        }
        return true;
    }

    /// <summary>
    /// Replaces the first occurrence of a specified string with another string.
    /// </summary>
    /// <param name="input">The string to search in</param>
    /// <param name="find">The string to find</param>
    /// <param name="replaceWith">The string to replace the first occurrence with</param>
    /// <param name="comparison">The type of string comparison to use</param>
    /// <returns>A new string with the first occurrence replaced</returns>
    public static string ReplaceFirst(this string input, string find, string replaceWith, StringComparison comparison = StringComparison.Ordinal)
    {
        var ix = input.IndexOf(find, comparison);
        if (ix < 0)
            return input;

        var removed = input.Remove(ix, find.Length);
        return replaceWith.Length == 0 ? removed : removed.Insert(ix, replaceWith);
    }

    /// <summary>
    /// Converts a camelCase or PascalCase string to sentence case by adding spaces before uppercase letters.
    /// </summary>
    /// <param name="input">The input string to convert</param>
    /// <returns>A string with spaces added before uppercase letters</returns>
    public static string ToSentenceCase(this string input)
    {
        return Regex.Replace(input, "[a-z][A-Z]", m => $"{m.Value[0]} {char.ToLower(m.Value[1])}");
    }

    /// <summary>
    /// Converts a snake_case string to Title Case format.
    /// </summary>
    /// <param name="input">The snake_case string to convert</param>
    /// <returns>A Title Case formatted string with spaces instead of underscores</returns>
    public static string SnakeCaseToTitleCase(this string input)
    {
        var split = input.Split('_');
        if (split.Length < 2)
            return MakeWordStartWithUpperCaseRestLower(input);

        var builder = new StringBuilder();
        for (var i = 0; i < split.Length; i++)
        {
            builder.Append(MakeWordStartWithUpperCaseRestLower(split[i]));
            if (i != (split.Length - 1))
                builder.Append(' ');
        }
        return builder.ToString();
    }
    private static string MakeWordStartWithUpperCaseRestLower(string input)
    {
        if (input.Length == 0) return "";

        var builder = new StringBuilder();
        builder.Append(char.ToUpper(input[0]));
        for (var i = 1; i < input.Length; i++)
        {
            builder.Append(char.ToLower(input[i]));
        }
        return builder.ToString();
    }

    /// <summary>
    /// Sanitizes a string to make it suitable for use as a file name by replacing invalid characters.
    /// </summary>
    /// <param name="input">The string to sanitize</param>
    /// <returns>A valid file name with invalid characters replaced by underscores</returns>
    public static string MakeValidFileName(this string input)
    {
        return Regex.Replace(input, InvalidRegStr, "_");
    }
    private static readonly string InvalidRegStr = string.Format(@"([{0}]*\.+$)|([{0}]+)", Regex.Escape(new string(Path.GetInvalidFileNameChars())));

    /// <summary>
    /// Limits a string to a maximum number of words separated by the specified separator.
    /// </summary>
    /// <param name="input">The input string to limit</param>
    /// <param name="separator">The character used to separate words</param>
    /// <param name="maxWords">The maximum number of words to keep</param>
    /// <returns>A string containing at most the specified number of words</returns>
    public static string LimitMaxWords(this string input, char separator, int maxWords)
    {
        var split = input.Split(separator);
        return split.Length <= 1 ? input : string.Join(separator, split.Take(Math.Min(maxWords, split.Length)));
    }

    /// <summary>
    /// Limits a string to a maximum number of characters, appending ellipsis if truncated.
    /// </summary>
    /// <param name="input">The input string to limit</param>
    /// <param name="maxCharacters">The maximum number of characters allowed</param>
    /// <returns>The original string if within limit, otherwise truncated with "..." appended</returns>
    public static string LimitMaxCharacters(this string input, int maxCharacters)
    {
        return input.Length <= maxCharacters ? input : $"{input[..(maxCharacters - 3)]}...";
    }

    /// <summary>
    /// Converts a string to a URL-friendly slug format by removing special characters and normalizing spaces.
    /// </summary>
    /// <param name="input">The string to convert to a slug</param>
    /// <returns>A lowercase, URL-friendly slug with hyphens instead of spaces and special characters removed</returns>
    public static string SanitizeToSlug(this string input)
    {
        var output = input.ToLower();

        output = SpecialChars.Aggregate(output, (current, sc) => current.Replace(sc, ""));

        output = output.Replace('–', '-');
        output = output.Replace('@', '-');
        output = output.Replace('.', '-');

        output = Regex.Replace(output, @"[\s-]+", "-");
        output = output.Trim('-').Trim('_');

        return output;
    }

    /// <summary>
    /// Array of special characters that are removed during slug sanitization.
    /// </summary>
    private static readonly string[] SpecialChars = [
        "?", "[", "]", "/", "\\", "=", "<", ">", ":", ";", ",", "'", "\"", "&", "$", "#", "*", "(", ")", "|", "~", "`", "!", "{", "}"
    ];
}
