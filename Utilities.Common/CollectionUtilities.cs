// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace Utilities.Common;

/// <summary>
/// Provides utilities for working with collections and key-value pairs.
/// </summary>
public static class CollectionUtilities
{
    /// <summary>
    /// Gets the first non-null, non-empty string from a list.
    /// </summary>
    /// <param name="list">The list of strings</param>
    /// <returns>The first valid string, or null if none found</returns>
    public static string? GetFirstValidString(IEnumerable<string?> list)
    {
        ArgumentNullException.ThrowIfNull(list);
        return list.FirstOrDefault(s => !string.IsNullOrEmpty(s));
    }

    /// <summary>
    /// Gets a value by key from a collection of key-value pairs.
    /// </summary>
    /// <param name="pairs">The collection of key-value pairs</param>
    /// <param name="key">The key to search for</param>
    /// <returns>The value if found, null otherwise</returns>
    public static string? GetValueByKey(IEnumerable<(string Key, string Value)> pairs, string key)
    {
        ArgumentNullException.ThrowIfNull(pairs);
        ArgumentNullException.ThrowIfNull(key);

        return pairs.FirstOrDefault(pair => pair.Key == key).Value;
    }

    /// <summary>
    /// Gets a value by key from an array of key-value pairs.
    /// </summary>
    /// <param name="pairs">The array of key-value pairs</param>
    /// <param name="key">The key to search for</param>
    /// <returns>The value if found, null otherwise</returns>
    public static string? GetValueByKey((string Key, string Value)[] pairs, string key)
    {
        ArgumentNullException.ThrowIfNull(pairs);
        ArgumentNullException.ThrowIfNull(key);

        return pairs.FirstOrDefault(pair => pair.Key == key).Value;
    }
}
