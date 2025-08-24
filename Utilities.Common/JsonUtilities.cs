// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Newtonsoft.Json.Linq;
// ReSharper disable MemberCanBePrivate.Global

namespace Utilities.Common;

/// <summary>
/// Provides utilities for JSON operations using both Newtonsoft.Json and System.Text.Json.
/// </summary>
public static class JsonUtilities
{
    /// <summary>
    /// Converts round float values to integers throughout a JObject recursively.
    /// </summary>
    /// <param name="jsonObject">The JObject to process</param>
    public static void ConvertRoundFloatToIntAllInJObject(JObject? jsonObject)
    {
        if (jsonObject == null) return;

        var properties = jsonObject.Properties().ToList();
        foreach (var property in properties)
        {
            if (property.Value is JObject nestedObject)
            {
                ConvertRoundFloatToIntAllInJObject(nestedObject);
            }
            else if (property.Value is JArray array)
            {
                ConvertRoundFloatToIntAllInJArray(array);
            }
            else if (property.Value.Type == JTokenType.Float)
            {
                if (TryConvertFloatToInt(property.Value, out var intValue))
                {
                    property.Value = intValue;
                }
            }
        }
    }

    /// <summary>
    /// Converts round float values to integers throughout a JArray recursively.
    /// </summary>
    /// <param name="jsonArray">The JArray to process</param>
    public static void ConvertRoundFloatToIntAllInJArray(JArray? jsonArray)
    {
        if (jsonArray == null || jsonArray.Count == 0) return;

        for (int i = 0; i < jsonArray.Count; i++)
        {
            var item = jsonArray[i];
            if (item is JObject nestedObject)
            {
                ConvertRoundFloatToIntAllInJObject(nestedObject);
            }
            else if (item is JArray nestedArray)
            {
                ConvertRoundFloatToIntAllInJArray(nestedArray);
            }
            else if (item.Type == JTokenType.Float)
            {
                if (TryConvertFloatToInt(item, out var intValue))
                {
                    jsonArray[i] = intValue;
                }
            }
        }
    }

    /// <summary>
    /// Sorts a JObject recursively by property names and optionally converts round floats to integers.
    /// </summary>
    /// <param name="jsonObject">The JObject to sort</param>
    /// <param name="convertRoundFloatToInt">Whether to convert round floats to integers</param>
    public static void SortJObject(JObject? jsonObject, bool convertRoundFloatToInt = false)
    {
        if (jsonObject == null) return;

        var properties = jsonObject.Properties().ToList();

        // Remove all properties first
        foreach (var property in properties)
        {
            if (convertRoundFloatToInt && property.Value.Type == JTokenType.Float)
            {
                if (TryConvertFloatToInt(property.Value, out var intValue))
                {
                    property.Value = intValue;
                }
            }
            property.Remove();
        }

        // Add properties back in sorted order
        foreach (var property in properties.OrderBy(p => p.Name, StringComparer.Ordinal))
        {
            jsonObject.Add(property);

            // Recursively sort nested objects and arrays
            if (property.Value is JObject nestedObject)
            {
                SortJObject(nestedObject, convertRoundFloatToInt);
            }
            else if (property.Value is JArray array)
            {
                SortJArray(array, convertRoundFloatToInt);
            }
        }
    }

    /// <summary>
    /// Sorts a JArray recursively and optionally converts round floats to integers.
    /// </summary>
    /// <param name="jsonArray">The JArray to sort</param>
    /// <param name="convertRoundFloatToInt">Whether to convert round floats to integers</param>
    public static void SortJArray(JArray? jsonArray, bool convertRoundFloatToInt = false)
    {
        if (jsonArray == null || jsonArray.Count == 0) return;

        var items = jsonArray.ToList();

        // Process and remove all items
        for (int i = 0; i < items.Count; i++)
        {
            var item = items[i];
            if (convertRoundFloatToInt && item.Type == JTokenType.Float)
            {
                if (TryConvertFloatToInt(item, out var intValue))
                {
                    items[i] = intValue;
                }
            }
            item.Remove();
        }

        // Sort items by their string representation for consistent ordering
        var sortedItems = items.OrderBy(GetSortableString).ToList();

        // Add sorted items back
        foreach (var item in sortedItems)
        {
            jsonArray.Add(item);

            // Recursively sort nested objects and arrays
            if (item is JObject nestedObject)
            {
                SortJObject(nestedObject, convertRoundFloatToInt);
            }
            else if (item is JArray nestedArray)
            {
                SortJArray(nestedArray, convertRoundFloatToInt);
            }
        }
    }

    /// <summary>
    /// Attempts to convert a float JToken to an integer if it's a whole number.
    /// </summary>
    /// <param name="token">The JToken to convert</param>
    /// <param name="intValue">The converted integer value</param>
    /// <returns>True if conversion was successful, false otherwise</returns>
    private static bool TryConvertFloatToInt(JToken token, out JToken intValue)
    {
        intValue = token;

        if (token.Type != JTokenType.Float)
            return false;

        var doubleValue = token.Value<double>();

        // Check if it's a whole number within the safe range for long
        if (!(Math.Abs(doubleValue - Math.Round(doubleValue)) < double.Epsilon) ||
            !(doubleValue >= long.MinValue) ||
            !(doubleValue <= long.MaxValue)) return false;
        intValue = new JValue((long)Math.Round(doubleValue));
        return true;

    }

    /// <summary>
    /// Gets a sortable string representation of a JToken for consistent array sorting.
    /// </summary>
    /// <param name="token">The JToken to get sortable string for</param>
    /// <returns>A sortable string representation</returns>
    private static string GetSortableString(JToken token)
    {
        return token.Type switch
        {
            JTokenType.Null => "0_null",
            JTokenType.Boolean => $"1_{token.Value<bool>()}",
            JTokenType.Integer => $"2_{token.Value<long>():D20}", // Pad for proper numeric sorting
            JTokenType.Float => $"3_{token.Value<double>():F10}",
            JTokenType.String => $"4_{token.Value<string>()}",
            JTokenType.Date => $"5_{token.Value<DateTime>():yyyy-MM-ddTHH:mm:ss.fffffffK}",
            JTokenType.Array => $"6_[{((JArray)token).Count}]",
            JTokenType.Object => $"7_{{{((JObject)token).Count}}}",
            _ => $"9_{token}"
        };
    }
}
