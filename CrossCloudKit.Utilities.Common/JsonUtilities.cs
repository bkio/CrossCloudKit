// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
// ReSharper disable MemberCanBePrivate.Global

namespace CrossCloudKit.Utilities.Common;

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

        // Check if it's effectively an integer
        const double tolerance = 1e-10;
        if (Math.Abs(doubleValue - Math.Round(doubleValue)) > tolerance)
            return false;

        // Clamp explicitly before casting
        var safeValue = doubleValue switch
        {
            >= long.MaxValue => long.MaxValue,
            <= long.MinValue => long.MinValue,
            _ => (long)Math.Round(doubleValue)
        };

        intValue = new JValue(safeValue);
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

    /// <summary>
    /// Safely retrieves a field value from a JObject and converts it to the specified type.
    /// </summary>
    /// <typeparam name="T">The type to convert the field value to</typeparam>
    /// <param name="this">The JObject to retrieve the field from</param>
    /// <param name="field">The field name to retrieve</param>
    /// <returns>The converted field value, or default(T) if the field doesn't exist or conversion fails</returns>
    public static T? GetFieldSafe<T>(this JObject @this, string field)
    {
        ArgumentNullException.ThrowIfNull(@this);
        ArgumentNullException.ThrowIfNull(field);

        try
        {
            if (@this.TryGetValue(field, out var result))
                return result.ToObject<T>();
        }
        catch (Exception ex) when (ex is JsonException or InvalidCastException or OverflowException)
        {
            // Log specific exceptions if logging is available
        }

        return default;
    }

    /// <summary>
    /// Attempts to retrieve a string value from a JObject for the specified key.
    /// </summary>
    /// <param name="input">The JObject to retrieve the value from</param>
    /// <param name="key">The key to look for</param>
    /// <param name="value">The retrieved string value, or null if not found</param>
    /// <returns>True if the value was successfully retrieved; otherwise, false</returns>
    public static bool TryGetTypedValue(this JObject input, string key, out string? value)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(key);

        value = null;

        if (!input.TryGetValue(key, out var vToken) || vToken.Type == JTokenType.Null)
            return false;

        value = vToken.Type == JTokenType.String ? vToken.Value<string>() : vToken.ToString();

        return true;
    }

    /// <summary>
    /// Attempts to retrieve an integer value from a JObject for the specified key, with automatic type conversion.
    /// </summary>
    /// <param name="input">The JObject to retrieve the value from</param>
    /// <param name="key">The key to look for</param>
    /// <param name="value">The retrieved integer value, or 0 if not found or conversion failed</param>
    /// <returns>True if the value was successfully retrieved and converted; otherwise, false</returns>
    /// <remarks>
    /// This method can convert from string and float types to integer if they represent whole numbers.
    /// </remarks>
    public static bool TryGetTypedValue(this JObject input, string key, out int value)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(key);

        value = 0;

        if (!input.TryGetValue(key, out var vToken))
            return false;

        switch (vToken.Type)
        {
            case JTokenType.Integer:
                var longValue = vToken.Value<long>();
                if (longValue is < int.MinValue or > int.MaxValue) return false;
                value = (int)longValue;
                return true;

            case JTokenType.Float:
                var doubleValue = vToken.Value<double>();
                if (Math.Abs(doubleValue - Math.Round(doubleValue)) < double.Epsilon &&
                    doubleValue is >= int.MinValue and <= int.MaxValue)
                {
                    value = (int)Math.Round(doubleValue);
                    return true;
                }
                return false;

            case JTokenType.String:
                var stringValue = vToken.Value<string>();
                if (int.TryParse(stringValue, out value))
                    return true;

                // Try parsing as double first, then check if it's a whole number
                if (!double.TryParse(stringValue, out var doubleFromString)) return false;
                if (!(Math.Abs(doubleFromString - Math.Round(doubleFromString)) < double.Epsilon) ||
                    doubleFromString is < int.MinValue or > int.MaxValue) return false;
                value = (int)Math.Round(doubleFromString);
                return true;

            default:
                return false;
        }
    }

    /// <summary>
    /// Attempts to retrieve a boolean value from a JObject for the specified key.
    /// </summary>
    /// <param name="input">The JObject to retrieve the value from</param>
    /// <param name="key">The key to look for</param>
    /// <param name="value">The retrieved boolean value, or false if not found</param>
    /// <returns>True if the value was successfully retrieved; otherwise, false</returns>
    /// <remarks>
    /// This method can convert from string representation of boolean values.
    /// </remarks>
    public static bool TryGetTypedValue(this JObject input, string key, out bool value)
    {
        value = false;

        if (!input.TryGetValue(key, out var vToken))
            return false;

        if (vToken.Type != JTokenType.Boolean)
            return vToken.Type == JTokenType.String && bool.TryParse(vToken.Value<string>(), out value);

        value = (bool)vToken;
        return true;
    }

    /// <summary>
    /// Attempts to retrieve a float value from a JObject for the specified key, with automatic type conversion.
    /// </summary>
    /// <param name="input">The JObject to retrieve the value from</param>
    /// <param name="key">The key to look for</param>
    /// <param name="value">The retrieved float value, or 0 if not found or conversion failed</param>
    /// <returns>True if the value was successfully retrieved and converted; otherwise, false</returns>
    /// <remarks>
    /// This method can convert from string and integer types to float.
    /// </remarks>
    public static bool TryGetTypedValue(this JObject input, string key, out float value)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(key);

        value = 0f;

        if (!input.TryGetValue(key, out var vToken))
            return false;

        switch (vToken.Type)
        {
            case JTokenType.Float:
                var doubleValue = vToken.Value<double>();
                // Check if the double value is within float range
                if (doubleValue is >= float.MinValue and <= float.MaxValue)
                {
                    value = (float)doubleValue;
                    return true;
                }
                return false;

            case JTokenType.Integer:
                var longValue = vToken.Value<long>();
                value = longValue;
                return true;

            case JTokenType.String:
                var stringValue = vToken.Value<string>();
                return float.TryParse(stringValue, out value);

            default:
                return false;
        }
    }

    /// <summary>
    /// Attempts to retrieve a JArray value from a JObject for the specified key.
    /// </summary>
    /// <param name="input">The JObject to retrieve the value from</param>
    /// <param name="key">The key to look for</param>
    /// <param name="value">The retrieved JArray value, or null if not found or not an array</param>
    /// <returns>True if the value was successfully retrieved and is a JArray; otherwise, false</returns>
    public static bool TryGetTypedValue(this JObject input, string key, out JArray? value)
    {
        value = null;

        if (!input.TryGetValue(key, out var vToken) || vToken.Type != JTokenType.Array)
            return false;

        value = (JArray)vToken;
        return true;
    }

    /// <summary>
    /// Attempts to retrieve a JObject value from a JObject for the specified key.
    /// </summary>
    /// <param name="input">The JObject to retrieve the value from</param>
    /// <param name="key">The key to look for</param>
    /// <param name="value">The retrieved JObject value, or null if not found or not an object</param>
    /// <returns>True if the value was successfully retrieved and is a JObject; otherwise, false</returns>
    public static bool TryGetTypedValue(this JObject input, string key, out JObject? value)
    {
        value = null;

        if (!input.TryGetValue(key, out var vToken) || vToken.Type != JTokenType.Object)
            return false;

        value = (JObject)vToken;
        return true;
    }

    /// <summary>
    /// Attempts to retrieve a list of JObject values from a JObject for the specified key.
    /// </summary>
    /// <param name="input">The JObject to retrieve the value from</param>
    /// <param name="key">The key to look for</param>
    /// <param name="value">The retrieved list of JObject values, or empty list if not found</param>
    /// <returns>True if the value was successfully retrieved and converted to List&lt;JObject&gt;; otherwise, false</returns>
    public static bool TryGetTypedValue(this JObject input, string key, out List<JObject> value)
    {
        return Internal_TryGetTypedValue(input, key, out value);
    }

    /// <summary>
    /// Attempts to retrieve a list of string values from a JObject for the specified key.
    /// </summary>
    /// <param name="input">The JObject to retrieve the value from</param>
    /// <param name="key">The key to look for</param>
    /// <param name="value">The retrieved list of string values, or empty list if not found</param>
    /// <returns>True if the value was successfully retrieved and converted to List&lt;string&gt;; otherwise, false</returns>
    public static bool TryGetTypedValue(this JObject input, string key, out List<string> value)
    {
        return Internal_TryGetTypedValue(input, key, out value);
    }

    /// <summary>
    /// Attempts to retrieve a list of integer values from a JObject for the specified key.
    /// </summary>
    /// <param name="input">The JObject to retrieve the value from</param>
    /// <param name="key">The key to look for</param>
    /// <param name="value">The retrieved list of integer values, or empty list if not found</param>
    /// <returns>True if the value was successfully retrieved and converted to List&lt;int&gt;; otherwise, false</returns>
    public static bool TryGetTypedValue(this JObject input, string key, out List<int> value)
    {
        return Internal_TryGetTypedValue(input, key, out value);
    }

    /// <summary>
    /// Attempts to retrieve a list of float values from a JObject for the specified key.
    /// </summary>
    /// <param name="input">The JObject to retrieve the value from</param>
    /// <param name="key">The key to look for</param>
    /// <param name="value">The retrieved list of float values, or empty list if not found</param>
    /// <returns>True if the value was successfully retrieved and converted to List&lt;float&gt;; otherwise, false</returns>
    public static bool TryGetTypedValue(this JObject input, string key, out List<float> value)
    {
        return Internal_TryGetTypedValue(input, key, out value);
    }

    /// <summary>
    /// Attempts to retrieve a list of double values from a JObject for the specified key.
    /// </summary>
    /// <param name="input">The JObject to retrieve the value from</param>
    /// <param name="key">The key to look for</param>
    /// <param name="value">The retrieved list of double values, or empty list if not found</param>
    /// <returns>True if the value was successfully retrieved and converted to List&lt;double&gt;; otherwise, false</returns>
    public static bool TryGetTypedValue(this JObject input, string key, out List<double> value)
    {
        return Internal_TryGetTypedValue(input, key, out value);
    }

    /// <summary>
    /// Attempts to retrieve a list of boolean values from a JObject for the specified key.
    /// </summary>
    /// <param name="input">The JObject to retrieve the value from</param>
    /// <param name="key">The key to look for</param>
    /// <param name="value">The retrieved list of boolean values, or empty list if not found</param>
    /// <returns>True if the value was successfully retrieved and converted to List&lt;bool&gt;; otherwise, false</returns>
    public static bool TryGetTypedValue(this JObject input, string key, out List<bool> value)
    {
        return Internal_TryGetTypedValue(input, key, out value);
    }

    /// <summary>
    /// Internal helper method that attempts to retrieve and convert a JArray to a List of the specified type.
    /// </summary>
    /// <typeparam name="T">The type of elements in the list</typeparam>
    /// <param name="input">The JObject to retrieve the array from</param>
    /// <param name="key">The key to look for</param>
    /// <param name="value">The retrieved and converted list, or empty list if conversion fails</param>
    /// <returns>True if the array was successfully retrieved and all elements converted; otherwise, false</returns>
    private static bool Internal_TryGetTypedValue<T>(JObject input, string key, out List<T> value)
    {
        value = [];

        if (!input.TryGetValue(key, out var vToken) || vToken.Type != JTokenType.Array)
            return false;

        var tmp = (JArray)vToken;

        if (typeof(T) == typeof(JObject))
        {
            foreach (var t in tmp)
            {
                if (t.Type != JTokenType.Object)
                    return false;
                value.Add((T)(object)(JObject)t);
            }
        }
        else if (typeof(T) == typeof(string))
        {
            foreach (var t in tmp)
            {
                if (t.Type != JTokenType.String)
                    return false;
                var tStr = t.Value<string>().NotNull();
                value.Add((T)(object)tStr);
            }
        }
        else if (typeof(T) == typeof(int))
        {
            foreach (var t in tmp)
            {
                if (t.Type != JTokenType.Integer)
                    return false;
                value.Add((T)(object)(int)t);
            }
        }
        else if (typeof(T) == typeof(float))
        {
            foreach (var t in tmp)
            {
                if (t.Type != JTokenType.Float)
                    return false;
                value.Add((T)(object)(float)t);
            }
        }
        else if (typeof(T) == typeof(double))
        {
            foreach (var t in tmp)
            {
                if (t.Type != JTokenType.Float)
                    return false;
                value.Add((T)(object)(double)t);
            }
        }
        else if (typeof(T) == typeof(bool))
        {
            foreach (var t in tmp)
            {
                if (t.Type != JTokenType.Boolean)
                    return false;
                value.Add((T)(object)(bool)t);
            }
        }
        return true;
    }

    /// <summary>
    /// Converts a generic List to a JArray.
    /// </summary>
    /// <typeparam name="T">The type of elements in the list</typeparam>
    /// <param name="input">The list to convert</param>
    /// <returns>A JArray containing all elements from the input list</returns>
    public static JArray ToJArray<T>(this List<T> input)
    {
        var result = new JArray();

        foreach (var c in input)
        {
            result.Add(c);
        }
        return result;
    }

    /// <summary>
    /// Converts a Dictionary&lt;string, string&gt; to a JObject.
    /// </summary>
    /// <param name="input">The dictionary to convert</param>
    /// <returns>A JObject with the same key-value pairs as the input dictionary</returns>
    public static JObject Convert(Dictionary<string, string> input)
    {
        ArgumentNullException.ThrowIfNull(input);

        var result = new JObject();
        foreach (var current in input)
            result[current.Key] = current.Value;
        return result;
    }

    /// <summary>
    /// Converts a Dictionary&lt;string, int&gt; to a JObject.
    /// </summary>
    /// <param name="input">The dictionary to convert</param>
    /// <returns>A JObject with the same key-value pairs as the input dictionary</returns>
    public static JObject Convert(Dictionary<string, int> input)
    {
        ArgumentNullException.ThrowIfNull(input);

        var result = new JObject();
        foreach (var current in input)
            result[current.Key] = current.Value;
        return result;
    }
}
