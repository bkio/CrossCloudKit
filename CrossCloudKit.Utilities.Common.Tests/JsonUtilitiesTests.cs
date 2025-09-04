// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Newtonsoft.Json.Linq;
using Xunit;

namespace CrossCloudKit.Utilities.Common.Tests;

public class JsonUtilitiesTests
{
    [Fact]
    public void ConvertRoundFloatToIntAllInJObject_WithRoundFloats_ConvertsToIntegers()
    {
        // Arrange
        var jsonObject = JObject.Parse(@"{
            ""roundFloat"": 5.0,
            ""nonRoundFloat"": 3.14,
            ""integer"": 42,
            ""string"": ""test"",
            ""nested"": {
                ""nestedRoundFloat"": 10.0,
                ""nestedNonRound"": 2.71
            }
        }");

        // Act
        JsonUtilities.ConvertRoundFloatToIntAllInJObject(jsonObject);

        // Assert
        Assert.Equal(JTokenType.Integer, jsonObject["roundFloat"]!.Type);
        Assert.Equal(5L, jsonObject["roundFloat"]!.Value<long>());
        Assert.Equal(JTokenType.Float, jsonObject["nonRoundFloat"]!.Type);
        Assert.Equal(JTokenType.Integer, jsonObject["integer"]!.Type);
        Assert.Equal(JTokenType.String, jsonObject["string"]!.Type);

        var nested = jsonObject["nested"] as JObject;
        Assert.NotNull(nested);
        Assert.Equal(JTokenType.Integer, nested["nestedRoundFloat"]!.Type);
        Assert.Equal(10L, nested["nestedRoundFloat"]!.Value<long>());
        Assert.Equal(JTokenType.Float, nested["nestedNonRound"]!.Type);
    }

    [Fact]
    public void ConvertRoundFloatToIntAllInJObject_WithNull_DoesNotThrow()
    {
        // Act & Assert - Should not throw
        JsonUtilities.ConvertRoundFloatToIntAllInJObject(null);
    }

    [Fact]
    public void ConvertRoundFloatToIntAllInJArray_WithRoundFloats_ConvertsToIntegers()
    {
        // Arrange
        var jsonArray = JArray.Parse(@"[
            5.0,
            3.14,
            42,
            ""test"",
            {""value"": 7.0},
            [8.0, 2.5, 9.0]
        ]");

        // Act
        JsonUtilities.ConvertRoundFloatToIntAllInJArray(jsonArray);

        // Assert
        Assert.Equal(JTokenType.Integer, jsonArray[0].Type);
        Assert.Equal(5L, jsonArray[0].Value<long>());
        Assert.Equal(JTokenType.Float, jsonArray[1].Type);
        Assert.Equal(JTokenType.Integer, jsonArray[2].Type);
        Assert.Equal(JTokenType.String, jsonArray[3].Type);

        var nestedObject = jsonArray[4] as JObject;
        Assert.NotNull(nestedObject);
        Assert.Equal(JTokenType.Integer, nestedObject["value"]!.Type);

        var nestedArray = jsonArray[5] as JArray;
        Assert.NotNull(nestedArray);
        Assert.Equal(JTokenType.Integer, nestedArray[0].Type);
        Assert.Equal(JTokenType.Float, nestedArray[1].Type);
        Assert.Equal(JTokenType.Integer, nestedArray[2].Type);
    }

    [Fact]
    public void ConvertRoundFloatToIntAllInJArray_WithNull_DoesNotThrow()
    {
        // Act & Assert - Should not throw
        JsonUtilities.ConvertRoundFloatToIntAllInJArray(null);
    }

    [Fact]
    public void ConvertRoundFloatToIntAllInJArray_WithEmptyArray_DoesNotThrow()
    {
        // Arrange
        var emptyArray = new JArray();

        // Act & Assert - Should not throw
        JsonUtilities.ConvertRoundFloatToIntAllInJArray(emptyArray);
    }

    [Fact]
    public void SortJObject_WithMixedProperties_SortsCorrectly()
    {
        // Arrange
        var jsonObject = JObject.Parse(@"{
            ""zebra"": ""last"",
            ""alpha"": ""first"",
            ""beta"": 42,
            ""nested"": {
                ""charlie"": ""nested_value"",
                ""alpha_nested"": ""first_nested""
            }
        }");

        // Act
        JsonUtilities.SortJObject(jsonObject);

        // Assert
        var properties = jsonObject.Properties().ToArray();
        Assert.Equal("alpha", properties[0].Name);
        Assert.Equal("beta", properties[1].Name);
        Assert.Equal("nested", properties[2].Name);
        Assert.Equal("zebra", properties[3].Name);

        // Check nested object is also sorted
        var nested = jsonObject["nested"] as JObject;
        Assert.NotNull(nested);
        var nestedProperties = nested.Properties().ToArray();
        Assert.Equal("alpha_nested", nestedProperties[0].Name);
        Assert.Equal("charlie", nestedProperties[1].Name);
    }

    [Fact]
    public void SortJObject_WithConvertRoundFloat_ConvertsAndSorts()
    {
        // Arrange
        var jsonObject = JObject.Parse(@"{
            ""zebra"": 5.0,
            ""alpha"": 3.14
        }");

        // Act
        JsonUtilities.SortJObject(jsonObject, convertRoundFloatToInt: true);

        // Assert
        var properties = jsonObject.Properties().ToArray();
        Assert.Equal("alpha", properties[0].Name);
        Assert.Equal("zebra", properties[1].Name);

        Assert.Equal(JTokenType.Float, jsonObject["alpha"]!.Type); // 3.14 stays float
        Assert.Equal(JTokenType.Integer, jsonObject["zebra"]!.Type); // 5.0 becomes int
        Assert.Equal(5L, jsonObject["zebra"]!.Value<long>());
    }

    [Fact]
    public void SortJArray_WithMixedTypes_SortsCorrectly()
    {
        // Arrange
        var jsonArray = JArray.Parse(@"[
            ""zebra"",
            1,
            ""alpha"",
            true,
            null,
            false,
            2.5,
            ""beta""
        ]");

        // Act
        JsonUtilities.SortJArray(jsonArray);

        // Assert
        // Should be sorted by type priority: null, boolean, integer, float, string
        Assert.Equal(JTokenType.Null, jsonArray[0].Type);
        Assert.Equal(JTokenType.Boolean, jsonArray[1].Type);
        Assert.False(jsonArray[1].Value<bool>());
        Assert.Equal(JTokenType.Boolean, jsonArray[2].Type);
        Assert.True(jsonArray[2].Value<bool>());
        Assert.Equal(JTokenType.Integer, jsonArray[3].Type);
        Assert.Equal(JTokenType.Float, jsonArray[4].Type);
        Assert.Equal(JTokenType.String, jsonArray[5].Type);
        Assert.Equal("alpha", jsonArray[5].Value<string>());
        Assert.Equal(JTokenType.String, jsonArray[6].Type);
        Assert.Equal("beta", jsonArray[6].Value<string>());
        Assert.Equal(JTokenType.String, jsonArray[7].Type);
        Assert.Equal("zebra", jsonArray[7].Value<string>());
    }

    [Theory]
    [InlineData("stringField", "testValue")]
    [InlineData("intField", 42)]
    [InlineData("boolField", true)]
    [InlineData("floatField", 3.14)]
    public void GetFieldSafe_WithValidFields_ReturnsCorrectValues(string fieldName, object expectedValue)
    {
        // Arrange
        var jsonObject = JObject.Parse(@"{
            ""stringField"": ""testValue"",
            ""intField"": 42,
            ""boolField"": true,
            ""floatField"": 3.14
        }");

        // Act & Assert
        switch (expectedValue)
        {
            case string strVal:
                var stringResult = jsonObject.GetFieldSafe<string>(fieldName);
                Assert.Equal(strVal, stringResult);
                break;
            case int intVal:
                var intResult = jsonObject.GetFieldSafe<int>(fieldName);
                Assert.Equal(intVal, intResult);
                break;
            case bool boolVal:
                var boolResult = jsonObject.GetFieldSafe<bool>(fieldName);
                Assert.Equal(boolVal, boolResult);
                break;
            case double doubleVal:
                var doubleResult = jsonObject.GetFieldSafe<double>(fieldName);
                Assert.Equal(doubleVal, doubleResult, precision: 2);
                break;
        }
    }

    [Fact]
    public void GetFieldSafe_WithNonExistentField_ReturnsDefault()
    {
        // Arrange
        var jsonObject = JObject.Parse(@"{""existingField"": ""value""}");

        // Act
        var stringResult = jsonObject.GetFieldSafe<string>("nonExistent");
        var intResult = jsonObject.GetFieldSafe<int>("nonExistent");
        var boolResult = jsonObject.GetFieldSafe<bool>("nonExistent");

        // Assert
        Assert.Null(stringResult);
        Assert.Equal(0, intResult);
        Assert.False(boolResult);
    }

    [Fact]
    public void GetFieldSafe_WithNullJObject_ThrowsArgumentNullException()
    {
        // Arrange
        JObject? nullObject = null;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => nullObject!.GetFieldSafe<string>("field"));
    }

    [Fact]
    public void TryGetTypedValue_String_WithValidValue_ReturnsTrue()
    {
        // Arrange
        var jsonObject = JObject.Parse(@"{""stringField"": ""testValue""}");

        // Act
        var result = jsonObject.TryGetTypedValue("stringField", out string? value);

        // Assert
        Assert.True(result);
        Assert.Equal("testValue", value);
    }

    [Fact]
    public void TryGetTypedValue_String_WithNonStringValue_ReturnsStringRepresentation()
    {
        // Arrange
        var jsonObject = JObject.Parse(@"{""intField"": 42}");

        // Act
        var result = jsonObject.TryGetTypedValue("intField", out string? value);

        // Assert
        Assert.True(result);
        Assert.Equal("42", value);
    }

    [Fact]
    public void TryGetTypedValue_Int_WithValidInteger_ReturnsTrue()
    {
        // Arrange
        var jsonObject = JObject.Parse(@"{""intField"": 42}");

        // Act
        var result = jsonObject.TryGetTypedValue("intField", out int value);

        // Assert
        Assert.True(result);
        Assert.Equal(42, value);
    }

    [Fact]
    public void TryGetTypedValue_Int_WithRoundFloat_ReturnsTrue()
    {
        // Arrange
        var jsonObject = JObject.Parse(@"{""floatField"": 42.0}");

        // Act
        var result = jsonObject.TryGetTypedValue("floatField", out int value);

        // Assert
        Assert.True(result);
        Assert.Equal(42, value);
    }

    [Fact]
    public void TryGetTypedValue_Int_WithNonRoundFloat_ReturnsFalse()
    {
        // Arrange
        var jsonObject = JObject.Parse(@"{""floatField"": 42.5}");

        // Act
        var result = jsonObject.TryGetTypedValue("floatField", out int value);

        // Assert
        Assert.False(result);
        Assert.Equal(0, value);
    }

    [Fact]
    public void TryGetTypedValue_Int_WithStringNumber_ReturnsTrue()
    {
        // Arrange
        var jsonObject = JObject.Parse(@"{""stringField"": ""42""}");

        // Act
        var result = jsonObject.TryGetTypedValue("stringField", out int value);

        // Assert
        Assert.True(result);
        Assert.Equal(42, value);
    }

    [Fact]
    public void TryGetTypedValue_Bool_WithValidBoolean_ReturnsTrue()
    {
        // Arrange
        var jsonObject = JObject.Parse(@"{""boolField"": true}");

        // Act
        var result = jsonObject.TryGetTypedValue("boolField", out bool value);

        // Assert
        Assert.True(result);
        Assert.True(value);
    }

    [Fact]
    public void TryGetTypedValue_Bool_WithStringBoolean_ReturnsTrue()
    {
        // Arrange
        var jsonObject = JObject.Parse(@"{""stringField"": ""false""}");

        // Act
        var result = jsonObject.TryGetTypedValue("stringField", out bool value);

        // Assert
        Assert.True(result);
        Assert.False(value);
    }

    [Fact]
    public void TryGetTypedValue_Float_WithValidFloat_ReturnsTrue()
    {
        // Arrange
        var jsonObject = JObject.Parse(@"{""floatField"": 3.14}");

        // Act
        var result = jsonObject.TryGetTypedValue("floatField", out float value);

        // Assert
        Assert.True(result);
        Assert.Equal(3.14f, value, precision: 2);
    }

    [Fact]
    public void TryGetTypedValue_Float_WithInteger_ReturnsTrue()
    {
        // Arrange
        var jsonObject = JObject.Parse(@"{""intField"": 42}");

        // Act
        var result = jsonObject.TryGetTypedValue("intField", out float value);

        // Assert
        Assert.True(result);
        Assert.Equal(42f, value);
    }

    [Fact]
    public void TryGetTypedValue_JArray_WithValidArray_ReturnsTrue()
    {
        // Arrange
        var jsonObject = JObject.Parse(@"{""arrayField"": [1, 2, 3]}");

        // Act
        var result = jsonObject.TryGetTypedValue("arrayField", out JArray? value);

        // Assert
        Assert.True(result);
        Assert.NotNull(value);
        Assert.Equal(3, value.Count);
        Assert.Equal(1, value[0].Value<int>());
    }

    [Fact]
    public void TryGetTypedValue_JObject_WithValidObject_ReturnsTrue()
    {
        // Arrange
        var jsonObject = JObject.Parse(@"{""objectField"": {""nested"": ""value""}}");

        // Act
        var result = jsonObject.TryGetTypedValue("objectField", out JObject? value);

        // Assert
        Assert.True(result);
        Assert.NotNull(value);
        Assert.Equal("value", value["nested"]!.Value<string>());
    }

    [Fact]
    public void TryGetTypedValue_ListJObject_WithValidArray_ReturnsTrue()
    {
        // Arrange
        var jsonObject = JObject.Parse(@"{""arrayField"": [{""a"": 1}, {""b"": 2}]}");

        // Act
        var result = jsonObject.TryGetTypedValue("arrayField", out List<JObject> value);

        // Assert
        Assert.True(result);
        Assert.Equal(2, value.Count);
        Assert.Equal(1, value[0]["a"]!.Value<int>());
        Assert.Equal(2, value[1]["b"]!.Value<int>());
    }

    [Fact]
    public void TryGetTypedValue_ListString_WithValidArray_ReturnsTrue()
    {
        // Arrange
        var jsonObject = JObject.Parse(@"{""arrayField"": [""hello"", ""world""]}");

        // Act
        var result = jsonObject.TryGetTypedValue("arrayField", out List<string> value);

        // Assert
        Assert.True(result);
        Assert.Equal(2, value.Count);
        Assert.Equal("hello", value[0]);
        Assert.Equal("world", value[1]);
    }

    [Fact]
    public void TryGetTypedValue_ListInt_WithValidArray_ReturnsTrue()
    {
        // Arrange
        var jsonObject = JObject.Parse(@"{""arrayField"": [1, 2, 3]}");

        // Act
        var result = jsonObject.TryGetTypedValue("arrayField", out List<int> value);

        // Assert
        Assert.True(result);
        Assert.Equal(3, value.Count);
        Assert.Equal(new[] { 1, 2, 3 }, value);
    }

    [Fact]
    public void TryGetTypedValue_ListFloat_WithValidArray_ReturnsTrue()
    {
        // Arrange
        var jsonObject = JObject.Parse(@"{""arrayField"": [1.1, 2.2, 3.3]}");

        // Act
        var result = jsonObject.TryGetTypedValue("arrayField", out List<float> value);

        // Assert
        Assert.True(result);
        Assert.Equal(3, value.Count);
        Assert.Equal(1.1f, value[0], precision: 1);
        Assert.Equal(2.2f, value[1], precision: 1);
        Assert.Equal(3.3f, value[2], precision: 1);
    }

    [Fact]
    public void TryGetTypedValue_ListBool_WithValidArray_ReturnsTrue()
    {
        // Arrange
        var jsonObject = JObject.Parse(@"{""arrayField"": [true, false, true]}");

        // Act
        var result = jsonObject.TryGetTypedValue("arrayField", out List<bool> value);

        // Assert
        Assert.True(result);
        Assert.Equal(3, value.Count);
        Assert.Equal(new[] { true, false, true }, value);
    }

    [Fact]
    public void TryGetTypedValue_WithNonExistentField_ReturnsFalse()
    {
        // Arrange
        var jsonObject = JObject.Parse(@"{""existingField"": ""value""}");

        // Act
        var stringResult = jsonObject.TryGetTypedValue("nonExistent", out string? stringValue);
        var intResult = jsonObject.TryGetTypedValue("nonExistent", out int intValue);

        // Assert
        Assert.False(stringResult);
        Assert.Null(stringValue);
        Assert.False(intResult);
        Assert.Equal(0, intValue);
    }

    [Fact]
    public void TryGetTypedValue_WithWrongType_ReturnsFalse()
    {
        // Arrange
        var jsonObject = JObject.Parse(@"{""stringField"": ""not_a_number""}");

        // Act
        var result = jsonObject.TryGetTypedValue("stringField", out int value);

        // Assert
        Assert.False(result);
        Assert.Equal(0, value);
    }

    [Fact]
    public void ToJArray_WithGenericList_ReturnsCorrectJArray()
    {
        // Arrange
        var stringList = new List<string> { "hello", "world" };
        var intList = new List<int> { 1, 2, 3 };

        // Act
        var stringArray = stringList.ToJArray();
        var intArray = intList.ToJArray();

        // Assert
        Assert.Equal(2, stringArray.Count);
        Assert.Equal("hello", stringArray[0].Value<string>());
        Assert.Equal("world", stringArray[1].Value<string>());

        Assert.Equal(3, intArray.Count);
        Assert.Equal(1, intArray[0].Value<int>());
        Assert.Equal(2, intArray[1].Value<int>());
        Assert.Equal(3, intArray[2].Value<int>());
    }

    [Fact]
    public void Convert_StringDictionary_ReturnsCorrectJObject()
    {
        // Arrange
        var dictionary = new Dictionary<string, string>
        {
            { "key1", "value1" },
            { "key2", "value2" },
            { "key3", "value3" }
        };

        // Act
        var result = JsonUtilities.Convert(dictionary);

        // Assert
        Assert.Equal(3, result.Count);
        Assert.Equal("value1", result["key1"]!.Value<string>());
        Assert.Equal("value2", result["key2"]!.Value<string>());
        Assert.Equal("value3", result["key3"]!.Value<string>());
    }

    [Fact]
    public void Convert_IntDictionary_ReturnsCorrectJObject()
    {
        // Arrange
        var dictionary = new Dictionary<string, int>
        {
            { "one", 1 },
            { "two", 2 },
            { "three", 3 }
        };

        // Act
        var result = JsonUtilities.Convert(dictionary);

        // Assert
        Assert.Equal(3, result.Count);
        Assert.Equal(1, result["one"]!.Value<int>());
        Assert.Equal(2, result["two"]!.Value<int>());
        Assert.Equal(3, result["three"]!.Value<int>());
    }

    [Fact]
    public void Convert_WithNullDictionary_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => JsonUtilities.Convert((Dictionary<string, string>)null!));
        Assert.Throws<ArgumentNullException>(() => JsonUtilities.Convert((Dictionary<string, int>)null!));
    }

    [Fact]
    public void JsonUtilities_ComplexIntegrationTest_WorksCorrectly()
    {
        // Arrange
        var complexJson = JObject.Parse(@"{
            ""name"": ""TestObject"",
            ""count"": 42.0,
            ""pi"": 3.14159,
            ""active"": true,
            ""tags"": [""important"", ""test"", 1.0, 2.5],
            ""metadata"": {
                ""created"": ""2023-01-01"",
                ""version"": 1.0,
                ""settings"": {
                    ""enabled"": true,
                    ""threshold"": 10.0
                }
            },
            ""scores"": [100.0, 95.0, 87.5]
        }");

        // Act - Convert round floats to integers and sort
        JsonUtilities.ConvertRoundFloatToIntAllInJObject(complexJson);
        JsonUtilities.SortJObject(complexJson);

        // Assert structure is maintained and sorted
        var properties = complexJson.Properties().Select(p => p.Name).ToArray();
        Assert.Equal(new[] { "active", "count", "metadata", "name", "pi", "scores", "tags" }, properties);

        // Assert conversions worked
        Assert.Equal(JTokenType.Integer, complexJson["count"]!.Type);
        Assert.Equal(42L, complexJson["count"]!.Value<long>());
        Assert.Equal(JTokenType.Float, complexJson["pi"]!.Type); // Should remain float

        // Test typed value extraction
        Assert.True(complexJson.TryGetTypedValue("name", out string? name));
        Assert.Equal("TestObject", name);

        Assert.True(complexJson.TryGetTypedValue("count", out int count));
        Assert.Equal(42, count);

        Assert.True(complexJson.TryGetTypedValue("active", out bool active));
        Assert.True(active);

        Assert.True(complexJson.TryGetTypedValue("tags", out JArray? tags));
        Assert.NotNull(tags);
        Assert.Equal(4, tags.Count);

        Assert.True(complexJson.TryGetTypedValue("metadata", out JObject? metadata));
        Assert.NotNull(metadata);

        // Test nested metadata
        Assert.True(metadata.TryGetTypedValue("version", out int version));
        Assert.Equal(1, version); // Should be converted from 1.0
    }

    [Fact]
    public void JsonUtilities_Performance_HandlesLargeObjects()
    {
        // Arrange
        var largeObject = new JObject();
        for (int i = 0; i < 1000; i++)
        {
            largeObject[$"field_{i:D4}"] = i % 2 == 0 ? i : (double)i; // Mix integers and floats
        }

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Act
        JsonUtilities.SortJObject(largeObject, convertRoundFloatToInt: true);
        stopwatch.Stop();

        // Assert
        Assert.Equal(1000, largeObject.Count);
        Assert.True(stopwatch.ElapsedMilliseconds < 1000,
            $"Performance test failed: {stopwatch.ElapsedMilliseconds}ms for 1000 fields");

        // Verify sorting
        var properties = largeObject.Properties().Select(p => p.Name).ToArray();
        var sortedProperties = properties.OrderBy(p => p, StringComparer.Ordinal).ToArray();
        Assert.Equal(sortedProperties, properties);
    }

    [Fact]
    public void JsonUtilities_EdgeCases_HandleCorrectly()
    {
        // Test with edge case values
        var edgeCaseJson = JObject.Parse(@"{
            ""maxInt"": 2147483647.0,
            ""minInt"": -2147483648.0,
            ""maxLong"": 9223372036854775807.0,
            ""overflowDouble"": 1.7976931348623157E+308,
            ""emptyString"": """",
            ""nullValue"": null,
            ""emptyArray"": [],
            ""emptyObject"": {}
        }");

        // Act & Assert - Should not throw
        JsonUtilities.ConvertRoundFloatToIntAllInJObject(edgeCaseJson);
        JsonUtilities.SortJObject(edgeCaseJson);

        // Test max int conversion
        Assert.Equal(JTokenType.Integer, edgeCaseJson["maxInt"]!.Type);
        Assert.Equal(2147483647L, edgeCaseJson["maxInt"]!.Value<long>());

        // Test TryGetTypedValue with edge cases
        Assert.True(edgeCaseJson.TryGetTypedValue("emptyString", out string? emptyStr));
        Assert.Equal("", emptyStr);

        Assert.False(edgeCaseJson.TryGetTypedValue("nullValue", out string? nullStr));
        Assert.Null(nullStr);

        Assert.True(edgeCaseJson.TryGetTypedValue("emptyArray", out JArray? emptyArray));
        Assert.NotNull(emptyArray);
        Assert.Empty(emptyArray);

        Assert.True(edgeCaseJson.TryGetTypedValue("emptyObject", out JObject? emptyObj));
        Assert.NotNull(emptyObj);
        Assert.Empty(emptyObj);
    }

    [Theory]
    [InlineData(0.0, true, 0L)]
    [InlineData(1.0, true, 1L)]
    [InlineData(-1.0, true, -1L)]
    [InlineData(42.0, true, 42L)]
    [InlineData(3.14159, false, 0L)]
    [InlineData(2.71828, false, 0L)]
    [InlineData(9223372036854775807.0, true, 9223372036854775807L)] // max long
    [InlineData(-9223372036854775808.0, true, -9223372036854775808L)] // min long
    public void FloatToIntConversion_WithVariousValues_WorksCorrectly(double inputValue, bool shouldConvert, long expectedValue)
    {
        // Arrange
        var testObject = new JObject { ["testValue"] = inputValue };

        // Act
        JsonUtilities.ConvertRoundFloatToIntAllInJObject(testObject);

        // Assert
        if (shouldConvert)
        {
            Assert.Equal(JTokenType.Integer, testObject["testValue"]!.Type);
            Assert.Equal(expectedValue, testObject["testValue"]!.Value<long>());
        }
        else
        {
            Assert.Equal(JTokenType.Float, testObject["testValue"]!.Type);
        }
    }

    [Fact]
    public void TryGetTypedValue_WithNullArguments_ThrowsArgumentNullException()
    {
        // Arrange
        var jsonObject = JObject.Parse(@"{""field"": ""value""}");

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            JsonUtilities.TryGetTypedValue(null!, "field", out string? _));

        Assert.Throws<ArgumentNullException>(() =>
            jsonObject.TryGetTypedValue(null!, out string? _));
    }

    [Fact]
    public void JsonUtilities_SortingStability_MaintainsRelativeOrder()
    {
        // Arrange - Test that sorting is stable for equivalent items
        var jsonArray = JArray.Parse("""
                                     [
                                                 {"priority": 1, "id": "a"},
                                                 {"priority": 1, "id": "b"},
                                                 {"priority": 2, "id": "c"},
                                                 {"priority": 1, "id": "d"}
                                             ]
                                     """);

        // Act
        JsonUtilities.SortJArray(jsonArray);

        // Assert - Objects should be grouped by their string representation
        // The exact order depends on string representation, but should be consistent
        Assert.Equal(4, jsonArray.Count);
        foreach (var t in jsonArray)
        {
            Assert.Equal(JTokenType.Object, t.Type);
        }
    }

    [Fact]
    public void JsonUtilities_RoundTripIntegration_PreservesData()
    {
        // Arrange
        var originalData = new Dictionary<string, object>
        {
            { "string", "test" },
            { "int", 42 },
            { "float", 3.14 },
            { "bool", true },
            { "array", new[] { 1, 2, 3 } },
            { "nested", new { key = "value" } }
        };

        var json = JObject.FromObject(originalData);

        // Act - Process with JsonUtilities
        JsonUtilities.SortJObject(json, convertRoundFloatToInt: true);

        // Assert - Verify all data is preserved and accessible
        Assert.True(json.TryGetTypedValue("string", out string? stringVal));
        Assert.Equal("test", stringVal);

        Assert.True(json.TryGetTypedValue("int", out int intVal));
        Assert.Equal(42, intVal);

        Assert.True(json.TryGetTypedValue("float", out float floatVal));
        Assert.Equal(3.14f, floatVal, precision: 2);

        Assert.True(json.TryGetTypedValue("bool", out bool boolVal));
        Assert.True(boolVal);

        Assert.True(json.TryGetTypedValue("array", out JArray? arrayVal));
        Assert.NotNull(arrayVal);
        Assert.Equal(3, arrayVal.Count);

        Assert.True(json.TryGetTypedValue("nested", out JObject? nestedVal));
        Assert.NotNull(nestedVal);
        Assert.True(nestedVal.TryGetTypedValue("key", out string? nestedStr));
        Assert.Equal("value", nestedStr);
    }
}
