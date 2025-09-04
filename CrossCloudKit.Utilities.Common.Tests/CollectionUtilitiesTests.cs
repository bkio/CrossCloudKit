// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Xunit;

namespace CrossCloudKit.Utilities.Common.Tests;

public class CollectionUtilitiesTests
{
    [Fact]
    public void GetFirstValidString_WithValidStrings_ReturnsFirstValid()
    {
        // Arrange
        var list = new List<string?> { null, "", "first", "second", "third" };

        // Act
        var result = CollectionUtilities.GetFirstValidString(list);

        // Assert
        Assert.Equal("first", result);
    }

    [Fact]
    public void GetFirstValidString_WithEmptyList_ReturnsNull()
    {
        // Arrange
        // ReSharper disable once CollectionNeverUpdated.Local
        var list = new List<string?>();

        // Act
        var result = CollectionUtilities.GetFirstValidString(list);

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public void GetFirstValidString_WithAllNullOrEmpty_ReturnsNull()
    {
        // Arrange
        var list = new List<string?> { null, "", null, "" };

        // Act
        var result = CollectionUtilities.GetFirstValidString(list);

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public void GetFirstValidString_WithWhitespaceString_ReturnsWhitespace()
    {
        // Arrange
        var list = new List<string?> { null, "", "   ", "valid" };

        // Act
        var result = CollectionUtilities.GetFirstValidString(list);

        // Assert
        Assert.Equal("   ", result);
    }

    [Fact]
    public void GetFirstValidString_WithSingleValidString_ReturnsThatString()
    {
        // Arrange
        var list = new List<string?> { "only valid string" };

        // Act
        var result = CollectionUtilities.GetFirstValidString(list);

        // Assert
        Assert.Equal("only valid string", result);
    }

    [Fact]
    public void GetFirstValidString_WithMixedContent_ReturnsFirstNonEmptyNonNull()
    {
        // Arrange
        var list = new List<string?> { "", null, "0", "1", null };

        // Act
        var result = CollectionUtilities.GetFirstValidString(list);

        // Assert
        Assert.Equal("0", result);
    }

    [Theory]
    [InlineData(new[] { "a", "b", "c" }, "a")]
    [InlineData(new[] { null, "b", "c" }, "b")]
    [InlineData(new[] { "", "b", "c" }, "b")]
    [InlineData(new[] { null, "", "c" }, "c")]
    public void GetFirstValidString_WithTheoryData_ReturnsExpectedResult(string?[] input, string expected)
    {
        // Act
        var result = CollectionUtilities.GetFirstValidString(input);

        // Assert
        Assert.Equal(expected, result);
    }

    [Fact]
    public void GetValueByKey_Collection_WithExistingKey_ReturnsValue()
    {
        // Arrange
        var pairs = new List<(string Key, string Value)>
        {
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3")
        };

        // Act
        var result = CollectionUtilities.GetValueByKey(pairs, "key2");

        // Assert
        Assert.Equal("value2", result);
    }

    [Fact]
    public void GetValueByKey_Collection_WithNonExistingKey_ReturnsNull()
    {
        // Arrange
        var pairs = new List<(string Key, string Value)>
        {
            ("key1", "value1"),
            ("key2", "value2")
        };

        // Act
        var result = CollectionUtilities.GetValueByKey(pairs, "nonexistent");

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public void GetValueByKey_Collection_WithEmptyCollection_ReturnsNull()
    {
        // Arrange
        // ReSharper disable once CollectionNeverUpdated.Local
        var pairs = new List<(string Key, string Value)>();

        // Act
        var result = CollectionUtilities.GetValueByKey(pairs, "anykey");

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public void GetValueByKey_Collection_WithDuplicateKeys_ReturnsFirstValue()
    {
        // Arrange
        var pairs = new List<(string Key, string Value)>
        {
            ("duplicate", "first"),
            ("duplicate", "second"),
            ("other", "value")
        };

        // Act
        var result = CollectionUtilities.GetValueByKey(pairs, "duplicate");

        // Assert
        Assert.Equal("first", result);
    }

    [Fact]
    public void GetValueByKey_Collection_WithNullValue_ReturnsNull()
    {
        // Arrange
        var pairs = new List<(string Key, string Value)>
        {
            ("key1", "value1"),
            ("keynull", null!),
            ("key3", "value3")
        };

        // Act
        var result = CollectionUtilities.GetValueByKey(pairs, "keynull");

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public void GetValueByKey_Array_WithExistingKey_ReturnsValue()
    {
        // Arrange
        var pairs = new (string Key, string Value)[]
        {
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3")
        };

        // Act
        var result = CollectionUtilities.GetValueByKey(pairs, "key2");

        // Assert
        Assert.Equal("value2", result);
    }

    [Fact]
    public void GetValueByKey_Array_WithNonExistingKey_ReturnsNull()
    {
        // Arrange
        var pairs = new (string Key, string Value)[]
        {
            ("key1", "value1"),
            ("key2", "value2")
        };

        // Act
        var result = CollectionUtilities.GetValueByKey(pairs, "nonexistent");

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public void GetValueByKey_Array_WithEmptyArray_ReturnsNull()
    {
        // Arrange
        var pairs = Array.Empty<(string Key, string Value)>();

        // Act
        var result = CollectionUtilities.GetValueByKey(pairs, "anykey");

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public void GetValueByKey_Array_WithDuplicateKeys_ReturnsFirstValue()
    {
        // Arrange
        var pairs = new (string Key, string Value)[]
        {
            ("duplicate", "first"),
            ("duplicate", "second"),
            ("other", "value")
        };

        // Act
        var result = CollectionUtilities.GetValueByKey(pairs, "duplicate");

        // Assert
        Assert.Equal("first", result);
    }

    [Fact]
    public void GetValueByKey_Array_WithNullValue_ReturnsNull()
    {
        // Arrange
        var pairs = new (string Key, string Value)[]
        {
            ("key1", "value1"),
            ("keynull", null!),
            ("key3", "value3")
        };

        // Act
        var result = CollectionUtilities.GetValueByKey(pairs, "keynull");

        // Assert
        Assert.Null(result);
    }

    [Theory]
    [InlineData("key1", "value1")]
    [InlineData("key2", "value2")]
    [InlineData("key3", "value3")]
    [InlineData("nonexistent", null)]
    public void GetValueByKey_Collection_WithTheoryData_ReturnsExpectedResult(string key, string? expected)
    {
        // Arrange
        var pairs = new List<(string Key, string Value)>
        {
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3")
        };

        // Act
        var result = CollectionUtilities.GetValueByKey(pairs, key);

        // Assert
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData("key1", "value1")]
    [InlineData("key2", "value2")]
    [InlineData("key3", "value3")]
    [InlineData("nonexistent", null)]
    public void GetValueByKey_Array_WithTheoryData_ReturnsExpectedResult(string key, string? expected)
    {
        // Arrange
        var pairs = new (string Key, string Value)[]
        {
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3")
        };

        // Act
        var result = CollectionUtilities.GetValueByKey(pairs, key);

        // Assert
        Assert.Equal(expected, result);
    }

    [Fact]
    public void GetValueByKey_Collection_CaseSensitive_ReturnsCorrectValue()
    {
        // Arrange
        var pairs = new List<(string Key, string Value)>
        {
            ("Key", "uppercase"),
            ("key", "lowercase"),
            ("KEY", "alluppercase")
        };

        // Act
        var resultUpper = CollectionUtilities.GetValueByKey(pairs, "Key");
        var resultLower = CollectionUtilities.GetValueByKey(pairs, "key");
        var resultAllUpper = CollectionUtilities.GetValueByKey(pairs, "KEY");

        // Assert
        Assert.Equal("uppercase", resultUpper);
        Assert.Equal("lowercase", resultLower);
        Assert.Equal("alluppercase", resultAllUpper);
    }

    [Fact]
    public void GetValueByKey_Array_CaseSensitive_ReturnsCorrectValue()
    {
        // Arrange
        var pairs = new (string Key, string Value)[]
        {
            ("Key", "uppercase"),
            ("key", "lowercase"),
            ("KEY", "alluppercase")
        };

        // Act
        var resultUpper = CollectionUtilities.GetValueByKey(pairs, "Key");
        var resultLower = CollectionUtilities.GetValueByKey(pairs, "key");
        var resultAllUpper = CollectionUtilities.GetValueByKey(pairs, "KEY");

        // Assert
        Assert.Equal("uppercase", resultUpper);
        Assert.Equal("lowercase", resultLower);
        Assert.Equal("alluppercase", resultAllUpper);
    }

    [Fact]
    public void GetFirstValidString_WithLargeCollection_PerformsEfficiently()
    {
        // Arrange
        var largeList = new List<string?>(10000);
        for (int i = 0; i < 9999; i++)
        {
            largeList.Add(null);
        }
        largeList.Add("found");

        // Act
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var result = CollectionUtilities.GetFirstValidString(largeList);
        stopwatch.Stop();

        // Assert
        Assert.Equal("found", result);
        Assert.True(stopwatch.ElapsedMilliseconds < 100); // Should be very fast
    }

    [Fact]
    public void GetValueByKey_Collection_WithLargeCollection_PerformsEfficiently()
    {
        // Arrange
        var largePairs = new List<(string Key, string Value)>(10000);
        for (int i = 0; i < 9999; i++)
        {
            largePairs.Add(($"key{i}", $"value{i}"));
        }
        largePairs.Add(("target", "found"));

        // Act
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var result = CollectionUtilities.GetValueByKey(largePairs, "target");
        stopwatch.Stop();

        // Assert
        Assert.Equal("found", result);
        Assert.True(stopwatch.ElapsedMilliseconds < 100); // Should be reasonably fast
    }

    [Fact]
    public void GetFirstValidString_WithIEnumerableImplementation_WorksCorrectly()
    {
        // Arrange
        var enumerable = GetStringEnumerable();

        // Act
        var result = CollectionUtilities.GetFirstValidString(enumerable);

        // Assert
        Assert.Equal("third", result);
    }

    [Fact]
    public void GetValueByKey_Collection_WithIEnumerableImplementation_WorksCorrectly()
    {
        // Arrange
        var enumerable = GetPairEnumerable();

        // Act
        var result = CollectionUtilities.GetValueByKey(enumerable, "key2");

        // Assert
        Assert.Equal("value2", result);
    }

    private static IEnumerable<string?> GetStringEnumerable()
    {
        yield return null;
        yield return "";
        yield return "third";
        yield return "fourth";
    }

    private static IEnumerable<(string Key, string Value)> GetPairEnumerable()
    {
        yield return ("key1", "value1");
        yield return ("key2", "value2");
        yield return ("key3", "value3");
    }
}
