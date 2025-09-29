// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Newtonsoft.Json;
using Xunit;

namespace CrossCloudKit.Utilities.Common.Tests;

public class PrimitiveTypeTests
{
    [Fact]
    public void PrimitiveType_StringConstructor_InitializesCorrectly()
    {
        // Arrange
        const string testValue = "test string";

        // Act
        var primitiveType = new PrimitiveType(testValue);

        // Assert
        Assert.Equal(PrimitiveTypeKind.String, primitiveType.Kind);
        Assert.Equal(testValue, primitiveType.AsString);
    }

    [Fact]
    public void PrimitiveType_IntegerConstructor_InitializesCorrectly()
    {
        // Arrange
        const long testValue = 42L;

        // Act
        var primitiveType = new PrimitiveType(testValue);

        // Assert
        Assert.Equal(PrimitiveTypeKind.Integer, primitiveType.Kind);
        Assert.Equal(testValue, primitiveType.AsInteger);
    }

    [Fact]
    public void PrimitiveType_DoubleConstructor_InitializesCorrectly()
    {
        // Arrange
        const double testValue = 3.14159;

        // Act
        var primitiveType = new PrimitiveType(testValue);

        // Assert
        Assert.Equal(PrimitiveTypeKind.Double, primitiveType.Kind);
        Assert.Equal(testValue, primitiveType.AsDouble);
    }

    [Fact]
    public void PrimitiveType_BooleanConstructor_InitializesCorrectly()
    {
        // Arrange
        const bool testValue = true;

        // Act
        var primitiveType = new PrimitiveType(testValue);

        // Assert
        Assert.Equal(PrimitiveTypeKind.Boolean, primitiveType.Kind);
        Assert.Equal(testValue, primitiveType.AsBoolean);
    }

    [Fact]
    public void PrimitiveType_ByteArrayConstructor_InitializesCorrectly()
    {
        // Arrange
        var testValue = new byte[] { 1, 2, 3, 4, 5 };

        // Act
        var primitiveType = new PrimitiveType(testValue);

        // Assert
        Assert.Equal(PrimitiveTypeKind.ByteArray, primitiveType.Kind);
        Assert.Equal(testValue, primitiveType.AsByteArray);
    }

    [Fact]
    public void PrimitiveType_ByteArrayConstructor_CreatesIndependentCopy()
    {
        // Arrange
        var originalArray = new byte[] { 1, 2, 3 };

        // Act
        var primitiveType = new PrimitiveType(originalArray);
        originalArray[0] = 99; // Modify original

        // Assert
        var retrievedArray = primitiveType.AsByteArray;
        Assert.Equal(1, retrievedArray[0]); // Should not be affected by original modification
    }

    [Fact]
    public void PrimitiveType_SpanConstructor_InitializesCorrectly()
    {
        // Arrange
        var testValue = new byte[] { 10, 20, 30, 40 };
        var span = testValue.AsSpan();

        // Act
        var primitiveType = new PrimitiveType(span);

        // Assert
        Assert.Equal(PrimitiveTypeKind.ByteArray, primitiveType.Kind);
        Assert.Equal(testValue, primitiveType.AsByteArray);
    }

    [Fact]
    public void PrimitiveType_CopyConstructor_CreatesExactCopy()
    {
        // Arrange
        var original = new PrimitiveType("test value");

        // Act
        var copy = new PrimitiveType(original);

        // Assert
        Assert.Equal(original.Kind, copy.Kind);
        Assert.Equal(original.AsString, copy.AsString);
        Assert.Equal(original, copy);
    }

    [Fact]
    public void PrimitiveType_CopyConstructor_WithByteArray_CreatesIndependentCopy()
    {
        // Arrange
        var originalBytes = new byte[] { 1, 2, 3, 4 };
        var original = new PrimitiveType(originalBytes);

        // Act
        var copy = new PrimitiveType(original);

        // Modify original's internal array indirectly (though this shouldn't be possible due to immutability)
        var retrievedOriginal = original.AsByteArray;
        var retrievedCopy = copy.AsByteArray;

        // Assert
        Assert.Equal(original.Kind, copy.Kind);
        Assert.Equal(retrievedOriginal, retrievedCopy);
        Assert.NotSame(retrievedOriginal, retrievedCopy); // Different array instances
    }

    [Fact]
    public void PrimitiveType_AsString_WithWrongType_ThrowsInvalidOperationException()
    {
        // Arrange
        var primitiveType = new PrimitiveType(42L);

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => primitiveType.AsString);
        Assert.Contains("Cannot access string value when Kind is Integer", exception.Message);
    }

    [Fact]
    public void PrimitiveType_AsInteger_WithWrongType_ThrowsInvalidOperationException()
    {
        // Arrange
        var primitiveType = new PrimitiveType("test");

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => primitiveType.AsInteger);
        Assert.Contains("Cannot access integer value when Kind is String", exception.Message);
    }

    [Fact]
    public void PrimitiveType_AsDouble_WithWrongType_ThrowsInvalidOperationException()
    {
        // Arrange
        var primitiveType = new PrimitiveType(42L);

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => primitiveType.AsDouble);
        Assert.Contains("Cannot access double value when Kind is Integer", exception.Message);
    }

    [Fact]
    public void PrimitiveType_AsByteArray_WithWrongType_ThrowsInvalidOperationException()
    {
        // Arrange
        var primitiveType = new PrimitiveType("test");

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => primitiveType.AsByteArray);
        Assert.Contains("Cannot access byte array value when Kind is String", exception.Message);
    }

    [Fact]
    public void PrimitiveType_AsBoolean_WithWrongType_ThrowsInvalidOperationException()
    {
        // Arrange
        var primitiveType = new PrimitiveType("test");

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => primitiveType.AsBoolean);
        Assert.Contains("Cannot access boolean value when Kind is String", exception.Message);
    }

    [Fact]
    public void PrimitiveType_TryGetString_WithStringType_ReturnsTrue()
    {
        // Arrange
        const string testValue = "test string";
        var primitiveType = new PrimitiveType(testValue);

        // Act
        var success = primitiveType.TryGetString(out var value);

        // Assert
        Assert.True(success);
        Assert.Equal(testValue, value);
    }

    [Fact]
    public void PrimitiveType_TryGetString_WithNonStringType_ReturnsFalse()
    {
        // Arrange
        var primitiveType = new PrimitiveType(42L);

        // Act
        var success = primitiveType.TryGetString(out var value);

        // Assert
        Assert.False(success);
        Assert.Null(value);
    }

    [Fact]
    public void PrimitiveType_TryGetInteger_WithIntegerType_ReturnsTrue()
    {
        // Arrange
        const long testValue = 123L;
        var primitiveType = new PrimitiveType(testValue);

        // Act
        var success = primitiveType.TryGetInteger(out var value);

        // Assert
        Assert.True(success);
        Assert.Equal(testValue, value);
    }

    [Fact]
    public void PrimitiveType_TryGetInteger_WithNonIntegerType_ReturnsFalse()
    {
        // Arrange
        var primitiveType = new PrimitiveType("test");

        // Act
        var success = primitiveType.TryGetInteger(out var value);

        // Assert
        Assert.False(success);
        Assert.Equal(0L, value);
    }

    [Fact]
    public void PrimitiveType_TryGetDouble_WithDoubleType_ReturnsTrue()
    {
        // Arrange
        const double testValue = 2.71828;
        var primitiveType = new PrimitiveType(testValue);

        // Act
        var success = primitiveType.TryGetDouble(out var value);

        // Assert
        Assert.True(success);
        Assert.Equal(testValue, value);
    }

    [Fact]
    public void PrimitiveType_TryGetDouble_WithNonDoubleType_ReturnsFalse()
    {
        // Arrange
        var primitiveType = new PrimitiveType(42L);

        // Act
        var success = primitiveType.TryGetDouble(out var value);

        // Assert
        Assert.False(success);
        Assert.Equal(0.0, value);
    }

    [Fact]
    public void PrimitiveType_TryGetByteArray_WithByteArrayType_ReturnsTrue()
    {
        // Arrange
        var testValue = new byte[] { 5, 10, 15, 20 };
        var primitiveType = new PrimitiveType(testValue);

        // Act
        var success = primitiveType.TryGetByteArray(out var value);

        // Assert
        Assert.True(success);
        Assert.NotNull(value);
        Assert.Equal(testValue, value);
    }

    [Fact]
    public void PrimitiveType_TryGetByteArray_WithNonByteArrayType_ReturnsFalse()
    {
        // Arrange
        var primitiveType = new PrimitiveType("test");

        // Act
        var success = primitiveType.TryGetByteArray(out var value);

        // Assert
        Assert.False(success);
        Assert.Null(value);
    }

    [Fact]
    public void PrimitiveType_TryGetBoolean_WithBooleanType_ReturnsTrue()
    {
        // Arrange
        const bool testValue = false;
        var primitiveType = new PrimitiveType(testValue);

        // Act
        var success = primitiveType.TryGetBoolean(out var value);

        // Assert
        Assert.True(success);
        Assert.Equal(testValue, value);
    }

    [Fact]
    public void PrimitiveType_TryGetBoolean_WithNonBooleanType_ReturnsFalse()
    {
        // Arrange
        var primitiveType = new PrimitiveType("test");

        // Act
        var success = primitiveType.TryGetBoolean(out var value);

        // Assert
        Assert.False(success);
        Assert.False(value);
    }

    [Theory]
    [InlineData("Hello World")]
    [InlineData("")]
    [InlineData("Special characters: !@#$%^&*()")]
    [InlineData("Unicode: 世界 🌍")]
    public void PrimitiveType_ToString_WithString_ReturnsString(string testValue)
    {
        // Arrange
        var primitiveType = new PrimitiveType(testValue);

        // Act
        var result = primitiveType.ToString();

        // Assert
        Assert.Equal(testValue, result);
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(42L)]
    [InlineData(-123L)]
    [InlineData(long.MaxValue)]
    [InlineData(long.MinValue)]
    public void PrimitiveType_ToString_WithInteger_ReturnsStringRepresentation(long testValue)
    {
        // Arrange
        var primitiveType = new PrimitiveType(testValue);

        // Act
        var result = primitiveType.ToString();

        // Assert
        Assert.Equal(testValue.ToString(), result);
    }

    [Theory]
    [InlineData(0.0)]
    [InlineData(3.14159)]
    [InlineData(-2.71828)]
    [InlineData(double.MaxValue)]
    [InlineData(double.MinValue)]
    public void PrimitiveType_ToString_WithDouble_ReturnsInvariantCultureString(double testValue)
    {
        // Arrange
        var primitiveType = new PrimitiveType(testValue);
        var expected = testValue.ToString(System.Globalization.CultureInfo.InvariantCulture);

        // Act
        var result = primitiveType.ToString();

        // Assert
        Assert.Equal(expected, result);
    }

    [Fact]
    public void PrimitiveType_ToString_WithByteArray_ReturnsBase64String()
    {
        // Arrange
        var testValue = "Hello"u8.ToArray(); // "Hello" in ASCII
        var primitiveType = new PrimitiveType(testValue);
        var expectedBase64 = Convert.ToBase64String(testValue);

        // Act
        var result = primitiveType.ToString();

        // Assert
        Assert.Equal(expectedBase64, result);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void PrimitiveType_ToString_WithBoolean_ReturnsStringRepresentation(bool testValue)
    {
        // Arrange
        var primitiveType = new PrimitiveType(testValue);
        var expected = testValue.ToString();

        // Act
        var result = primitiveType.ToString();

        // Assert
        Assert.Equal(expected, result);
    }

    [Fact]
    public void PrimitiveType_Equals_WithSameStringValues_ReturnsTrue()
    {
        // Arrange
        const string testValue = "test value";
        var primitiveType1 = new PrimitiveType(testValue);
        var primitiveType2 = new PrimitiveType(testValue);

        // Act & Assert
        Assert.True(primitiveType1.Equals(primitiveType2));
        Assert.True(primitiveType1 == primitiveType2);
        Assert.False(primitiveType1 != primitiveType2);
    }

    [Fact]
    public void PrimitiveType_Equals_WithSameIntegerValues_ReturnsTrue()
    {
        // Arrange
        const long testValue = 42L;
        var primitiveType1 = new PrimitiveType(testValue);
        var primitiveType2 = new PrimitiveType(testValue);

        // Act & Assert
        Assert.True(primitiveType1.Equals(primitiveType2));
        Assert.True(primitiveType1 == primitiveType2);
        Assert.False(primitiveType1 != primitiveType2);
    }

    [Fact]
    public void PrimitiveType_Equals_WithSimilarDoubleValues_ReturnsTrue()
    {
        // Arrange - Using values that are very close (within tolerance)
        const double testValue1 = 3.14159;
        const double testValue2 = 3.14159000001; // Very close but not exactly equal
        var primitiveType1 = new PrimitiveType(testValue1);
        var primitiveType2 = new PrimitiveType(testValue2);

        // Act & Assert
        Assert.True(primitiveType1.Equals(primitiveType2));
    }

    [Fact]
    public void PrimitiveType_Equals_WithSameByteArrayValues_ReturnsTrue()
    {
        // Arrange
        var testValue1 = new byte[] { 1, 2, 3, 4 };
        var testValue2 = new byte[] { 1, 2, 3, 4 };
        var primitiveType1 = new PrimitiveType(testValue1);
        var primitiveType2 = new PrimitiveType(testValue2);

        // Act & Assert
        Assert.True(primitiveType1.Equals(primitiveType2));
    }

    [Fact]
    public void PrimitiveType_Equals_WithSameBooleanValues_ReturnsTrue()
    {
        // Arrange
        const bool testValue = true;
        var primitiveType1 = new PrimitiveType(testValue);
        var primitiveType2 = new PrimitiveType(testValue);

        // Act & Assert
        Assert.True(primitiveType1.Equals(primitiveType2));
        Assert.True(primitiveType1 == primitiveType2);
        Assert.False(primitiveType1 != primitiveType2);
    }

    [Fact]
    public void PrimitiveType_Equals_WithDifferentKinds_ReturnsFalse()
    {
        // Arrange
        var stringType = new PrimitiveType("42");
        var integerType = new PrimitiveType(42L);

        // Act & Assert
        Assert.False(stringType.Equals(integerType));
        Assert.False(stringType == integerType);
        Assert.True(stringType != integerType);
    }

    [Fact]
    public void PrimitiveType_Equals_WithNull_ReturnsFalse()
    {
        // Arrange
        var primitiveType = new PrimitiveType("test");

        // Act & Assert
        Assert.False(primitiveType.Equals(null));
        Assert.False(primitiveType == null);
        Assert.True(primitiveType != null);
    }

    [Fact]
    public void PrimitiveType_GetHashCode_WithEqualValues_ReturnsSameHashCode()
    {
        // Arrange
        const string testValue = "test value";
        var primitiveType1 = new PrimitiveType(testValue);
        var primitiveType2 = new PrimitiveType(testValue);

        // Act
        var hash1 = primitiveType1.GetHashCode();
        var hash2 = primitiveType2.GetHashCode();

        // Assert
        Assert.Equal(hash1, hash2);
    }

    [Fact]
    public void PrimitiveType_GetHashCode_WithDifferentValues_ReturnsDifferentHashCodes()
    {
        // Arrange
        var primitiveType1 = new PrimitiveType("value1");
        var primitiveType2 = new PrimitiveType("value2");

        // Act
        var hash1 = primitiveType1.GetHashCode();
        var hash2 = primitiveType2.GetHashCode();

        // Assert
        Assert.NotEqual(hash1, hash2);
    }

    [Fact]
    public void PrimitiveType_ImplicitConversion_FromString_WorksCorrectly()
    {
        // Arrange
        const string testValue = "implicit test";

        // Act
        PrimitiveType primitiveType = testValue;

        // Assert
        Assert.Equal(PrimitiveTypeKind.String, primitiveType.Kind);
        Assert.Equal(testValue, primitiveType.AsString);
    }

    [Fact]
    public void PrimitiveType_ImplicitConversion_FromLong_WorksCorrectly()
    {
        // Arrange
        const long testValue = 999L;

        // Act
        PrimitiveType primitiveType = testValue;

        // Assert
        Assert.Equal(PrimitiveTypeKind.Integer, primitiveType.Kind);
        Assert.Equal(testValue, primitiveType.AsInteger);
    }

    [Fact]
    public void PrimitiveType_ImplicitConversion_FromDouble_WorksCorrectly()
    {
        // Arrange
        const double testValue = 1.41421;

        // Act
        PrimitiveType primitiveType = testValue;

        // Assert
        Assert.Equal(PrimitiveTypeKind.Double, primitiveType.Kind);
        Assert.Equal(testValue, primitiveType.AsDouble);
    }

    [Fact]
    public void PrimitiveType_ImplicitConversion_FromByteArray_WorksCorrectly()
    {
        // Arrange
        var testValue = new byte[] { 10, 20, 30 };

        // Act
        PrimitiveType primitiveType = testValue;

        // Assert
        Assert.Equal(PrimitiveTypeKind.ByteArray, primitiveType.Kind);
        Assert.Equal(testValue, primitiveType.AsByteArray);
    }

    [Fact]
    public void PrimitiveType_ImplicitConversion_FromBoolean_WorksCorrectly()
    {
        // Arrange
        const bool testValue = true;

        // Act
        PrimitiveType primitiveType = testValue;

        // Assert
        Assert.Equal(PrimitiveTypeKind.Boolean, primitiveType.Kind);
        Assert.Equal(testValue, primitiveType.AsBoolean);
    }

    [Fact]
    public void PrimitiveType_Match_WithStringAction_ExecutesCorrectAction()
    {
        // Arrange
        const string testValue = "match test";
        var primitiveType = new PrimitiveType(testValue);
        string? capturedValue = null;

        // Act
        primitiveType.Match(
            onString: value => capturedValue = value,
            onBoolean: _ => Assert.Fail("Should not execute boolean action"),
            onInteger: _ => Assert.Fail("Should not execute integer action"),
            onDouble: _ => Assert.Fail("Should not execute double action"),
            onByteArray: _ => Assert.Fail("Should not execute byte array action")
        );

        // Assert
        Assert.Equal(testValue, capturedValue);
    }

    [Fact]
    public void PrimitiveType_Match_WithIntegerAction_ExecutesCorrectAction()
    {
        // Arrange
        const long testValue = 456L;
        var primitiveType = new PrimitiveType(testValue);
        long? capturedValue = null;

        // Act
        primitiveType.Match(
            onString: _ => Assert.Fail("Should not execute string action"),
            onBoolean: _ => Assert.Fail("Should not execute boolean action"),
            onInteger: value => capturedValue = value,
            onDouble: _ => Assert.Fail("Should not execute double action"),
            onByteArray: _ => Assert.Fail("Should not execute byte array action")
        );

        // Assert
        Assert.Equal(testValue, capturedValue);
    }

    [Fact]
    public void PrimitiveType_Match_WithDoubleAction_ExecutesCorrectAction()
    {
        // Arrange
        const double testValue = 6.28318;
        var primitiveType = new PrimitiveType(testValue);
        double? capturedValue = null;

        // Act
        primitiveType.Match(
            onString: _ => Assert.Fail("Should not execute string action"),
            onBoolean: _ => Assert.Fail("Should not execute boolean action"),
            onInteger: _ => Assert.Fail("Should not execute integer action"),
            onDouble: value => capturedValue = value,
            onByteArray: _ => Assert.Fail("Should not execute byte array action")
        );

        // Assert
        Assert.Equal(testValue, capturedValue);
    }

    [Fact]
    public void PrimitiveType_Match_WithByteArrayAction_ExecutesCorrectAction()
    {
        // Arrange
        var testValue = new byte[] { 100, 200, 50, 150 };
        var primitiveType = new PrimitiveType(testValue);
        byte[]? capturedValue = null;

        // Act
        primitiveType.Match(
            onString: _ => Assert.Fail("Should not execute string action"),
            onBoolean: _ => Assert.Fail("Should not execute boolean action"),
            onInteger: _ => Assert.Fail("Should not execute integer action"),
            onDouble: _ => Assert.Fail("Should not execute double action"),
            onByteArray: span => capturedValue = span.ToArray()
        );

        // Assert
        Assert.NotNull(capturedValue);
        Assert.Equal(testValue, capturedValue);
    }

    [Fact]
    public void PrimitiveType_Match_WithBooleanAction_ExecutesCorrectAction()
    {
        // Arrange
        const bool testValue = false;
        var primitiveType = new PrimitiveType(testValue);
        bool? capturedValue = null;

        // Act
        primitiveType.Match(
            onString: _ => Assert.Fail("Should not execute string action"),
            onBoolean: value => capturedValue = value,
            onInteger: _ => Assert.Fail("Should not execute integer action"),
            onDouble: _ => Assert.Fail("Should not execute double action"),
            onByteArray: _ => Assert.Fail("Should not execute byte array action")
        );

        // Assert
        Assert.Equal(testValue, capturedValue);
    }

    [Fact]
    public void PrimitiveType_Match_WithNullActions_DoesNotThrow()
    {
        // Arrange
        var primitiveType = new PrimitiveType("test");

        // Act & Assert - Should not throw when non-matching actions are null
        primitiveType.Match(
            onString: value => Assert.Equal("test", value),
            onBoolean: null,
            onInteger: null,
            onDouble: null,
            onByteArray: null
        );
    }

    [Fact]
    public void PrimitiveType_MatchWithReturn_WithString_ReturnsCorrectValue()
    {
        // Arrange
        const string testValue = "return test";
        var primitiveType = new PrimitiveType(testValue);

        // Act
        var result = primitiveType.Match(
            onString: value => $"String: {value}",
            onBoolean: value => $"Boolean: {value}",
            onInteger: value => $"Integer: {value}",
            onDouble: value => $"Double: {value}",
            onByteArray: span => $"ByteArray: {span.Length} bytes"
        );

        // Assert
        Assert.Equal($"String: {testValue}", result);
    }

    [Fact]
    public void PrimitiveType_MatchWithReturn_WithInteger_ReturnsCorrectValue()
    {
        // Arrange
        const long testValue = 789L;
        var primitiveType = new PrimitiveType(testValue);

        // Act
        var result = primitiveType.Match(
            onString: value => $"String: {value}",
            onBoolean: value => $"Boolean: {value}",
            onInteger: value => $"Integer: {value}",
            onDouble: value => $"Double: {value}",
            onByteArray: span => $"ByteArray: {span.Length} bytes"
        );

        // Assert
        Assert.Equal($"Integer: {testValue}", result);
    }

    [Fact]
    public void PrimitiveType_MatchWithReturn_WithDouble_ReturnsCorrectValue()
    {
        // Arrange
        const double testValue = 9.80665;
        var primitiveType = new PrimitiveType(testValue);

        // Act
        var result = primitiveType.Match(
            onString: value => $"String: {value}",
            onBoolean: value => $"Boolean: {value}",
            onInteger: value => $"Integer: {value}",
            onDouble: value => $"Double: {value}",
            onByteArray: span => $"ByteArray: {span.Length} bytes"
        );

        // Assert
        Assert.Equal($"Double: {testValue}", result);
    }

    [Fact]
    public void PrimitiveType_MatchWithReturn_WithByteArray_ReturnsCorrectValue()
    {
        // Arrange
        var testValue = new byte[] { 1, 2, 3, 4, 5, 6 };
        var primitiveType = new PrimitiveType(testValue);

        // Act
        var result = primitiveType.Match(
            onString: value => $"String: {value}",
            onBoolean: value => $"Boolean: {value}",
            onInteger: value => $"Integer: {value}",
            onDouble: value => $"Double: {value}",
            onByteArray: span => $"ByteArray: {span.Length} bytes"
        );

        // Assert
        Assert.Equal($"ByteArray: {testValue.Length} bytes", result);
    }

    [Fact]
    public void PrimitiveType_MatchWithReturn_WithBoolean_ReturnsCorrectValue()
    {
        // Arrange
        const bool testValue = true;
        var primitiveType = new PrimitiveType(testValue);

        // Act
        var result = primitiveType.Match(
            onString: value => $"String: {value}",
            onBoolean: value => $"Boolean: {value}",
            onInteger: value => $"Integer: {value}",
            onDouble: value => $"Double: {value}",
            onByteArray: span => $"ByteArray: {span.Length} bytes"
        );

        // Assert
        Assert.Equal($"Boolean: {testValue}", result);
    }

    [Fact]
    public void PrimitiveType_JsonSerialization_WithString_WorksCorrectly()
    {
        // Arrange
        const string testValue = "json test string";
        var primitiveType = new PrimitiveType(testValue);

        // Act
        var json = JsonConvert.SerializeObject(primitiveType);
        var deserialized = JsonConvert.DeserializeObject<PrimitiveType>(json);

        // Assert
        Assert.NotNull(deserialized);
        Assert.Equal(primitiveType.Kind, deserialized.Kind);
        Assert.Equal(primitiveType.AsString, deserialized.AsString);
        Assert.Equal(primitiveType, deserialized);
    }

    [Fact]
    public void PrimitiveType_JsonSerialization_WithInteger_WorksCorrectly()
    {
        // Arrange
        const long testValue = 12345L;
        var primitiveType = new PrimitiveType(testValue);

        // Act
        var json = JsonConvert.SerializeObject(primitiveType);
        var deserialized = JsonConvert.DeserializeObject<PrimitiveType>(json);

        // Assert
        Assert.NotNull(deserialized);
        Assert.Equal(primitiveType.Kind, deserialized.Kind);
        Assert.Equal(primitiveType.AsInteger, deserialized.AsInteger);
        Assert.Equal(primitiveType, deserialized);
    }

    [Fact]
    public void PrimitiveType_JsonSerialization_WithDouble_WorksCorrectly()
    {
        // Arrange
        const double testValue = 123.456;
        var primitiveType = new PrimitiveType(testValue);

        // Act
        var json = JsonConvert.SerializeObject(primitiveType);
        var deserialized = JsonConvert.DeserializeObject<PrimitiveType>(json);

        // Assert
        Assert.NotNull(deserialized);
        Assert.Equal(primitiveType.Kind, deserialized.Kind);
        Assert.Equal(primitiveType.AsDouble, deserialized.AsDouble, precision: 10);
        Assert.Equal(primitiveType, deserialized);
    }

    [Fact]
    public void PrimitiveType_JsonSerialization_WithByteArray_WorksCorrectly()
    {
        // Arrange
        var testValue = new byte[] { 0xFF, 0x00, 0x80, 0x7F };
        var primitiveType = new PrimitiveType(testValue);

        // Act
        var json = JsonConvert.SerializeObject(primitiveType);
        var deserialized = JsonConvert.DeserializeObject<PrimitiveType>(json);

        // Assert
        Assert.NotNull(deserialized);
        Assert.Equal(primitiveType.Kind, deserialized.Kind);
        Assert.Equal(primitiveType.AsByteArray, deserialized.AsByteArray);
        Assert.Equal(primitiveType, deserialized);
    }

    [Fact]
    public void PrimitiveType_JsonSerialization_WithBoolean_WorksCorrectly()
    {
        // Arrange
        const bool testValue = true;
        var primitiveType = new PrimitiveType(testValue);

        // Act
        var json = JsonConvert.SerializeObject(primitiveType);
        var deserialized = JsonConvert.DeserializeObject<PrimitiveType>(json);

        // Assert
        Assert.NotNull(deserialized);
        Assert.Equal(primitiveType.Kind, deserialized.Kind);
        Assert.Equal(primitiveType.AsBoolean, deserialized.AsBoolean);
        Assert.Equal(primitiveType, deserialized);
    }

    [Fact]
    public void PrimitiveType_JsonSerialization_WithNull_WorksCorrectly()
    {
        // Arrange
        PrimitiveType? nullPrimitiveType = null;

        // Act
        var json = JsonConvert.SerializeObject(nullPrimitiveType);
        var deserialized = JsonConvert.DeserializeObject<PrimitiveType?>(json);

        // Assert
        Assert.Equal("null", json);
        Assert.Null(deserialized);
    }

    [Fact]
    public void PrimitiveType_JsonRoundTrip_PreservesAllTypes()
    {
        // Arrange
        var primitiveTypes = new PrimitiveType[]
        {
            new("test string"),
            new(true),
            new(42L),
            new(3.14159),
            new(new byte[] { 1, 2, 3, 4, 5 })
        };

        foreach (var original in primitiveTypes)
        {
            // Act
            var json = JsonConvert.SerializeObject(original);
            var deserialized = JsonConvert.DeserializeObject<PrimitiveType>(json);

            // Assert
            Assert.NotNull(deserialized);
            Assert.Equal(original, deserialized);
        }
    }

    [Fact]
    public void PrimitiveType_EdgeCase_EmptyString_HandledCorrectly()
    {
        // Arrange
        var primitiveType = new PrimitiveType("");

        // Act & Assert
        Assert.Equal(PrimitiveTypeKind.String, primitiveType.Kind);
        Assert.Equal("", primitiveType.AsString);
        Assert.Equal("", primitiveType.ToString());

        // Test serialization
        var json = JsonConvert.SerializeObject(primitiveType);
        var deserialized = JsonConvert.DeserializeObject<PrimitiveType>(json);
        Assert.Equal(primitiveType, deserialized);
    }

    [Fact]
    public void PrimitiveType_EdgeCase_EmptyByteArray_HandledCorrectly()
    {
        // Arrange
        var emptyArray = Array.Empty<byte>();
        var primitiveType = new PrimitiveType(emptyArray);

        // Act & Assert
        Assert.Equal(PrimitiveTypeKind.ByteArray, primitiveType.Kind);
        Assert.Empty(primitiveType.AsByteArray);
        Assert.Equal(Convert.ToBase64String(emptyArray), primitiveType.ToString());

        // Test serialization
        var json = JsonConvert.SerializeObject(primitiveType);
        var deserialized = JsonConvert.DeserializeObject<PrimitiveType>(json);
        Assert.Equal(primitiveType, deserialized);
    }

    [Theory]
    [InlineData(long.MaxValue)]
    [InlineData(long.MinValue)]
    [InlineData(0L)]
    public void PrimitiveType_EdgeCase_ExtremeIntegerValues_HandledCorrectly(long extremeValue)
    {
        // Arrange
        var primitiveType = new PrimitiveType(extremeValue);

        // Act & Assert
        Assert.Equal(PrimitiveTypeKind.Integer, primitiveType.Kind);
        Assert.Equal(extremeValue, primitiveType.AsInteger);
        Assert.Equal(extremeValue.ToString(), primitiveType.ToString());

        // Test serialization
        var json = JsonConvert.SerializeObject(primitiveType);
        var deserialized = JsonConvert.DeserializeObject<PrimitiveType>(json);
        Assert.Equal(primitiveType, deserialized);
    }

    [Theory]
    [InlineData(double.MaxValue)]
    [InlineData(double.MinValue)]
    [InlineData(double.PositiveInfinity)]
    [InlineData(double.NegativeInfinity)]
    [InlineData(0.0)]
    public void PrimitiveType_EdgeCase_ExtremeDoubleValues_HandledCorrectly(double extremeValue)
    {
        // Arrange
        var primitiveType = new PrimitiveType(extremeValue);

        // Act & Assert
        Assert.Equal(PrimitiveTypeKind.Double, primitiveType.Kind);
        Assert.Equal(extremeValue, primitiveType.AsDouble);

        if (!double.IsNaN(extremeValue))
        {
            // Test serialization (skip for NaN as it has special handling)
            var json = JsonConvert.SerializeObject(primitiveType);
            var deserialized = JsonConvert.DeserializeObject<PrimitiveType>(json);
            Assert.Equal(primitiveType.AsDouble, deserialized!.AsDouble, precision: 10);
        }
    }

    [Fact]
    public void PrimitiveType_EdgeCase_NaNDouble_HandledCorrectly()
    {
        // Arrange
        var primitiveType = new PrimitiveType(double.NaN);

        // Act & Assert
        Assert.Equal(PrimitiveTypeKind.Double, primitiveType.Kind);
        Assert.True(double.IsNaN(primitiveType.AsDouble));

        // NaN has special equality behavior
        var other = new PrimitiveType(double.NaN);
        Assert.False(primitiveType.Equals(other)); // NaN != NaN
    }

    [Fact]
    public void PrimitiveType_Performance_ManyCreationsAndAccesses()
    {
        // Arrange
        const int iterationCount = 10000;
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Act
        for (int i = 0; i < iterationCount; i++)
        {
            var stringType = new PrimitiveType($"string_{i}");
            var intType = new PrimitiveType(i);
            var doubleType = new PrimitiveType((double)i);
            var byteType = new PrimitiveType(new[] { (byte)i });

            // Access values
            var _ = stringType.AsString;
            var __ = intType.AsInteger;
            var ___ = doubleType.AsDouble;
            var ____ = byteType.AsByteArray;
        }

        stopwatch.Stop();

        // Assert
        Assert.True(stopwatch.ElapsedMilliseconds < 1000,
            $"Performance test failed: took {stopwatch.ElapsedMilliseconds}ms");
    }

    [Fact]
    public void PrimitiveType_StressTest_ManyEqualityComparisons()
    {
        // Arrange
        const int comparisonCount = 1000;
        var types = new PrimitiveType[]
        {
            new("test"),
            new(42L),
            new(3.14),
            new(new byte[] { 1, 2, 3 })
        };

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Act
        for (int i = 0; i < comparisonCount; i++)
        {
            foreach (var type1 in types)
            {
                foreach (var type2 in types)
                {
                    var _ = type1.Equals(type2);
                    var __ = type1.GetHashCode();
                }
            }
        }

        stopwatch.Stop();

        // Assert
        Assert.True(stopwatch.ElapsedMilliseconds < 1000,
            $"Stress test failed: took {stopwatch.ElapsedMilliseconds}ms");
    }

    [Fact]
    public void PrimitiveType_StressTest_JsonSerializationPerformance()
    {
        // Arrange
        const int serializationCount = 1000;
        var testTypes = new PrimitiveType[]
        {
            new("serialization test"),
            new(999L),
            new(1.234567),
            new(new byte[] { 0xAB, 0xCD, 0xEF })
        };

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Act
        for (int i = 0; i < serializationCount; i++)
        {
            foreach (var type in testTypes)
            {
                var json = JsonConvert.SerializeObject(type);
                var deserialized = JsonConvert.DeserializeObject<PrimitiveType>(json);
                Assert.Equal(type, deserialized);
            }
        }

        stopwatch.Stop();

        // Assert
        Assert.True(stopwatch.ElapsedMilliseconds < 5000,
            $"JSON serialization stress test failed: took {stopwatch.ElapsedMilliseconds}ms");
    }

    [Fact]
    public void PrimitiveType_IntegrationTest_ComplexScenario()
    {
        // Arrange - Complex scenario with all types and operations
        var stringType = new PrimitiveType("integration test");
        var boolType = new PrimitiveType(false);
        var intType = new PrimitiveType(42L);
        var doubleType = new PrimitiveType(3.14159);
        var byteType = new PrimitiveType("Hello"u8.ToArray()); // "Hello"

        var allTypes = new[] { stringType, boolType, intType, doubleType, byteType };

        // Act & Assert - Test all operations work together
        foreach (var type in allTypes)
        {
            // Test ToString
            var stringRepresentation = type.ToString();
            Assert.NotNull(stringRepresentation);

            // Test Try methods
            var stringSuccess = type.TryGetString(out _);
            var boolSuccess = type.TryGetBoolean(out _);
            var intSuccess = type.TryGetInteger(out _);
            var doubleSuccess = type.TryGetDouble(out _);
            var byteSuccess = type.TryGetByteArray(out _);

            // Exactly one should succeed based on the type
            var successCount = new[] { stringSuccess, boolSuccess, intSuccess, doubleSuccess, byteSuccess }.Count(b => b);
            Assert.Equal(1, successCount);

            // Test Match pattern
            var matchResult = type.Match(
                onString: s => $"String: {s}",
                onBoolean: b => $"Boolean: {b}",
                onInteger: i => $"Integer: {i}",
                onDouble: d => $"Double: {d}",
                onByteArray: b => $"Bytes: {b.Length}"
            );
            Assert.NotNull(matchResult);

            // Test JSON serialization
            var json = JsonConvert.SerializeObject(type);
            var deserialized = JsonConvert.DeserializeObject<PrimitiveType>(json);
            Assert.Equal(type, deserialized);

            // Test equality and hash codes
            var copy = new PrimitiveType(type);
            Assert.Equal(type, copy);
            Assert.Equal(type.GetHashCode(), copy.GetHashCode());
        }

        // Test cross-type operations
        for (int i = 0; i < allTypes.Length; i++)
        {
            for (int j = i + 1; j < allTypes.Length; j++)
            {
                Assert.NotEqual(allTypes[i], allTypes[j]);
                Assert.NotEqual(allTypes[i].GetHashCode(), allTypes[j].GetHashCode());
            }
        }
    }

    [Fact]
    public void PrimitiveType_ThreadSafety_ConcurrentOperations()
    {
        // Arrange
        var sharedType = new PrimitiveType("thread safety test");
        const int taskCount = 10;
        const int operationsPerTask = 100;

        // Act
        var tasks = Enumerable.Range(0, taskCount).Select(taskId => Task.Run(() =>
        {
            for (int i = 0; i < operationsPerTask; i++)
            {
                // Test concurrent read operations (PrimitiveType should be immutable)
                var _ = sharedType.Kind;
                var __ = sharedType.AsString;
                var ___ = sharedType.ToString();
                var ____ = sharedType.GetHashCode();

                // Test TryGet methods
                sharedType.TryGetString(out var _);
                sharedType.TryGetBoolean(out var _);
                sharedType.TryGetInteger(out var _);
                sharedType.TryGetDouble(out var _);
                sharedType.TryGetByteArray(out var _);

                // Test Match
                sharedType.Match(
                    onString: s => s.Length,
                    onBoolean: b => b ? 1 : 0,
                    onInteger: ib => (int)ib,
                    onDouble: d => (int)d,
                    onByteArray: b => b.Length
                );

                // Test JSON serialization
                var json = JsonConvert.SerializeObject(sharedType);
                JsonConvert.DeserializeObject<PrimitiveType>(json);
            }
        })).ToArray();

        // Assert
#pragma warning disable xUnit1031
        Task.WaitAll(tasks);
#pragma warning restore xUnit1031
        // If we get here without exceptions, thread safety test passed
        Assert.True(true);
    }

    [Fact]
    public void PrimitiveType_AsByteSpan_ReturnsCorrectSpan()
    {
        // Arrange
        var testBytes = new byte[] { 1, 2, 3, 4, 5 };
        var primitiveType = new PrimitiveType(testBytes);

        // Act
        var span = primitiveType.AsByteSpan;

        // Assert
        Assert.Equal(testBytes.Length, span.Length);
        Assert.True(testBytes.AsSpan().SequenceEqual(span));
    }

    [Fact]
    public void PrimitiveType_AsByteSpan_IsReadOnly()
    {
        // Arrange
        var testBytes = new byte[] { 1, 2, 3 };
        var primitiveType = new PrimitiveType(testBytes);

        // Act
        var span = primitiveType.AsByteSpan;
        var originalValue = span[0];

        // Note: We can't modify the span directly since it's ReadOnlySpan<byte>
        // This test verifies the type safety - ReadOnlySpan prevents modification

        // Assert
        Assert.Equal(originalValue, span[0]);
        Assert.Equal(testBytes[0], span[0]);
    }

    [Fact]
    public void PrimitiveType_CopyConstructor_WithNullArgument_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new PrimitiveType((PrimitiveType)null!));
    }

    [Fact]
    public void PrimitiveType_ByteArrayConstructor_WithNullArray_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new PrimitiveType((byte[])null!));
    }

    [Fact]
    public void PrimitiveType_StringConstructor_WithNullString_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new PrimitiveType((string)null!));
    }

    [Fact]
    public void PrimitiveType_AllKinds_CoveredByTests()
    {
        // Arrange & Act - Create instances of all PrimitiveTypeKind values
        var stringType = new PrimitiveType("test");
        var booleanType = new PrimitiveType(true);
        var integerType = new PrimitiveType(42L);
        var doubleType = new PrimitiveType(3.14);
        var byteArrayType = new PrimitiveType(new byte[] { 1, 2, 3 });

        // Assert - All enum values are covered
        Assert.Equal(PrimitiveTypeKind.String, stringType.Kind);
        Assert.Equal(PrimitiveTypeKind.Boolean, booleanType.Kind);
        Assert.Equal(PrimitiveTypeKind.Integer, integerType.Kind);
        Assert.Equal(PrimitiveTypeKind.Double, doubleType.Kind);
        Assert.Equal(PrimitiveTypeKind.ByteArray, byteArrayType.Kind);

        // Verify all enum values are tested (this ensures we don't miss new kinds)
        var testedKinds = new HashSet<PrimitiveTypeKind>
        {
            stringType.Kind,
            booleanType.Kind,
            integerType.Kind,
            doubleType.Kind,
            byteArrayType.Kind
        };

        var allKinds = Enum.GetValues<PrimitiveTypeKind>().ToHashSet();
        Assert.Equal(allKinds, testedKinds);
    }
}
