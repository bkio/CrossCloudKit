// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Newtonsoft.Json;
using Xunit;

namespace CrossCloudKit.Utilities.Common.Tests;

public class PrimitiveTests
{
    [Fact]
    public void Primitive_StringConstructor_InitializesCorrectly()
    {
        // Arrange
        const string testValue = "test string";

        // Act
        var primitiveType = new Primitive(testValue);

        // Assert
        Assert.Equal(PrimitiveKind.String, primitiveType.Kind);
        Assert.Equal(testValue, primitiveType.AsString);
    }

    [Fact]
    public void Primitive_IntegerConstructor_InitializesCorrectly()
    {
        // Arrange
        const long testValue = 42L;

        // Act
        var primitiveType = new Primitive(testValue);

        // Assert
        Assert.Equal(PrimitiveKind.Integer, primitiveType.Kind);
        Assert.Equal(testValue, primitiveType.AsInteger);
    }

    [Fact]
    public void Primitive_DoubleConstructor_InitializesCorrectly()
    {
        // Arrange
        const double testValue = 3.14159;

        // Act
        var primitiveType = new Primitive(testValue);

        // Assert
        Assert.Equal(PrimitiveKind.Double, primitiveType.Kind);
        Assert.Equal(testValue, primitiveType.AsDouble);
    }

    [Fact]
    public void Primitive_BooleanConstructor_InitializesCorrectly()
    {
        // Arrange
        const bool testValue = true;

        // Act
        var primitiveType = new Primitive(testValue);

        // Assert
        Assert.Equal(PrimitiveKind.Boolean, primitiveType.Kind);
        Assert.Equal(testValue, primitiveType.AsBoolean);
    }

    [Fact]
    public void Primitive_ByteArrayConstructor_InitializesCorrectly()
    {
        // Arrange
        var testValue = new byte[] { 1, 2, 3, 4, 5 };

        // Act
        var primitiveType = new Primitive(testValue);

        // Assert
        Assert.Equal(PrimitiveKind.ByteArray, primitiveType.Kind);
        Assert.Equal(testValue, primitiveType.AsByteArray);
    }

    [Fact]
    public void Primitive_ByteArrayConstructor_CreatesIndependentCopy()
    {
        // Arrange
        var originalArray = new byte[] { 1, 2, 3 };

        // Act
        var primitiveType = new Primitive(originalArray);
        originalArray[0] = 99; // Modify original

        // Assert
        var retrievedArray = primitiveType.AsByteArray;
        Assert.Equal(1, retrievedArray[0]); // Should not be affected by original modification
    }

    [Fact]
    public void Primitive_SpanConstructor_InitializesCorrectly()
    {
        // Arrange
        var testValue = new byte[] { 10, 20, 30, 40 };
        var span = testValue.AsSpan();

        // Act
        var primitiveType = new Primitive(span);

        // Assert
        Assert.Equal(PrimitiveKind.ByteArray, primitiveType.Kind);
        Assert.Equal(testValue, primitiveType.AsByteArray);
    }

    [Fact]
    public void Primitive_CopyConstructor_CreatesExactCopy()
    {
        // Arrange
        var original = new Primitive("test value");

        // Act
        var copy = new Primitive(original);

        // Assert
        Assert.Equal(original.Kind, copy.Kind);
        Assert.Equal(original.AsString, copy.AsString);
        Assert.Equal(original, copy);
    }

    [Fact]
    public void Primitive_CopyConstructor_WithByteArray_CreatesIndependentCopy()
    {
        // Arrange
        var originalBytes = new byte[] { 1, 2, 3, 4 };
        var original = new Primitive(originalBytes);

        // Act
        var copy = new Primitive(original);

        // Modify original's internal array indirectly (though this shouldn't be possible due to immutability)
        var retrievedOriginal = original.AsByteArray;
        var retrievedCopy = copy.AsByteArray;

        // Assert
        Assert.Equal(original.Kind, copy.Kind);
        Assert.Equal(retrievedOriginal, retrievedCopy);
        Assert.NotSame(retrievedOriginal, retrievedCopy); // Different array instances
    }

    [Fact]
    public void Primitive_AsString_WithWrongType_ThrowsInvalidOperationException()
    {
        // Arrange
        var primitiveType = new Primitive(42L);

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => primitiveType.AsString);
        Assert.Contains("Cannot access string value when Kind is Integer", exception.Message);
    }

    [Fact]
    public void Primitive_AsInteger_WithWrongType_ThrowsInvalidOperationException()
    {
        // Arrange
        var primitiveType = new Primitive("test");

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => primitiveType.AsInteger);
        Assert.Contains("Cannot access integer value when Kind is String", exception.Message);
    }

    [Fact]
    public void Primitive_AsDouble_WithWrongType_ThrowsInvalidOperationException()
    {
        // Arrange
        var primitiveType = new Primitive(42L);

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => primitiveType.AsDouble);
        Assert.Contains("Cannot access double value when Kind is Integer", exception.Message);
    }

    [Fact]
    public void Primitive_AsByteArray_WithWrongType_ThrowsInvalidOperationException()
    {
        // Arrange
        var primitiveType = new Primitive("test");

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => primitiveType.AsByteArray);
        Assert.Contains("Cannot access byte array value when Kind is String", exception.Message);
    }

    [Fact]
    public void Primitive_AsBoolean_WithWrongType_ThrowsInvalidOperationException()
    {
        // Arrange
        var primitiveType = new Primitive("test");

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => primitiveType.AsBoolean);
        Assert.Contains("Cannot access boolean value when Kind is String", exception.Message);
    }

    [Fact]
    public void Primitive_TryGetString_WithStringType_ReturnsTrue()
    {
        // Arrange
        const string testValue = "test string";
        var primitiveType = new Primitive(testValue);

        // Act
        var success = primitiveType.TryGetString(out var value);

        // Assert
        Assert.True(success);
        Assert.Equal(testValue, value);
    }

    [Fact]
    public void Primitive_TryGetString_WithNonStringType_ReturnsFalse()
    {
        // Arrange
        var primitiveType = new Primitive(42L);

        // Act
        var success = primitiveType.TryGetString(out var value);

        // Assert
        Assert.False(success);
        Assert.Null(value);
    }

    [Fact]
    public void Primitive_TryGetInteger_WithIntegerType_ReturnsTrue()
    {
        // Arrange
        const long testValue = 123L;
        var primitiveType = new Primitive(testValue);

        // Act
        var success = primitiveType.TryGetInteger(out var value);

        // Assert
        Assert.True(success);
        Assert.Equal(testValue, value);
    }

    [Fact]
    public void Primitive_TryGetInteger_WithNonIntegerType_ReturnsFalse()
    {
        // Arrange
        var primitiveType = new Primitive("test");

        // Act
        var success = primitiveType.TryGetInteger(out var value);

        // Assert
        Assert.False(success);
        Assert.Equal(0L, value);
    }

    [Fact]
    public void Primitive_TryGetDouble_WithDoubleType_ReturnsTrue()
    {
        // Arrange
        const double testValue = 2.71828;
        var primitiveType = new Primitive(testValue);

        // Act
        var success = primitiveType.TryGetDouble(out var value);

        // Assert
        Assert.True(success);
        Assert.Equal(testValue, value);
    }

    [Fact]
    public void Primitive_TryGetDouble_WithNonDoubleType_ReturnsFalse()
    {
        // Arrange
        var primitiveType = new Primitive(42L);

        // Act
        var success = primitiveType.TryGetDouble(out var value);

        // Assert
        Assert.False(success);
        Assert.Equal(0.0, value);
    }

    [Fact]
    public void Primitive_TryGetByteArray_WithByteArrayType_ReturnsTrue()
    {
        // Arrange
        var testValue = new byte[] { 5, 10, 15, 20 };
        var primitiveType = new Primitive(testValue);

        // Act
        var success = primitiveType.TryGetByteArray(out var value);

        // Assert
        Assert.True(success);
        Assert.NotNull(value);
        Assert.Equal(testValue, value);
    }

    [Fact]
    public void Primitive_TryGetByteArray_WithNonByteArrayType_ReturnsFalse()
    {
        // Arrange
        var primitiveType = new Primitive("test");

        // Act
        var success = primitiveType.TryGetByteArray(out var value);

        // Assert
        Assert.False(success);
        Assert.Null(value);
    }

    [Fact]
    public void Primitive_TryGetBoolean_WithBooleanType_ReturnsTrue()
    {
        // Arrange
        const bool testValue = false;
        var primitiveType = new Primitive(testValue);

        // Act
        var success = primitiveType.TryGetBoolean(out var value);

        // Assert
        Assert.True(success);
        Assert.Equal(testValue, value);
    }

    [Fact]
    public void Primitive_TryGetBoolean_WithNonBooleanType_ReturnsFalse()
    {
        // Arrange
        var primitiveType = new Primitive("test");

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
    public void Primitive_ToString_WithString_ReturnsString(string testValue)
    {
        // Arrange
        var primitiveType = new Primitive(testValue);

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
    public void Primitive_ToString_WithInteger_ReturnsStringRepresentation(long testValue)
    {
        // Arrange
        var primitiveType = new Primitive(testValue);

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
    public void Primitive_ToString_WithDouble_ReturnsInvariantCultureString(double testValue)
    {
        // Arrange
        var primitiveType = new Primitive(testValue);
        var expected = testValue.ToString(System.Globalization.CultureInfo.InvariantCulture);

        // Act
        var result = primitiveType.ToString();

        // Assert
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Primitive_ToString_WithByteArray_ReturnsBase64String()
    {
        // Arrange
        var testValue = "Hello"u8.ToArray(); // "Hello" in ASCII
        var primitiveType = new Primitive(testValue);
        var expectedBase64 = Convert.ToBase64String(testValue);

        // Act
        var result = primitiveType.ToString();

        // Assert
        Assert.Equal(expectedBase64, result);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Primitive_ToString_WithBoolean_ReturnsStringRepresentation(bool testValue)
    {
        // Arrange
        var primitiveType = new Primitive(testValue);
        var expected = testValue.ToString();

        // Act
        var result = primitiveType.ToString();

        // Assert
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Primitive_Equals_WithSameStringValues_ReturnsTrue()
    {
        // Arrange
        const string testValue = "test value";
        var primitiveType1 = new Primitive(testValue);
        var primitiveType2 = new Primitive(testValue);

        // Act & Assert
        Assert.True(primitiveType1.Equals(primitiveType2));
        Assert.True(primitiveType1 == primitiveType2);
        Assert.False(primitiveType1 != primitiveType2);
    }

    [Fact]
    public void Primitive_Equals_WithSameIntegerValues_ReturnsTrue()
    {
        // Arrange
        const long testValue = 42L;
        var primitiveType1 = new Primitive(testValue);
        var primitiveType2 = new Primitive(testValue);

        // Act & Assert
        Assert.True(primitiveType1.Equals(primitiveType2));
        Assert.True(primitiveType1 == primitiveType2);
        Assert.False(primitiveType1 != primitiveType2);
    }

    [Fact]
    public void Primitive_Equals_WithSimilarDoubleValues_ReturnsTrue()
    {
        // Arrange - Using values that are very close (within tolerance)
        const double testValue1 = 3.14159;
        const double testValue2 = 3.14159000001; // Very close but not exactly equal
        var primitiveType1 = new Primitive(testValue1);
        var primitiveType2 = new Primitive(testValue2);

        // Act & Assert
        Assert.True(primitiveType1.Equals(primitiveType2));
    }

    [Fact]
    public void Primitive_Equals_WithSameByteArrayValues_ReturnsTrue()
    {
        // Arrange
        var testValue1 = new byte[] { 1, 2, 3, 4 };
        var testValue2 = new byte[] { 1, 2, 3, 4 };
        var primitiveType1 = new Primitive(testValue1);
        var primitiveType2 = new Primitive(testValue2);

        // Act & Assert
        Assert.True(primitiveType1.Equals(primitiveType2));
    }

    [Fact]
    public void Primitive_Equals_WithSameBooleanValues_ReturnsTrue()
    {
        // Arrange
        const bool testValue = true;
        var primitiveType1 = new Primitive(testValue);
        var primitiveType2 = new Primitive(testValue);

        // Act & Assert
        Assert.True(primitiveType1.Equals(primitiveType2));
        Assert.True(primitiveType1 == primitiveType2);
        Assert.False(primitiveType1 != primitiveType2);
    }

    [Fact]
    public void Primitive_Equals_WithDifferentKinds_ReturnsFalse()
    {
        // Arrange
        var stringType = new Primitive("42");
        var integerType = new Primitive(42L);

        // Act & Assert
        Assert.False(stringType.Equals(integerType));
        Assert.False(stringType == integerType);
        Assert.True(stringType != integerType);
    }

    [Fact]
    public void Primitive_Equals_WithNull_ReturnsFalse()
    {
        // Arrange
        var primitiveType = new Primitive("test");

        // Act & Assert
        Assert.False(primitiveType.Equals(null));
        Assert.False(primitiveType == null);
        Assert.True(primitiveType != null);
    }

    [Fact]
    public void Primitive_GetHashCode_WithEqualValues_ReturnsSameHashCode()
    {
        // Arrange
        const string testValue = "test value";
        var primitiveType1 = new Primitive(testValue);
        var primitiveType2 = new Primitive(testValue);

        // Act
        var hash1 = primitiveType1.GetHashCode();
        var hash2 = primitiveType2.GetHashCode();

        // Assert
        Assert.Equal(hash1, hash2);
    }

    [Fact]
    public void Primitive_GetHashCode_WithDifferentValues_ReturnsDifferentHashCodes()
    {
        // Arrange
        var primitiveType1 = new Primitive("value1");
        var primitiveType2 = new Primitive("value2");

        // Act
        var hash1 = primitiveType1.GetHashCode();
        var hash2 = primitiveType2.GetHashCode();

        // Assert
        Assert.NotEqual(hash1, hash2);
    }

    [Fact]
    public void Primitive_ImplicitConversion_FromString_WorksCorrectly()
    {
        // Arrange
        const string testValue = "implicit test";

        // Act
        Primitive primitiveType = testValue;

        // Assert
        Assert.Equal(PrimitiveKind.String, primitiveType.Kind);
        Assert.Equal(testValue, primitiveType.AsString);
    }

    [Fact]
    public void Primitive_ImplicitConversion_FromLong_WorksCorrectly()
    {
        // Arrange
        const long testValue = 999L;

        // Act
        Primitive primitiveType = testValue;

        // Assert
        Assert.Equal(PrimitiveKind.Integer, primitiveType.Kind);
        Assert.Equal(testValue, primitiveType.AsInteger);
    }

    [Fact]
    public void Primitive_ImplicitConversion_FromDouble_WorksCorrectly()
    {
        // Arrange
        const double testValue = 1.41421;

        // Act
        Primitive primitiveType = testValue;

        // Assert
        Assert.Equal(PrimitiveKind.Double, primitiveType.Kind);
        Assert.Equal(testValue, primitiveType.AsDouble);
    }

    [Fact]
    public void Primitive_ImplicitConversion_FromByteArray_WorksCorrectly()
    {
        // Arrange
        var testValue = new byte[] { 10, 20, 30 };

        // Act
        Primitive primitiveType = testValue;

        // Assert
        Assert.Equal(PrimitiveKind.ByteArray, primitiveType.Kind);
        Assert.Equal(testValue, primitiveType.AsByteArray);
    }

    [Fact]
    public void Primitive_ImplicitConversion_FromBoolean_WorksCorrectly()
    {
        // Arrange
        const bool testValue = true;

        // Act
        Primitive primitiveType = testValue;

        // Assert
        Assert.Equal(PrimitiveKind.Boolean, primitiveType.Kind);
        Assert.Equal(testValue, primitiveType.AsBoolean);
    }

    [Fact]
    public void Primitive_Match_WithStringAction_ExecutesCorrectAction()
    {
        // Arrange
        const string testValue = "match test";
        var primitiveType = new Primitive(testValue);
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
    public void Primitive_Match_WithIntegerAction_ExecutesCorrectAction()
    {
        // Arrange
        const long testValue = 456L;
        var primitiveType = new Primitive(testValue);
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
    public void Primitive_Match_WithDoubleAction_ExecutesCorrectAction()
    {
        // Arrange
        const double testValue = 6.28318;
        var primitiveType = new Primitive(testValue);
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
    public void Primitive_Match_WithByteArrayAction_ExecutesCorrectAction()
    {
        // Arrange
        var testValue = new byte[] { 100, 200, 50, 150 };
        var primitiveType = new Primitive(testValue);
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
    public void Primitive_Match_WithBooleanAction_ExecutesCorrectAction()
    {
        // Arrange
        const bool testValue = false;
        var primitiveType = new Primitive(testValue);
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
    public void Primitive_Match_WithNullActions_DoesNotThrow()
    {
        // Arrange
        var primitiveType = new Primitive("test");

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
    public void Primitive_MatchWithReturn_WithString_ReturnsCorrectValue()
    {
        // Arrange
        const string testValue = "return test";
        var primitiveType = new Primitive(testValue);

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
    public void Primitive_MatchWithReturn_WithInteger_ReturnsCorrectValue()
    {
        // Arrange
        const long testValue = 789L;
        var primitiveType = new Primitive(testValue);

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
    public void Primitive_MatchWithReturn_WithDouble_ReturnsCorrectValue()
    {
        // Arrange
        const double testValue = 9.80665;
        var primitiveType = new Primitive(testValue);

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
    public void Primitive_MatchWithReturn_WithByteArray_ReturnsCorrectValue()
    {
        // Arrange
        var testValue = new byte[] { 1, 2, 3, 4, 5, 6 };
        var primitiveType = new Primitive(testValue);

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
    public void Primitive_MatchWithReturn_WithBoolean_ReturnsCorrectValue()
    {
        // Arrange
        const bool testValue = true;
        var primitiveType = new Primitive(testValue);

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
    public void Primitive_JsonSerialization_WithString_WorksCorrectly()
    {
        // Arrange
        const string testValue = "json test string";
        var primitiveType = new Primitive(testValue);

        // Act
        var json = JsonConvert.SerializeObject(primitiveType);
        var deserialized = JsonConvert.DeserializeObject<Primitive>(json);

        // Assert
        Assert.NotNull(deserialized);
        Assert.Equal(primitiveType.Kind, deserialized.Kind);
        Assert.Equal(primitiveType.AsString, deserialized.AsString);
        Assert.Equal(primitiveType, deserialized);
    }

    [Fact]
    public void Primitive_JsonSerialization_WithInteger_WorksCorrectly()
    {
        // Arrange
        const long testValue = 12345L;
        var primitiveType = new Primitive(testValue);

        // Act
        var json = JsonConvert.SerializeObject(primitiveType);
        var deserialized = JsonConvert.DeserializeObject<Primitive>(json);

        // Assert
        Assert.NotNull(deserialized);
        Assert.Equal(primitiveType.Kind, deserialized.Kind);
        Assert.Equal(primitiveType.AsInteger, deserialized.AsInteger);
        Assert.Equal(primitiveType, deserialized);
    }

    [Fact]
    public void Primitive_JsonSerialization_WithDouble_WorksCorrectly()
    {
        // Arrange
        const double testValue = 123.456;
        var primitiveType = new Primitive(testValue);

        // Act
        var json = JsonConvert.SerializeObject(primitiveType);
        var deserialized = JsonConvert.DeserializeObject<Primitive>(json);

        // Assert
        Assert.NotNull(deserialized);
        Assert.Equal(primitiveType.Kind, deserialized.Kind);
        Assert.Equal(primitiveType.AsDouble, deserialized.AsDouble, precision: 10);
        Assert.Equal(primitiveType, deserialized);
    }

    [Fact]
    public void Primitive_JsonSerialization_WithByteArray_WorksCorrectly()
    {
        // Arrange
        var testValue = new byte[] { 0xFF, 0x00, 0x80, 0x7F };
        var primitiveType = new Primitive(testValue);

        // Act
        var json = JsonConvert.SerializeObject(primitiveType);
        var deserialized = JsonConvert.DeserializeObject<Primitive>(json);

        // Assert
        Assert.NotNull(deserialized);
        Assert.Equal(primitiveType.Kind, deserialized.Kind);
        Assert.Equal(primitiveType.AsByteArray, deserialized.AsByteArray);
        Assert.Equal(primitiveType, deserialized);
    }

    [Fact]
    public void Primitive_JsonSerialization_WithBoolean_WorksCorrectly()
    {
        // Arrange
        const bool testValue = true;
        var primitiveType = new Primitive(testValue);

        // Act
        var json = JsonConvert.SerializeObject(primitiveType);
        var deserialized = JsonConvert.DeserializeObject<Primitive>(json);

        // Assert
        Assert.NotNull(deserialized);
        Assert.Equal(primitiveType.Kind, deserialized.Kind);
        Assert.Equal(primitiveType.AsBoolean, deserialized.AsBoolean);
        Assert.Equal(primitiveType, deserialized);
    }

    [Fact]
    public void Primitive_JsonSerialization_WithNull_WorksCorrectly()
    {
        // Arrange
        Primitive? nullPrimitive = null;

        // Act
        var json = JsonConvert.SerializeObject(nullPrimitive);
        var deserialized = JsonConvert.DeserializeObject<Primitive?>(json);

        // Assert
        Assert.Equal("null", json);
        Assert.Null(deserialized);
    }

    [Fact]
    public void Primitive_JsonRoundTrip_PreservesAllTypes()
    {
        // Arrange
        var primitiveTypes = new Primitive[]
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
            var deserialized = JsonConvert.DeserializeObject<Primitive>(json);

            // Assert
            Assert.NotNull(deserialized);
            Assert.Equal(original, deserialized);
        }
    }

    [Fact]
    public void Primitive_EdgeCase_EmptyString_HandledCorrectly()
    {
        // Arrange
        var primitiveType = new Primitive("");

        // Act & Assert
        Assert.Equal(PrimitiveKind.String, primitiveType.Kind);
        Assert.Equal("", primitiveType.AsString);
        Assert.Equal("", primitiveType.ToString());

        // Test serialization
        var json = JsonConvert.SerializeObject(primitiveType);
        var deserialized = JsonConvert.DeserializeObject<Primitive>(json);
        Assert.Equal(primitiveType, deserialized);
    }

    [Fact]
    public void Primitive_EdgeCase_EmptyByteArray_HandledCorrectly()
    {
        // Arrange
        var emptyArray = Array.Empty<byte>();
        var primitiveType = new Primitive(emptyArray);

        // Act & Assert
        Assert.Equal(PrimitiveKind.ByteArray, primitiveType.Kind);
        Assert.Empty(primitiveType.AsByteArray);
        Assert.Equal(Convert.ToBase64String(emptyArray), primitiveType.ToString());

        // Test serialization
        var json = JsonConvert.SerializeObject(primitiveType);
        var deserialized = JsonConvert.DeserializeObject<Primitive>(json);
        Assert.Equal(primitiveType, deserialized);
    }

    [Theory]
    [InlineData(long.MaxValue)]
    [InlineData(long.MinValue)]
    [InlineData(0L)]
    public void Primitive_EdgeCase_ExtremeIntegerValues_HandledCorrectly(long extremeValue)
    {
        // Arrange
        var primitiveType = new Primitive(extremeValue);

        // Act & Assert
        Assert.Equal(PrimitiveKind.Integer, primitiveType.Kind);
        Assert.Equal(extremeValue, primitiveType.AsInteger);
        Assert.Equal(extremeValue.ToString(), primitiveType.ToString());

        // Test serialization
        var json = JsonConvert.SerializeObject(primitiveType);
        var deserialized = JsonConvert.DeserializeObject<Primitive>(json);
        Assert.Equal(primitiveType, deserialized);
    }

    [Theory]
    [InlineData(double.MaxValue)]
    [InlineData(double.MinValue)]
    [InlineData(double.PositiveInfinity)]
    [InlineData(double.NegativeInfinity)]
    [InlineData(0.0)]
    public void Primitive_EdgeCase_ExtremeDoubleValues_HandledCorrectly(double extremeValue)
    {
        // Arrange
        var primitiveType = new Primitive(extremeValue);

        // Act & Assert
        Assert.Equal(PrimitiveKind.Double, primitiveType.Kind);
        Assert.Equal(extremeValue, primitiveType.AsDouble);

        if (!double.IsNaN(extremeValue))
        {
            // Test serialization (skip for NaN as it has special handling)
            var json = JsonConvert.SerializeObject(primitiveType);
            var deserialized = JsonConvert.DeserializeObject<Primitive>(json);
            Assert.Equal(primitiveType.AsDouble, deserialized!.AsDouble, precision: 10);
        }
    }

    [Fact]
    public void Primitive_EdgeCase_NaNDouble_HandledCorrectly()
    {
        // Arrange
        var primitiveType = new Primitive(double.NaN);

        // Act & Assert
        Assert.Equal(PrimitiveKind.Double, primitiveType.Kind);
        Assert.True(double.IsNaN(primitiveType.AsDouble));

        // NaN has special equality behavior
        var other = new Primitive(double.NaN);
        Assert.False(primitiveType.Equals(other)); // NaN != NaN
    }

    [Fact]
    public void Primitive_Performance_ManyCreationsAndAccesses()
    {
        // Arrange
        const int iterationCount = 10000;
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Act
        for (int i = 0; i < iterationCount; i++)
        {
            var stringType = new Primitive($"string_{i}");
            var intType = new Primitive(i);
            var doubleType = new Primitive((double)i);
            var byteType = new Primitive(new[] { (byte)i });

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
    public void Primitive_StressTest_ManyEqualityComparisons()
    {
        // Arrange
        const int comparisonCount = 1000;
        var types = new Primitive[]
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
    public void Primitive_StressTest_JsonSerializationPerformance()
    {
        // Arrange
        const int serializationCount = 1000;
        var testTypes = new Primitive[]
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
                var deserialized = JsonConvert.DeserializeObject<Primitive>(json);
                Assert.Equal(type, deserialized);
            }
        }

        stopwatch.Stop();

        // Assert
        Assert.True(stopwatch.ElapsedMilliseconds < 5000,
            $"JSON serialization stress test failed: took {stopwatch.ElapsedMilliseconds}ms");
    }

    [Fact]
    public void Primitive_IntegrationTest_ComplexScenario()
    {
        // Arrange - Complex scenario with all types and operations
        var stringType = new Primitive("integration test");
        var boolType = new Primitive(false);
        var intType = new Primitive(42L);
        var doubleType = new Primitive(3.14159);
        var byteType = new Primitive("Hello"u8.ToArray()); // "Hello"

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
            var deserialized = JsonConvert.DeserializeObject<Primitive>(json);
            Assert.Equal(type, deserialized);

            // Test equality and hash codes
            var copy = new Primitive(type);
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
    public void Primitive_ThreadSafety_ConcurrentOperations()
    {
        // Arrange
        var sharedType = new Primitive("thread safety test");
        const int taskCount = 10;
        const int operationsPerTask = 100;

        // Act
        var tasks = Enumerable.Range(0, taskCount).Select(taskId => Task.Run(() =>
        {
            for (int i = 0; i < operationsPerTask; i++)
            {
                // Test concurrent read operations (Primitive should be immutable)
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
                JsonConvert.DeserializeObject<Primitive>(json);
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
    public void Primitive_AsByteSpan_ReturnsCorrectSpan()
    {
        // Arrange
        var testBytes = new byte[] { 1, 2, 3, 4, 5 };
        var primitiveType = new Primitive(testBytes);

        // Act
        var span = primitiveType.AsByteSpan;

        // Assert
        Assert.Equal(testBytes.Length, span.Length);
        Assert.True(testBytes.AsSpan().SequenceEqual(span));
    }

    [Fact]
    public void Primitive_AsByteSpan_IsReadOnly()
    {
        // Arrange
        var testBytes = new byte[] { 1, 2, 3 };
        var primitiveType = new Primitive(testBytes);

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
    public void Primitive_CopyConstructor_WithNullArgument_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new Primitive((Primitive)null!));
    }

    [Fact]
    public void Primitive_ByteArrayConstructor_WithNullArray_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new Primitive((byte[])null!));
    }

    [Fact]
    public void Primitive_StringConstructor_WithNullString_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new Primitive((string)null!));
    }

    [Fact]
    public void Primitive_AllKinds_CoveredByTests()
    {
        // Arrange & Act - Create instances of all PrimitiveKind values
        var stringType = new Primitive("test");
        var booleanType = new Primitive(true);
        var integerType = new Primitive(42L);
        var doubleType = new Primitive(3.14);
        var byteArrayType = new Primitive(new byte[] { 1, 2, 3 });

        // Assert - All enum values are covered
        Assert.Equal(PrimitiveKind.String, stringType.Kind);
        Assert.Equal(PrimitiveKind.Boolean, booleanType.Kind);
        Assert.Equal(PrimitiveKind.Integer, integerType.Kind);
        Assert.Equal(PrimitiveKind.Double, doubleType.Kind);
        Assert.Equal(PrimitiveKind.ByteArray, byteArrayType.Kind);

        // Verify all enum values are tested (this ensures we don't miss new kinds)
        var testedKinds = new HashSet<PrimitiveKind>
        {
            stringType.Kind,
            booleanType.Kind,
            integerType.Kind,
            doubleType.Kind,
            byteArrayType.Kind
        };

        var allKinds = Enum.GetValues<PrimitiveKind>().ToHashSet();
        Assert.Equal(allKinds, testedKinds);
    }
}
