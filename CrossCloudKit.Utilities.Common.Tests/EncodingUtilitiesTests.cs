// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Xunit;

namespace CrossCloudKit.Utilities.Common.Tests;

public class EncodingUtilitiesTests
{
    [Theory]
    [InlineData("0123456789ABCDEF", true)]
    [InlineData("0123456789abcdef", true)]
    [InlineData("DeadBeef", true)]
    [InlineData("CAFEBABE", true)]
    [InlineData("FF", true)]
    [InlineData("00", true)]
    public void IsHexString_WithValidHexStrings_ReturnsTrue(string input, bool expected)
    {
        // Act
        var result = EncodingUtilities.IsHexString(input);

        // Assert
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData("G123", false)]
    [InlineData("12G3", false)]
    [InlineData("123G", false)]
    [InlineData("Hello", false)]
    [InlineData("123!", false)]
    [InlineData("12 34", false)]
    [InlineData("", false)]
    public void IsHexString_WithInvalidHexStrings_ReturnsFalse(string input, bool expected)
    {
        // Act
        var result = EncodingUtilities.IsHexString(input);

        // Assert
        Assert.Equal(expected, result);
    }

    [Fact]
    public void IsHexString_WithEmptyString_ReturnsFalse()
    {
        // Act
        var result = EncodingUtilities.IsHexString("");

        // Assert
        Assert.False(result);
    }

    [Theory]
    [InlineData("48656C6C6F", "Hello")]
    [InlineData("576F726C64", "World")]
    [InlineData("41", "A")]
    [InlineData("4142434445", "ABCDE")]
    [InlineData("313233", "123")]
    public void HexDecode_WithValidHexStrings_ReturnsCorrectAscii(string hexInput, string expectedOutput)
    {
        // Act
        var result = EncodingUtilities.HexDecode(hexInput);

        // Assert
        Assert.Equal(expectedOutput, result);
    }

    [Fact]
    public void HexDecode_WithOddLengthString_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        const string oddLengthHex = "ABC"; // 3 characters

        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => EncodingUtilities.HexDecode(oddLengthHex));
    }

    [Theory]
    [InlineData("XY")]
    [InlineData("GG")]
    [InlineData("ZZ")]
    public void HexDecode_WithInvalidHexCharacters_ThrowsFormatException(string invalidHex)
    {
        // Act & Assert
        Assert.Throws<FormatException>(() => EncodingUtilities.HexDecode(invalidHex));
    }

    [Fact]
    public void HexDecode_WithEmptyString_ReturnsEmptyString()
    {
        // Act
        var result = EncodingUtilities.HexDecode("");

        // Assert
        Assert.Equal("", result);
    }

    [Theory]
    [InlineData("Hello World")]
    [InlineData("")]
    [InlineData("Test123")]
    [InlineData("Special!@#$%^&*()")]
    [InlineData("Unicode: 世界")]
    public void Base64Encode_WithVariousStrings_ReturnsValidBase64(string input)
    {
        // Act
        var result = EncodingUtilities.Base64Encode(input);

        // Assert
        Assert.NotNull(result);
        Assert.True(IsValidBase64(result));
    }

    [Theory]
    [InlineData("SGVsbG8gV29ybGQ=", "Hello World")]
    [InlineData("", "")]
    [InlineData("VGVzdDEyMw==", "Test123")]
    [InlineData("QQ==", "A")]
    public void Base64Decode_WithValidBase64Strings_ReturnsCorrectText(string base64Input, string expectedOutput)
    {
        // Act
        var result = EncodingUtilities.Base64Decode(base64Input);

        // Assert
        Assert.Equal(expectedOutput, result);
    }

    [Fact]
    public void Base64Decode_WithInvalidBase64_ThrowsFormatException()
    {
        // Arrange
        const string invalidBase64 = "InvalidBase64String!";

        // Act & Assert
        Assert.Throws<FormatException>(() => EncodingUtilities.Base64Decode(invalidBase64));
    }

    [Theory]
    [InlineData("Hello World")]
    [InlineData("Test string with special chars: !@#$%")]
    [InlineData("Unicode test: 你好世界 🌍")]
    [InlineData("")]
    [InlineData("A")]
    [InlineData("Long string with various content including numbers 123456789")]
    public void Base64_EncodeDecodeRoundTrip_RetursOriginalString(string originalString)
    {
        // Act
        var encoded = EncodingUtilities.Base64Encode(originalString);
        var decoded = EncodingUtilities.Base64Decode(encoded);

        // Assert
        Assert.Equal(originalString, decoded);
    }

    [Theory]
    [InlineData("hello world", "hello%20world")]
    [InlineData("test/topic", "test%2Ftopic")]
    [InlineData("topic with spaces", "topic%20with%20spaces")]
    [InlineData("special!@#$", "special%21%40%23%24")]
    [InlineData("", "")]
    public void EncodeTopic_WithVariousInputs_ReturnsCorrectUrlEncoding(string input, string expected)
    {
        // Act
        var result = EncodingUtilities.EncodeTopic(input);

        // Assert
        Assert.Equal(expected, result);
    }

    [Fact]
    public void EncodeTopic_WithNull_ReturnsNull()
    {
        // Act
        var result = EncodingUtilities.EncodeTopic(null);

        // Assert
        Assert.Null(result);
    }

    [Theory]
    [InlineData("hello%20world", "hello world")]
    [InlineData("test%2Ftopic", "test/topic")]
    [InlineData("topic%20with%20spaces", "topic with spaces")]
    [InlineData("special%21%40%23%24", "special!@#$")]
    [InlineData("", "")]
    public void DecodeTopic_WithValidUrlEncodedStrings_ReturnsCorrectDecoding(string encodedInput, string expected)
    {
        // Act
        var result = EncodingUtilities.DecodeTopic(encodedInput);

        // Assert
        Assert.Equal(expected, result);
    }

    [Fact]
    public void DecodeTopic_WithNull_ReturnsNull()
    {
        // Act
        var result = EncodingUtilities.DecodeTopic(null);

        // Assert
        Assert.Null(result);
    }

    [Theory]
    [InlineData("hello world")]
    [InlineData("test/topic")]
    [InlineData("topic with special chars: !@#$%^&*()")]
    [InlineData("unicode: 世界 🌍")]
    [InlineData("")]
    [InlineData("simple")]
    public void Topic_EncodeDecodeRoundTrip_ReturnsOriginalString(string originalTopic)
    {
        // Act
        var encoded = EncodingUtilities.EncodeTopic(originalTopic);
        var decoded = EncodingUtilities.DecodeTopic(encoded);

        // Assert
        Assert.Equal(originalTopic, decoded);
    }

    [Fact]
    public void IsHexString_WithMixedCaseHex_ReturnsTrue()
    {
        // Arrange
        const string mixedCaseHex = "DeAdBeEf";

        // Act
        var result = EncodingUtilities.IsHexString(mixedCaseHex);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public void HexDecode_WithLowercaseHex_WorksCorrectly()
    {
        // Arrange
        const string lowercaseHex = "48656c6c6f"; // "Hello" in lowercase hex

        // Act
        var result = EncodingUtilities.HexDecode(lowercaseHex);

        // Assert
        Assert.Equal("Hello", result);
    }

    [Fact]
    public void HexDecode_WithMixedCaseHex_WorksCorrectly()
    {
        // Arrange
        const string mixedCaseHex = "48656C6c6F"; // "Hello" in mixed case hex

        // Act
        var result = EncodingUtilities.HexDecode(mixedCaseHex);

        // Assert
        Assert.Equal("Hello", result);
    }

    [Fact]
    public void Base64Encode_WithUnicodeString_HandlesUtf8Correctly()
    {
        // Arrange
        const string unicodeString = "Hello 世界 🌍";

        // Act
        var encoded = EncodingUtilities.Base64Encode(unicodeString);
        var decoded = EncodingUtilities.Base64Decode(encoded);

        // Assert
        Assert.Equal(unicodeString, decoded);
        Assert.True(IsValidBase64(encoded));
    }

    [Fact]
    public void EncodingUtilities_Performance_HandlesLargeStrings()
    {
        // Arrange
        var largeString = new string('A', 100000); // 100KB string
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Act
        var base64Encoded = EncodingUtilities.Base64Encode(largeString);
        var base64Decoded = EncodingUtilities.Base64Decode(base64Encoded);
        stopwatch.Stop();

        // Assert
        Assert.Equal(largeString, base64Decoded);
        Assert.True(stopwatch.ElapsedMilliseconds < 1000, $"Performance test failed: took {stopwatch.ElapsedMilliseconds}ms");
    }

    [Fact]
    public void IsHexString_Performance_HandlesLargeHexStrings()
    {
        // Arrange
        var largeHexString = string.Join("", Enumerable.Repeat("ABCDEF123456", 10000)); // Large hex string
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Act
        var result = EncodingUtilities.IsHexString(largeHexString);
        stopwatch.Stop();

        // Assert
        Assert.True(result);
        Assert.True(stopwatch.ElapsedMilliseconds < 100, $"Performance test failed: took {stopwatch.ElapsedMilliseconds}ms");
    }

    [Fact]
    public void HexDecode_WithControlCharacters_WorksCorrectly()
    {
        // Arrange - Test with control characters like newline (0x0A), carriage return (0x0D)
        const string hexWithControlChars = "48656C6C6F0A576F726C640D"; // "Hello\nWorld\r"

        // Act
        var result = EncodingUtilities.HexDecode(hexWithControlChars);

        // Assert
        Assert.Equal("Hello\nWorld\r", result);
    }

    [Theory]
    [InlineData("topic-name")]
    [InlineData("topic_name")]
    [InlineData("TopicName")]
    [InlineData("123")]
    [InlineData("topic.name")]
    public void EncodeTopic_WithAlreadyValidTopicNames_DoesNotChangeSimpleNames(string simpleTopic)
    {
        // Act
        var encoded = EncodingUtilities.EncodeTopic(simpleTopic);

        // Assert
        Assert.Equal(simpleTopic, encoded);
    }

    [Fact]
    public void EncodingUtilities_IntegrationTest_MultipleEncodingsWork()
    {
        // Arrange
        const string originalText = "Hello World 123!";

        // Act - Chain multiple encoding operations
        var base64Encoded = EncodingUtilities.Base64Encode(originalText);
        var topicEncoded = EncodingUtilities.EncodeTopic(base64Encoded);

        // Reverse the operations
        var topicDecoded = EncodingUtilities.DecodeTopic(topicEncoded);
        var base64Decoded = EncodingUtilities.Base64Decode(topicDecoded!);

        // Assert
        Assert.Equal(originalText, base64Decoded);
    }

    [Fact]
    public void EncodingUtilities_EdgeCase_EmptyAndWhitespaceStrings()
    {
        // Test empty string
        Assert.Equal("", EncodingUtilities.Base64Encode(""));
        Assert.Equal("", EncodingUtilities.Base64Decode(""));
        Assert.Equal("", EncodingUtilities.EncodeTopic(""));
        Assert.Equal("", EncodingUtilities.DecodeTopic(""));
        Assert.False(EncodingUtilities.IsHexString(""));
        Assert.Equal("", EncodingUtilities.HexDecode(""));

        // Test whitespace string
        const string whitespace = "   ";
        var base64Whitespace = EncodingUtilities.Base64Encode(whitespace);
        Assert.Equal(whitespace, EncodingUtilities.Base64Decode(base64Whitespace));

        var encodedWhitespace = EncodingUtilities.EncodeTopic(whitespace);
        Assert.Equal(whitespace, EncodingUtilities.DecodeTopic(encodedWhitespace));
    }

    [Theory]
    [InlineData("user@example.com")]
    [InlineData("path/to/resource")]
    [InlineData("query?param=value&other=123")]
    [InlineData("fragment#section")]
    public void Topic_EncodeDecode_HandlesUrlSpecialCharacters(string topicWithSpecialChars)
    {
        // Act
        var encoded = EncodingUtilities.EncodeTopic(topicWithSpecialChars);
        var decoded = EncodingUtilities.DecodeTopic(encoded);

        // Assert
        Assert.Equal(topicWithSpecialChars, decoded);
        Assert.NotEqual(topicWithSpecialChars, encoded); // Should be different after encoding
    }

    [Fact]
    public void HexDecode_WithNullByteInResult_HandlesCorrectly()
    {
        // Arrange - Hex for "A\0B" (A, null byte, B)
        const string hexWithNull = "41004";

        // Act & Assert - Should throw because odd length
        Assert.Throws<ArgumentOutOfRangeException>(() => EncodingUtilities.HexDecode(hexWithNull));

        // Test valid hex with null byte
        const string validHexWithNull = "410042"; // "A\0B"
        var result = EncodingUtilities.HexDecode(validHexWithNull);
        Assert.Equal("A\0B", result);
    }

    // Helper method to validate Base64 strings
    private static bool IsValidBase64(string base64String)
    {
        if (string.IsNullOrEmpty(base64String))
            return true;

        try
        {
            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            Convert.FromBase64String(base64String);
            return true;
        }
        catch
        {
            return false;
        }
    }
}
