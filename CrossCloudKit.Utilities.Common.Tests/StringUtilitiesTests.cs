// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Xunit;

namespace CrossCloudKit.Utilities.Common.Tests;

public class StringUtilitiesTests
{
    #region TrimStart Tests

    [Fact]
    public void TrimStart_WithValidInput_RemovesSpecifiedStringFromStart()
    {
        // Arrange
        var input = "HelloHelloWorld";
        var trimString = "Hello";

        // Act
        var result = input.TrimStart(trimString);

        // Assert
        Assert.Equal("World", result);
    }

    [Fact]
    public void TrimStart_WithMultipleOccurrencesAtStart_RemovesAllOccurrences()
    {
        // Arrange
        var input = "abcabcabctest";
        var trimString = "abc";

        // Act
        var result = input.TrimStart(trimString);

        // Assert
        Assert.Equal("test", result);
    }

    [Fact]
    public void TrimStart_WithNullTrimString_ReturnsOriginal()
    {
        // Arrange
        var input = "test";

        // Act
        var result = input.TrimStart(null);

        // Assert
        Assert.Equal("test", result);
    }

    [Fact]
    public void TrimStart_WithEmptyTrimString_ReturnsOriginal()
    {
        // Arrange
        var input = "test";
        var trimString = "";

        // Act
        var result = input.TrimStart(trimString);

        // Assert
        Assert.Equal("test", result);
    }

    [Fact]
    public void TrimStart_WithNullInput_ThrowsArgumentNullException()
    {
        // Arrange
        string? input = null;
        var trimString = "test";

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => input!.TrimStart(trimString));
    }

    #endregion

    #region TrimEnd Tests

    [Fact]
    public void TrimEnd_WithValidInput_RemovesSpecifiedStringFromEnd()
    {
        // Arrange
        var input = "WorldHelloHello";
        var trimString = "Hello";

        // Act
        var result = input.TrimEnd(trimString);

        // Assert
        Assert.Equal("World", result);
    }

    [Fact]
    public void TrimEnd_WithMultipleOccurrencesAtEnd_RemovesAllOccurrences()
    {
        // Arrange
        var input = "testabcabcabc";
        var trimString = "abc";

        // Act
        var result = input.TrimEnd(trimString);

        // Assert
        Assert.Equal("test", result);
    }

    [Fact]
    public void TrimEnd_WithNullTrimString_ReturnsOriginal()
    {
        // Arrange
        var input = "test";

        // Act
        var result = input.TrimEnd(null);

        // Assert
        Assert.Equal("test", result);
    }

    [Fact]
    public void TrimEnd_WithNullInput_ThrowsArgumentNullException()
    {
        // Arrange
        string? input = null;
        const string trimString = "test";

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => input!.TrimEnd(trimString));
    }

    #endregion

    #region Trim Tests

    [Fact]
    public void Trim_WithValidInput_RemovesSpecifiedStringFromBothEnds()
    {
        // Arrange
        var input = "HelloWorldHello";
        var trimString = "Hello";

        // Act
        var result = input.Trim(trimString);

        // Assert
        Assert.Equal("World", result);
    }

    [Fact]
    public void Trim_WithMultipleOccurrences_RemovesFromBothEnds()
    {
        // Arrange
        var input = "abcabctestabcabc";
        var trimString = "abc";

        // Act
        var result = input.Trim(trimString);

        // Assert
        Assert.Equal("test", result);
    }

    #endregion

    #region GenerateRandomString Tests

    [Fact]
    public void GenerateRandomString_WithValidLength_ReturnsStringOfCorrectLength()
    {
        // Arrange
        var length = 10;

        // Act
        var result = StringUtilities.GenerateRandomString(length);

        // Assert
        Assert.Equal(length, result.Length);
    }

    [Fact]
    public void GenerateRandomString_WithLowercase_ReturnsLowercaseString()
    {
        // Arrange
        var length = 10;

        // Act
        var result = StringUtilities.GenerateRandomString(length, CaseOptions.FullLowercase);

        // Assert
        Assert.Equal(result.ToLowerInvariant(), result);
    }

    [Fact]
    public void GenerateRandomString_WithUppercase_ReturnsUppercaseString()
    {
        // Arrange
        var length = 10;

        // Act
        var result = StringUtilities.GenerateRandomString(length, CaseOptions.FullUppercase);

        // Assert
        Assert.Equal(result.ToUpperInvariant(), result);
    }

    [Fact]
    public void GenerateRandomString_WithUppercaseAndDigits_ContainsUppercaseAndDigits()
    {
        // Arrange
        var length = 100;

        // Act
        var result = StringUtilities.GenerateRandomString(length, CaseOptions.FullUppercase, includeDigits: true);

        // Assert
        Assert.True(result.All(c => char.IsUpper(c) || char.IsDigit(c)));
        Assert.True(result.All(char.IsLetterOrDigit));
    }

    [Fact]
    public void GenerateRandomString_WithUppercaseWithoutDigits_ContainsOnlyUppercaseLetters()
    {
        // Arrange
        var length = 100;

        // Act
        var result = StringUtilities.GenerateRandomString(length, CaseOptions.FullUppercase, includeDigits: false);

        // Assert
        Assert.True(result.All(char.IsUpper));
        Assert.True(result.All(char.IsLetter));
        Assert.DoesNotContain(result, char.IsDigit);
    }

    [Fact]
    public void GenerateRandomString_WithLowercaseAndDigits_ContainsLowercaseAndDigits()
    {
        // Arrange
        var length = 100;

        // Act
        var result = StringUtilities.GenerateRandomString(length, CaseOptions.FullLowercase, includeDigits: true);

        // Assert
        Assert.True(result.All(c => char.IsLower(c) || char.IsDigit(c)));
        Assert.True(result.All(char.IsLetterOrDigit));
    }

    [Fact]
    public void GenerateRandomString_WithLowercaseWithoutDigits_ContainsOnlyLowercaseLetters()
    {
        // Arrange
        var length = 100;

        // Act
        var result = StringUtilities.GenerateRandomString(length, CaseOptions.FullLowercase, includeDigits: false);

        // Assert
        Assert.True(result.All(char.IsLower));
        Assert.True(result.All(char.IsLetter));
        Assert.DoesNotContain(result, char.IsDigit);
    }

    [Fact]
    public void GenerateRandomString_WithMixedCase_ContainsBothUpperAndLowercase()
    {
        // Arrange
        var length = 100;

        // Act
        var result = StringUtilities.GenerateRandomString(length);

        // Assert
        Assert.True(result.All(char.IsLetter));
        // With a length of 100, it's statistically almost certain to have both cases
        Assert.Contains(result, char.IsUpper);
        Assert.Contains(result, char.IsLower);
    }

    [Fact]
    public void GenerateRandomString_WithMixedCaseAndDigits_ContainsLettersAndDigits()
    {
        // Arrange
        var length = 100;

        // Act
        var result = StringUtilities.GenerateRandomString(length, includeDigits: true);

        // Assert
        Assert.True(result.All(char.IsLetterOrDigit));
        // With a length of 100, it's statistically almost certain to have both letters and digits
        Assert.Contains(result, char.IsLetter);
    }

    [Fact]
    public void GenerateRandomString_WithIncludeDigits_ContainsOnlyLettersAndDigits()
    {
        // Arrange
        var length = 100;

        // Act
        var result = StringUtilities.GenerateRandomString(length, includeDigits: true);

        // Assert
        Assert.True(result.All(char.IsLetterOrDigit));
    }

    [Fact]
    public void GenerateRandomString_WithoutIncludeDigits_ContainsOnlyLetters()
    {
        // Arrange
        var length = 100;

        // Act
        var result = StringUtilities.GenerateRandomString(length, includeDigits: false);

        // Assert
        Assert.True(result.All(char.IsLetter));
    }

    [Fact]
    public void GenerateRandomString_WithZeroLength_ThrowsArgumentOutOfRangeException()
    {
        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => StringUtilities.GenerateRandomString(0));
    }

    [Fact]
    public void GenerateRandomString_WithNegativeLength_ThrowsArgumentOutOfRangeException()
    {
        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => StringUtilities.GenerateRandomString(-1));
    }

    [Fact]
    public void GenerateRandomString_WithInvalidCaseOption_ThrowsArgumentOutOfRangeException()
    {
        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => StringUtilities.GenerateRandomString(10, (CaseOptions)999));
    }

    #endregion

    #region WildcardToRegex Tests

    [Fact]
    public void WildcardToRegex_WithAsterisk_ConvertsToRegexPattern()
    {
        // Arrange
        var wildcardPattern = "test*";

        // Act
        var result = StringUtilities.WildcardToRegex(wildcardPattern);

        // Assert
        Assert.Equal("^test.*$", result);
    }

    [Fact]
    public void WildcardToRegex_WithQuestionMark_ConvertsToRegexPattern()
    {
        // Arrange
        var wildcardPattern = "test?";

        // Act
        var result = StringUtilities.WildcardToRegex(wildcardPattern);

        // Assert
        Assert.Equal("^test.$", result);
    }

    [Fact]
    public void WildcardToRegex_WithMixedWildcards_ConvertsCorrectly()
    {
        // Arrange
        var wildcardPattern = "test*file?.txt";

        // Act
        var result = StringUtilities.WildcardToRegex(wildcardPattern);

        // Assert
        Assert.Equal("^test.*file.\\.txt$", result);
    }

    [Fact]
    public void WildcardToRegex_WithNullInput_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => StringUtilities.WildcardToRegex(null));
    }

    #endregion

    #region SanitizeElasticsearchIndexName Tests

    [Fact]
    public void SanitizeElasticsearchIndexName_WithUppercase_ConvertsToLowercase()
    {
        // Arrange
        var input = "TestIndex";

        // Act
        var result = StringUtilities.SanitizeElasticsearchIndexName(input);

        // Assert
        Assert.Equal("testindex", result);
    }

    [Fact]
    public void SanitizeElasticsearchIndexName_WithIllegalCharacters_RemovesCharacters()
    {
        // Arrange
        var input = "test/index*with?illegal<chars>";

        // Act
        var result = StringUtilities.SanitizeElasticsearchIndexName(input);

        // Assert
        Assert.Equal("testindexwithillegalchars", result);
    }

    [Fact]
    public void SanitizeElasticsearchIndexName_StartingWithSymbol_AddsLogPrefix()
    {
        // Arrange
        var input = "_testindex";

        // Act
        var result = StringUtilities.SanitizeElasticsearchIndexName(input);

        // Assert
        Assert.Equal("log__testindex", result);
    }

    [Fact]
    public void SanitizeElasticsearchIndexName_TooLong_TruncatesToMaxLength()
    {
        // Arrange
        var input = new string('a', 300);

        // Act
        var result = StringUtilities.SanitizeElasticsearchIndexName(input);

        // Assert
        Assert.Equal(255, result.Length);
    }

    [Fact]
    public void SanitizeElasticsearchIndexName_WithNullInput_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => StringUtilities.SanitizeElasticsearchIndexName(null));
    }

    #endregion

    #region ReplaceFirst Tests

    [Fact]
    public void ReplaceFirst_WithExistingString_ReplacesFirstOccurrence()
    {
        // Arrange
        var input = "hello world hello";

        // Act
        var result = input.ReplaceFirst("hello", "hi");

        // Assert
        Assert.Equal("hi world hello", result);
    }

    [Fact]
    public void ReplaceFirst_WithNonExistingString_ReturnsOriginal()
    {
        // Arrange
        var input = "hello world";

        // Act
        var result = input.ReplaceFirst("goodbye", "hi");

        // Assert
        Assert.Equal("hello world", result);
    }

    [Fact]
    public void ReplaceFirst_WithEmptyReplacement_RemovesFirstOccurrence()
    {
        // Arrange
        var input = "hello world hello";

        // Act
        var result = input.ReplaceFirst("hello ", "");

        // Assert
        Assert.Equal("world hello", result);
    }

    #endregion

    #region ToSentenceCase Tests

    [Fact]
    public void ToSentenceCase_WithCamelCase_AddsSpaces()
    {
        // Arrange
        var input = "camelCaseString";

        // Act
        var result = input.ToSentenceCase();

        // Assert
        Assert.Equal("camel case string", result);
    }

    [Fact]
    public void ToSentenceCase_WithPascalCase_AddsSpaces()
    {
        // Arrange
        var input = "PascalCaseString";

        // Act
        var result = input.ToSentenceCase();

        // Assert
        Assert.Equal("Pascal case string", result);
    }

    [Fact]
    public void ToSentenceCase_WithNoUppercase_ReturnsOriginal()
    {
        // Arrange
        var input = "lowercase";

        // Act
        var result = input.ToSentenceCase();

        // Assert
        Assert.Equal("lowercase", result);
    }

    #endregion

    #region SnakeCaseToTitleCase Tests

    [Fact]
    public void SnakeCaseToTitleCase_WithSnakeCase_ConvertsTitleCase()
    {
        // Arrange
        var input = "snake_case_string";

        // Act
        var result = input.SnakeCaseToTitleCase();

        // Assert
        Assert.Equal("Snake Case String", result);
    }

    [Fact]
    public void SnakeCaseToTitleCase_WithSingleWord_CapitalizesFirst()
    {
        // Arrange
        var input = "word";

        // Act
        var result = input.SnakeCaseToTitleCase();

        // Assert
        Assert.Equal("Word", result);
    }

    [Fact]
    public void SnakeCaseToTitleCase_WithEmptyString_ReturnsEmpty()
    {
        // Arrange
        var input = "";

        // Act
        var result = input.SnakeCaseToTitleCase();

        // Assert
        Assert.Equal("", result);
    }

    #endregion

    #region MakeValidFileName Tests

    [Fact]
    public void MakeValidFileName_WithInvalidCharacters_ReplacesWithUnderscore()
    {
        // Arrange
        var input = "file<name>with|invalid*chars";

        // Act
        var result = input.MakeValidFileName();

        // Assert
        Assert.False(result.Contains('<'));
        Assert.False(result.Contains('>'));
        Assert.False(result.Contains('|'));
        Assert.False(result.Contains('*'));
    }

    [Fact]
    public void MakeValidFileName_WithValidName_ReturnsUnchanged()
    {
        // Arrange
        var input = "validfilename.txt";

        // Act
        var result = input.MakeValidFileName();

        // Assert
        Assert.Equal("validfilename.txt", result);
    }

    #endregion

    #region LimitMaxWords Tests

    [Fact]
    public void LimitMaxWords_WithMoreWordsThanLimit_TruncatesWords()
    {
        // Arrange
        var input = "one two three four five";

        // Act
        var result = input.LimitMaxWords(' ', 3);

        // Assert
        Assert.Equal("one two three", result);
    }

    [Fact]
    public void LimitMaxWords_WithFewerWordsThanLimit_ReturnsOriginal()
    {
        // Arrange
        var input = "one two";

        // Act
        var result = input.LimitMaxWords(' ', 5);

        // Assert
        Assert.Equal("one two", result);
    }

    [Fact]
    public void LimitMaxWords_WithCustomSeparator_WorksCorrectly()
    {
        // Arrange
        var input = "one,two,three,four";

        // Act
        var result = input.LimitMaxWords(',', 2);

        // Assert
        Assert.Equal("one,two", result);
    }

    #endregion

    #region LimitMaxCharacters Tests

    [Fact]
    public void LimitMaxCharacters_WithLongerString_TruncatesWithEllipsis()
    {
        // Arrange
        var input = "This is a very long string";

        // Act
        var result = input.LimitMaxCharacters(10);

        // Assert
        Assert.Equal("This is...", result);
    }

    [Fact]
    public void LimitMaxCharacters_WithShorterString_ReturnsOriginal()
    {
        // Arrange
        var input = "Short";

        // Act
        var result = input.LimitMaxCharacters(10);

        // Assert
        Assert.Equal("Short", result);
    }

    [Fact]
    public void LimitMaxCharacters_WithExactLength_ReturnsOriginal()
    {
        // Arrange
        var input = "Exactly10!";

        // Act
        var result = input.LimitMaxCharacters(10);

        // Assert
        Assert.Equal("Exactly10!", result);
    }

    #endregion

    #region SanitizeToSlug Tests

    [Fact]
    public void SanitizeToSlug_WithSpecialCharacters_CreatesValidSlug()
    {
        // Arrange
        var input = "Hello World! This is a Test.";

        // Act
        var result = input.SanitizeToSlug();

        // Assert
        Assert.Equal("hello-world-this-is-a-test", result);
    }

    [Fact]
    public void SanitizeToSlug_WithMultipleSpaces_NormalizesToSingleHyphens()
    {
        // Arrange
        var input = "Multiple    spaces   here";

        // Act
        var result = input.SanitizeToSlug();

        // Assert
        Assert.Equal("multiple-spaces-here", result);
    }

    [Fact]
    public void SanitizeToSlug_WithLeadingTrailingHyphens_TrimsHyphens()
    {
        // Arrange
        var input = "--test string--";

        // Act
        var result = input.SanitizeToSlug();

        // Assert
        Assert.False(result.StartsWith("-"));
        Assert.False(result.EndsWith("-"));
    }

    [Fact]
    public void SanitizeToSlug_WithAtSymbol_ReplacesWithHyphen()
    {
        // Arrange
        var input = "email@domain.com";

        // Act
        var result = input.SanitizeToSlug();

        // Assert
        Assert.Equal("email-domain-com", result);
    }

    #endregion
}
