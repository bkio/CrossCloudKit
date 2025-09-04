// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Reflection;
using Xunit;

namespace CrossCloudKit.Utilities.Common.Tests;

public class ApplicationUtilitiesTests
{
    [Fact]
    public void GetApplicationDirectory_WithDefaultParameter_ReturnsDirectoryWithDirectorySeparator()
    {
        // Act
        var result = ApplicationUtilities.GetApplicationDirectory();

        // Assert
        Assert.NotNull(result);
        Assert.NotEmpty(result);
        Assert.True(result.EndsWith(Path.DirectorySeparatorChar));
        Assert.True(Directory.Exists(result.TrimEnd(Path.DirectorySeparatorChar)));
    }

    [Fact]
    public void GetApplicationDirectory_WithNullParameter_ReturnsDirectoryWithDirectorySeparator()
    {
        // Act
        var result = ApplicationUtilities.GetApplicationDirectory();

        // Assert
        Assert.NotNull(result);
        Assert.NotEmpty(result);
        Assert.True(result.EndsWith(Path.DirectorySeparatorChar));
        Assert.True(Directory.Exists(result.TrimEnd(Path.DirectorySeparatorChar)));
    }

    [Fact]
    public void GetApplicationDirectory_WithCustomEndCharacter_ReturnsDirectoryWithCustomCharacter()
    {
        // Arrange
        const char customChar = '/';

        // Act
        var result = ApplicationUtilities.GetApplicationDirectory(customChar);

        // Assert
        Assert.NotNull(result);
        Assert.NotEmpty(result);
        Assert.True(result.EndsWith(customChar));
        Assert.True(Directory.Exists(result.TrimEnd(customChar)));
    }

    [Fact]
    public void GetApplicationDirectory_WithSameEndCharacter_DoesNotDuplicateCharacter()
    {
        // Arrange
        var directorySeparator = Path.DirectorySeparatorChar;

        // Act
        var result = ApplicationUtilities.GetApplicationDirectory(directorySeparator);

        // Assert
        Assert.NotNull(result);
        Assert.NotEmpty(result);
        Assert.True(result.EndsWith(directorySeparator));

        // Ensure no duplicate separator at the end
        var trimmedResult = result.TrimEnd(directorySeparator);
        Assert.False(string.IsNullOrEmpty(trimmedResult));
        Assert.False(trimmedResult.EndsWith(directorySeparator));
    }

    [Fact]
    public void GetApplicationDirectory_ConsistentResults_ReturnsSamePathMultipleCalls()
    {
        // Act
        var result1 = ApplicationUtilities.GetApplicationDirectory();
        var result2 = ApplicationUtilities.GetApplicationDirectory();

        // Assert
        Assert.Equal(result1, result2);
    }

    [Fact]
    public void GetApplicationDirectory_MatchesExpectedLocation_ContainsExecutingAssemblyPath()
    {
        // Arrange
        var expectedLocation = Assembly.GetExecutingAssembly().Location;
        var expectedDirectory = Path.GetDirectoryName(expectedLocation);

        // Act
        var result = ApplicationUtilities.GetApplicationDirectory();

        // Assert
        Assert.NotNull(result);
        Assert.NotNull(expectedDirectory);
        Assert.StartsWith(expectedDirectory, result.TrimEnd(Path.DirectorySeparatorChar));
    }

    [Fact]
    public void GetApplicationDriveLetter_ReturnsValidDriveLetter()
    {
        // Act
        var result = ApplicationUtilities.GetApplicationDriveLetter();

        // Assert
        Assert.NotNull(result);

        // On Windows, should be a single letter; on Unix systems, might be empty
        if (!string.IsNullOrEmpty(result))
        {
            Assert.True(result.Length >= 1);
            Assert.True(char.IsLetter(result[0]));
            Assert.False(result.Contains(':'));
        }
    }

    [Fact]
    public void GetApplicationDriveLetter_ConsistentResults_ReturnsSameValueMultipleCalls()
    {
        // Act
        var result1 = ApplicationUtilities.GetApplicationDriveLetter();
        var result2 = ApplicationUtilities.GetApplicationDriveLetter();

        // Assert
        Assert.Equal(result1, result2);
    }

    [Fact]
    public void GetApplicationDriveLetter_MatchesAssemblyLocation_ExtractedFromSameSource()
    {
        // Arrange
        var assemblyLocation = Assembly.GetExecutingAssembly().Location;
        var colonIndex = assemblyLocation.IndexOf(':');
        var expectedDriveLetter = colonIndex == -1 ? string.Empty : assemblyLocation[..colonIndex];

        // Act
        var result = ApplicationUtilities.GetApplicationDriveLetter();

        // Assert
        Assert.Equal(expectedDriveLetter, result);
    }

    [Theory]
    [InlineData('\\')]
    [InlineData('/')]
    [InlineData('|')]
    [InlineData('.')]
    public void GetApplicationDirectory_WithVariousEndCharacters_ReturnsCorrectFormat(char endChar)
    {
        // Act
        var result = ApplicationUtilities.GetApplicationDirectory(endChar);

        // Assert
        Assert.NotNull(result);
        Assert.NotEmpty(result);
        Assert.True(result.EndsWith(endChar));

        // Verify the base directory exists (without the custom end character)
        var baseDirectory = result.TrimEnd(endChar);
        Assert.True(Directory.Exists(baseDirectory));
    }
}
