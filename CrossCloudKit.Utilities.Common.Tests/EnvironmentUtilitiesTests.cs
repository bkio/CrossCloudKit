// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Xunit;

namespace CrossCloudKit.Utilities.Common.Tests;

public class EnvironmentUtilitiesTests
{
    [Fact]
    public void GetRequiredEnvironmentVariables_WithExistingVariables_ReturnsCorrectDictionary()
    {
        // Arrange
        const string testKey1 = "TEST_ENV_VAR_1";
        const string testKey2 = "TEST_ENV_VAR_2";
        const string testValue1 = "TestValue1";
        const string testValue2 = "TestValue2";

        Environment.SetEnvironmentVariable(testKey1, testValue1);
        Environment.SetEnvironmentVariable(testKey2, testValue2);

        try
        {
            var variableKeyOptions = new[]
            {
                new[] { testKey1 },
                new[] { testKey2 }
            };

            // Act
            var result = EnvironmentUtilities.GetRequiredEnvironmentVariables(variableKeyOptions);

            // Assert
            Assert.Equal(2, result.Count);
            Assert.Equal(testValue1, result[testKey1]);
            Assert.Equal(testValue2, result[testKey2]);
        }
        finally
        {
            Environment.SetEnvironmentVariable(testKey1, null);
            Environment.SetEnvironmentVariable(testKey2, null);
        }
    }

    [Fact]
    public void GetRequiredEnvironmentVariables_WithFallbackOptions_UsesFirstAvailable()
    {
        // Arrange
        const string primaryKey = "MISSING_ENV_VAR";
        const string fallbackKey = "FALLBACK_ENV_VAR";
        const string fallbackValue = "FallbackValue";

        Environment.SetEnvironmentVariable(primaryKey, null); // Ensure primary doesn't exist
        Environment.SetEnvironmentVariable(fallbackKey, fallbackValue);

        try
        {
            var variableKeyOptions = new[]
            {
                new[] { primaryKey, fallbackKey }
            };

            // Act
            var result = EnvironmentUtilities.GetRequiredEnvironmentVariables(variableKeyOptions);

            // Assert
            Assert.Single(result);
            Assert.Equal(fallbackValue, result[fallbackKey]);
            Assert.False(result.ContainsKey(primaryKey));
        }
        finally
        {
            Environment.SetEnvironmentVariable(fallbackKey, null);
        }
    }

    [Fact]
    public void GetRequiredEnvironmentVariables_WithMissingVariable_ThrowsInvalidOperationException()
    {
        // Arrange
        const string missingKey1 = "DEFINITELY_MISSING_ENV_VAR_1";
        const string missingKey2 = "DEFINITELY_MISSING_ENV_VAR_2";

        Environment.SetEnvironmentVariable(missingKey1, null);
        Environment.SetEnvironmentVariable(missingKey2, null);

        var variableKeyOptions = new[]
        {
            new[] { missingKey1, missingKey2 }
        };

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() =>
            EnvironmentUtilities.GetRequiredEnvironmentVariables(variableKeyOptions));

        Assert.Contains(missingKey1, exception.Message);
        Assert.Contains(missingKey2, exception.Message);
        Assert.Contains("Required environment variable not found", exception.Message);
    }

    [Fact]
    public void GetRequiredEnvironmentVariables_WithEmptyOptions_ThrowsArgumentException()
    {
        // Arrange
        var emptyOptions = Array.Empty<IEnumerable<string>>();

        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(() =>
            EnvironmentUtilities.GetRequiredEnvironmentVariables(emptyOptions));

        Assert.Contains("No environment variable options provided", exception.Message);
        Assert.Equal("variableKeyOptions", exception.ParamName);
    }

    [Fact]
    public void GetRequiredEnvironmentVariables_WithEmptyKeyOptions_ThrowsArgumentException()
    {
        // Arrange
        var variableKeyOptions = new[]
        {
            Array.Empty<string>()
        };

        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(() =>
            EnvironmentUtilities.GetRequiredEnvironmentVariables(variableKeyOptions));

        Assert.Contains("Empty environment variable key options provided", exception.Message);
        Assert.Equal("variableKeyOptions", exception.ParamName);
    }

    [Fact]
    public void GetRequiredEnvironmentVariables_WithEmptyStringValue_ThrowsInvalidOperationException()
    {
        // Arrange
        const string testKey = "TEST_EMPTY_ENV_VAR";
        Environment.SetEnvironmentVariable(testKey, ""); // Set to empty string

        try
        {
            var variableKeyOptions = new[]
            {
                new[] { testKey }
            };

            // Act & Assert
            Assert.Throws<InvalidOperationException>(() =>
                EnvironmentUtilities.GetRequiredEnvironmentVariables(variableKeyOptions));
        }
        finally
        {
            Environment.SetEnvironmentVariable(testKey, null);
        }
    }

    [Fact]
    public void TryGetRequiredEnvironmentVariables_WithExistingVariables_ReturnsTrue()
    {
        // Arrange
        const string testKey1 = "TRY_TEST_ENV_VAR_1";
        const string testKey2 = "TRY_TEST_ENV_VAR_2";
        const string testValue1 = "TryTestValue1";
        const string testValue2 = "TryTestValue2";

        Environment.SetEnvironmentVariable(testKey1, testValue1);
        Environment.SetEnvironmentVariable(testKey2, testValue2);

        try
        {
            var variableKeyOptions = new[]
            {
                new[] { testKey1 },
                new[] { testKey2 }
            };

            // Act
            var success = EnvironmentUtilities.TryGetRequiredEnvironmentVariables(variableKeyOptions, out var result);

            // Assert
            Assert.True(success);
            Assert.NotNull(result);
            Assert.Equal(2, result.Count);
            Assert.Equal(testValue1, result[testKey1]);
            Assert.Equal(testValue2, result[testKey2]);
        }
        finally
        {
            Environment.SetEnvironmentVariable(testKey1, null);
            Environment.SetEnvironmentVariable(testKey2, null);
        }
    }

    [Fact]
    public void TryGetRequiredEnvironmentVariables_WithMissingVariables_ReturnsFalse()
    {
        // Arrange
        const string missingKey = "TRY_MISSING_ENV_VAR";
        Environment.SetEnvironmentVariable(missingKey, null);

        var variableKeyOptions = new[]
        {
            new[] { missingKey }
        };

        // Act
        var success = EnvironmentUtilities.TryGetRequiredEnvironmentVariables(variableKeyOptions, out var result);

        // Assert
        Assert.False(success);
        Assert.Null(result);
    }

    [Fact]
    public void TryGetRequiredEnvironmentVariables_WithInvalidOptions_ReturnsFalse()
    {
        // Arrange
        var emptyOptions = Array.Empty<IEnumerable<string>>();

        // Act
        var success = EnvironmentUtilities.TryGetRequiredEnvironmentVariables(emptyOptions, out var result);

        // Assert
        Assert.False(success);
        Assert.Null(result);
    }

    [Theory]
    [InlineData("PATH")]        // Common on all platforms
    [InlineData("HOME")]        // Unix-like systems
    [InlineData("USERPROFILE")] // Windows
    public void GetRequiredEnvironmentVariables_WithCommonSystemVariables_WorksCorrectly(string systemVariable)
    {
        // Arrange - Only test if the variable actually exists
        var actualValue = Environment.GetEnvironmentVariable(systemVariable);
        if (actualValue == null)
        {
            // Skip test if system variable doesn't exist
            return;
        }

        var variableKeyOptions = new[]
        {
            new[] { systemVariable }
        };

        // Act
        var result = EnvironmentUtilities.GetRequiredEnvironmentVariables(variableKeyOptions);

        // Assert
        Assert.Single(result);
        Assert.Equal(actualValue, result[systemVariable]);
    }

    [Fact]
    public void GetRequiredEnvironmentVariables_WithMultipleFallbacks_UsesFirstAvailable()
    {
        // Arrange
        const string primary = "PRIMARY_MISSING";
        const string secondary = "SECONDARY_MISSING";
        const string tertiary = "TERTIARY_AVAILABLE";
        const string quaternary = "QUATERNARY_AVAILABLE";
        const string tertiaryValue = "TertiaryValue";
        const string quaternaryValue = "QuaternaryValue";

        Environment.SetEnvironmentVariable(primary, null);
        Environment.SetEnvironmentVariable(secondary, null);
        Environment.SetEnvironmentVariable(tertiary, tertiaryValue);
        Environment.SetEnvironmentVariable(quaternary, quaternaryValue);

        try
        {
            var variableKeyOptions = new[]
            {
                new[] { primary, secondary, tertiary, quaternary }
            };

            // Act
            var result = EnvironmentUtilities.GetRequiredEnvironmentVariables(variableKeyOptions);

            // Assert
            Assert.Single(result);
            Assert.Equal(tertiaryValue, result[tertiary]);
            Assert.False(result.ContainsKey(quaternary)); // Should stop at first found
        }
        finally
        {
            Environment.SetEnvironmentVariable(tertiary, null);
            Environment.SetEnvironmentVariable(quaternary, null);
        }
    }

    [Fact]
    public void GetRequiredEnvironmentVariables_WithComplexScenario_HandlesCorrectly()
    {
        // Arrange - Complex scenario with multiple variable groups and fallbacks
        const string group1Primary = "G1_PRIMARY";
        const string group1Fallback = "G1_FALLBACK";
        const string group2Single = "G2_SINGLE";
        const string group3Primary = "G3_PRIMARY";
        const string group3Secondary = "G3_SECONDARY";

        const string g1Value = "Group1Value";
        const string g2Value = "Group2Value";
        const string g3Value = "Group3Value";

        Environment.SetEnvironmentVariable(group1Primary, null);    // Missing, should use fallback
        Environment.SetEnvironmentVariable(group1Fallback, g1Value);
        Environment.SetEnvironmentVariable(group2Single, g2Value);   // Single option, available
        Environment.SetEnvironmentVariable(group3Primary, g3Value);  // Primary available
        Environment.SetEnvironmentVariable(group3Secondary, "ShouldNotUse");

        try
        {
            var variableKeyOptions = new[]
            {
                new[] { group1Primary, group1Fallback },
                new[] { group2Single },
                new[] { group3Primary, group3Secondary }
            };

            // Act
            var result = EnvironmentUtilities.GetRequiredEnvironmentVariables(variableKeyOptions);

            // Assert
            Assert.Equal(3, result.Count);
            Assert.Equal(g1Value, result[group1Fallback]);
            Assert.Equal(g2Value, result[group2Single]);
            Assert.Equal(g3Value, result[group3Primary]);
        }
        finally
        {
            Environment.SetEnvironmentVariable(group1Fallback, null);
            Environment.SetEnvironmentVariable(group2Single, null);
            Environment.SetEnvironmentVariable(group3Primary, null);
            Environment.SetEnvironmentVariable(group3Secondary, null);
        }
    }

    [Fact]
    public void EnvironmentUtilities_Performance_HandlesLargeNumberOfVariables()
    {
        // Arrange
        const int variableCount = 100;
        var variableKeys = new List<string>();
        var variableKeyOptions = new List<string[]>();

        for (var i = 0; i < variableCount; i++)
        {
            var key = $"PERF_TEST_VAR_{i}";
            variableKeys.Add(key);
            Environment.SetEnvironmentVariable(key, $"Value_{i}");
            variableKeyOptions.Add([key]);
        }

        try
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            // Act
            var result = EnvironmentUtilities.GetRequiredEnvironmentVariables(variableKeyOptions);
            stopwatch.Stop();

            // Assert
            Assert.Equal(variableCount, result.Count);
            Assert.True(stopwatch.ElapsedMilliseconds < 1000,
                $"Performance test failed: {variableCount} variables took {stopwatch.ElapsedMilliseconds}ms");
        }
        finally
        {
            foreach (var key in variableKeys)
            {
                Environment.SetEnvironmentVariable(key, null);
            }
        }
    }

    [Fact]
    public void GetRequiredEnvironmentVariables_WithWhitespaceValues_TreatsAsEmpty()
    {
        // Arrange
        const string testKey = "WHITESPACE_TEST_VAR";
        Environment.SetEnvironmentVariable(testKey, "   "); // Whitespace only

        try
        {
            var variableKeyOptions = new[]
            {
                new[] { testKey }
            };

            // Act & Assert
            Assert.Throws<InvalidOperationException>(() =>
                EnvironmentUtilities.GetRequiredEnvironmentVariables(variableKeyOptions));
        }
        finally
        {
            Environment.SetEnvironmentVariable(testKey, null);
        }
    }

    [Fact]
    public void TryGetRequiredEnvironmentVariables_WithWhitespaceValues_ReturnsFalse()
    {
        // Arrange
        const string testKey = "TRY_WHITESPACE_TEST_VAR";
        Environment.SetEnvironmentVariable(testKey, "   "); // Whitespace only

        try
        {
            var variableKeyOptions = new[]
            {
                new[] { testKey }
            };

            // Act
            var success = EnvironmentUtilities.TryGetRequiredEnvironmentVariables(variableKeyOptions, out var result);

            // Assert
            Assert.False(success);
            Assert.Null(result);
        }
        finally
        {
            Environment.SetEnvironmentVariable(testKey, null);
        }
    }

    [Fact]
    public void GetRequiredEnvironmentVariables_WithSpecialCharacters_HandlesCorrectly()
    {
        // Arrange
        const string testKey = "SPECIAL_CHARS_TEST";
        const string specialValue = "Value with spaces, symbols: !@#$%^&*()_+-={}[]|\\:;\"'<>?,./ and unicode: 世界 🌍";

        Environment.SetEnvironmentVariable(testKey, specialValue);

        try
        {
            var variableKeyOptions = new[]
            {
                new[] { testKey }
            };

            // Act
            var result = EnvironmentUtilities.GetRequiredEnvironmentVariables(variableKeyOptions);

            // Assert
            Assert.Single(result);
            Assert.Equal(specialValue, result[testKey]);
        }
        finally
        {
            Environment.SetEnvironmentVariable(testKey, null);
        }
    }

    [Fact]
    public void EnvironmentUtilities_IntegrationTest_RealWorldScenario()
    {
        // Arrange - Simulate a real-world scenario with database connection fallbacks
        const string dbHost = "TEST_DB_HOST";
        const string dbHostAlt = "TEST_DATABASE_HOST";
        const string dbPort = "TEST_DB_PORT";
        const string dbUser = "TEST_DB_USER";
        const string dbUserAlt = "TEST_DATABASE_USER";

        Environment.SetEnvironmentVariable(dbHost, null);           // Primary missing
        Environment.SetEnvironmentVariable(dbHostAlt, "localhost"); // Fallback available
        Environment.SetEnvironmentVariable(dbPort, "5432");         // Single option
        Environment.SetEnvironmentVariable(dbUser, "testuser");     // Primary available
        Environment.SetEnvironmentVariable(dbUserAlt, "altuser");   // Should not be used

        try
        {
            var variableKeyOptions = new[]
            {
                new[] { dbHost, dbHostAlt },     // Use fallback
                new[] { dbPort },                // Single option
                new[] { dbUser, dbUserAlt }      // Use primary
            };

            // Act
            var result = EnvironmentUtilities.GetRequiredEnvironmentVariables(variableKeyOptions);

            // Assert
            Assert.Equal(3, result.Count);
            Assert.Equal("localhost", result[dbHostAlt]);
            Assert.Equal("5432", result[dbPort]);
            Assert.Equal("testuser", result[dbUser]);
            Assert.False(result.ContainsKey(dbHost));
            Assert.False(result.ContainsKey(dbUserAlt));
        }
        finally
        {
            Environment.SetEnvironmentVariable(dbHostAlt, null);
            Environment.SetEnvironmentVariable(dbPort, null);
            Environment.SetEnvironmentVariable(dbUser, null);
            Environment.SetEnvironmentVariable(dbUserAlt, null);
        }
    }

    [Fact]
    public void EnvironmentUtilities_BothMethods_ConsistentBehavior()
    {
        // Arrange
        const string testKey1 = "CONSISTENCY_TEST_1";
        const string testKey2 = "CONSISTENCY_TEST_2";
        const string testValue1 = "ConsistentValue1";
        const string testValue2 = "ConsistentValue2";

        Environment.SetEnvironmentVariable(testKey1, testValue1);
        Environment.SetEnvironmentVariable(testKey2, testValue2);

        try
        {
            var variableKeyOptions = new[]
            {
                new[] { testKey1 },
                new[] { testKey2 }
            };

            // Act
            var directResult = EnvironmentUtilities.GetRequiredEnvironmentVariables(variableKeyOptions);
            var trySuccess = EnvironmentUtilities.TryGetRequiredEnvironmentVariables(variableKeyOptions, out var tryResult);

            // Assert
            Assert.True(trySuccess);
            Assert.NotNull(tryResult);
            Assert.Equal(directResult.Count, tryResult.Count);

            foreach (var kvp in directResult)
            {
                Assert.True(tryResult.ContainsKey(kvp.Key));
                Assert.Equal(kvp.Value, tryResult[kvp.Key]);
            }
        }
        finally
        {
            Environment.SetEnvironmentVariable(testKey1, null);
            Environment.SetEnvironmentVariable(testKey2, null);
        }
    }

    [Fact]
    public void GetRequiredEnvironmentVariables_WithDuplicateKeysInSameGroup_UsesFirst()
    {
        // Arrange
        const string testKey = "DUPLICATE_KEY_TEST";
        const string testValue = "DuplicateTestValue";

        Environment.SetEnvironmentVariable(testKey, testValue);

        try
        {
            var variableKeyOptions = new[]
            {
                new[] { testKey, testKey, testKey } // Same key multiple times
            };

            // Act
            var result = EnvironmentUtilities.GetRequiredEnvironmentVariables(variableKeyOptions);

            // Assert
            Assert.Single(result);
            Assert.Equal(testValue, result[testKey]);
        }
        finally
        {
            Environment.SetEnvironmentVariable(testKey, null);
        }
    }
}
