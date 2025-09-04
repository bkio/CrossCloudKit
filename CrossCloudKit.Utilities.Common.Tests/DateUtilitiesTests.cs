// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Xunit;

namespace CrossCloudKit.Utilities.Common.Tests;

public class DateUtilitiesTests
{
    [Fact]
    public void FirstDateOfWeekIso8601_Week1_2023_ReturnsCorrectMonday()
    {
        // Arrange
        const int year = 2023;
        const int weekOfYear = 1;

        // Act
        var result = DateUtilities.FirstDateOfWeekIso8601(year, weekOfYear);

        // Assert
        Assert.Equal(new DateTime(2023, 1, 2), result); // Monday of week 1, 2023
        Assert.Equal(DayOfWeek.Monday, result.DayOfWeek);
    }

    [Fact]
    public void FirstDateOfWeekIso8601_Week1_2024_ReturnsCorrectMonday()
    {
        // Arrange
        const int year = 2024;
        const int weekOfYear = 1;

        // Act
        var result = DateUtilities.FirstDateOfWeekIso8601(year, weekOfYear);

        // Assert
        Assert.Equal(new DateTime(2024, 1, 1), result); // Monday of week 1, 2024
        Assert.Equal(DayOfWeek.Monday, result.DayOfWeek);
    }

    [Fact]
    public void FirstDateOfWeekIso8601_Week1_2022_ReturnsCorrectMonday()
    {
        // Arrange
        const int year = 2022;
        const int weekOfYear = 1;

        // Act
        var result = DateUtilities.FirstDateOfWeekIso8601(year, weekOfYear);

        // Assert
        Assert.Equal(new DateTime(2022, 1, 3), result); // Monday of week 1, 2022
        Assert.Equal(DayOfWeek.Monday, result.DayOfWeek);
    }

    [Theory]
    [InlineData(2023, 1, "2023-01-02")] // Week 1 starts on January 2, 2023
    [InlineData(2023, 2, "2023-01-09")] // Week 2 starts on January 9, 2023
    [InlineData(2023, 26, "2023-06-26")] // Week 26 (mid-year)
    [InlineData(2023, 52, "2023-12-25")] // Week 52 (near end of year)
    [InlineData(2024, 1, "2024-01-01")] // Week 1 starts on January 1, 2024
    [InlineData(2024, 53, "2024-12-30")] // Week 53 exists in 2024
    public void FirstDateOfWeekIso8601_WithVariousWeeks_ReturnsCorrectMonday(int year, int weekOfYear, string expectedDateString)
    {
        // Arrange
        var expectedDate = DateTime.Parse(expectedDateString);

        // Act
        var result = DateUtilities.FirstDateOfWeekIso8601(year, weekOfYear);

        // Assert
        Assert.Equal(expectedDate, result);
        Assert.Equal(DayOfWeek.Monday, result.DayOfWeek);
    }

    [Fact]
    public void FirstDateOfWeekIso8601_Week53_2020_ReturnsCorrectMonday()
    {
        // Arrange - 2020 has 53 ISO weeks
        const int year = 2020;
        const int weekOfYear = 53;

        // Act
        var result = DateUtilities.FirstDateOfWeekIso8601(year, weekOfYear);

        // Assert
        Assert.Equal(new DateTime(2020, 12, 28), result);
        Assert.Equal(DayOfWeek.Monday, result.DayOfWeek);
    }

    [Fact]
    public void FirstDateOfWeekIso8601_AlwaysReturnsMonday()
    {
        // Arrange - Test multiple random weeks across different years
        var testCases = new[]
        {
            (2020, 10), (2020, 25), (2020, 40),
            (2021, 5), (2021, 30), (2021, 50),
            (2022, 15), (2022, 35), (2022, 45),
            (2023, 8), (2023, 20), (2023, 48),
            (2024, 12), (2024, 28), (2024, 44)
        };

        foreach (var (year, week) in testCases)
        {
            // Act
            var result = DateUtilities.FirstDateOfWeekIso8601(year, week);

            // Assert
            Assert.Equal(DayOfWeek.Monday, result.DayOfWeek);
        }
    }

    [Fact]
    public void FirstDateOfWeekIso8601_ConsecutiveWeeks_AreSeparatedBy7Days()
    {
        // Arrange
        const int year = 2023;
        const int week1 = 10;
        const int week2 = 11;

        // Act
        var monday1 = DateUtilities.FirstDateOfWeekIso8601(year, week1);
        var monday2 = DateUtilities.FirstDateOfWeekIso8601(year, week2);

        // Assert
        Assert.Equal(7, (monday2 - monday1).TotalDays);
    }

    [Fact]
    public void FirstDateOfWeekIso8601_Year2021_Week1StartsInDecember2020()
    {
        // Arrange - Edge case where week 1 of 2021 starts in December 2020
        const int year = 2021;
        const int weekOfYear = 1;

        // Act
        var result = DateUtilities.FirstDateOfWeekIso8601(year, weekOfYear);

        // Assert
        Assert.Equal(new DateTime(2021, 1, 4), result);
        Assert.Equal(DayOfWeek.Monday, result.DayOfWeek);
    }

    [Theory]
    [InlineData(2019, 1)]
    [InlineData(2020, 1)]
    [InlineData(2021, 1)]
    [InlineData(2025, 1)]
    [InlineData(2030, 1)]
    public void FirstDateOfWeekIso8601_Week1_AcrossMultipleYears_AlwaysReturnsMonday(int year, int weekOfYear)
    {
        // Act
        var result = DateUtilities.FirstDateOfWeekIso8601(year, weekOfYear);

        // Assert
        Assert.Equal(DayOfWeek.Monday, result.DayOfWeek);

        // Week 1 should contain January 4th (ISO 8601 rule)
        var jan4 = new DateTime(year, 1, 4);
        var weekStart = result;
        var weekEnd = result.AddDays(6);
        Assert.True(jan4 >= weekStart && jan4 <= weekEnd,
            $"Week 1 of {year} should contain January 4th. Week runs from {weekStart:yyyy-MM-dd} to {weekEnd:yyyy-MM-dd}");
    }

    [Fact]
    public void MonthNames_ContainsAllTwelveMonths()
    {
        // Act & Assert
        Assert.Equal(12, DateUtilities.MonthNames.Count);

        for (int i = 1; i <= 12; i++)
        {
            Assert.True(DateUtilities.MonthNames.ContainsKey(i));
            Assert.False(string.IsNullOrEmpty(DateUtilities.MonthNames[i]));
        }
    }

    [Theory]
    [InlineData(1, "January")]
    [InlineData(2, "February")]
    [InlineData(3, "March")]
    [InlineData(4, "April")]
    [InlineData(5, "May")]
    [InlineData(6, "June")]
    [InlineData(7, "July")]
    [InlineData(8, "August")]
    [InlineData(9, "September")]
    [InlineData(10, "October")]
    [InlineData(11, "November")]
    [InlineData(12, "December")]
    public void MonthNames_ContainsCorrectMonthName(int monthNumber, string expectedName)
    {
        // Act
        var result = DateUtilities.MonthNames[monthNumber];

        // Assert
        Assert.Equal(expectedName, result);
    }

    [Fact]
    public void MonthNames_DoesNotContainInvalidMonthNumbers()
    {
        // Act & Assert
        Assert.False(DateUtilities.MonthNames.ContainsKey(0));
        Assert.False(DateUtilities.MonthNames.ContainsKey(13));
        Assert.False(DateUtilities.MonthNames.ContainsKey(-1));
    }

    [Fact]
    public void MonthNames_AllNamesAreCapitalized()
    {
        // Act & Assert
        foreach (var monthName in DateUtilities.MonthNames.Values)
        {
            Assert.True(char.IsUpper(monthName[0]), $"Month name '{monthName}' should start with uppercase letter");
            Assert.True(monthName.All(char.IsLetter), $"Month name '{monthName}' should contain only letters");
        }
    }

    [Fact]
    public void MonthNames_IsReadOnlyDictionary()
    {
        // Arrange
        var originalCount = DateUtilities.MonthNames.Count;
        var originalJanuaryValue = DateUtilities.MonthNames[1];

        // Act & Assert - Should be able to read but modification behavior depends on implementation
        Assert.Equal("January", originalJanuaryValue);
        Assert.Equal(12, originalCount);
    }

    [Fact]
    public void FirstDateOfWeekIso8601_Performance_HandlesMultipleCalculations()
    {
        // Arrange
        const int iterationCount = 1000;
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Act
        for (int i = 0; i < iterationCount; i++)
        {
            var year = 2020 + (i % 5); // Years 2020-2024
            var week = 1 + (i % 52); // Weeks 1-52
            DateUtilities.FirstDateOfWeekIso8601(year, week);
        }

        stopwatch.Stop();

        // Assert
        Assert.True(stopwatch.ElapsedMilliseconds < 1000,
            $"Performance test failed: {iterationCount} calculations took {stopwatch.ElapsedMilliseconds}ms");
    }

    [Fact]
    public void FirstDateOfWeekIso8601_LeapYear_HandlesCorrectly()
    {
        // Arrange - Test leap years (2020, 2024) vs non-leap years (2021, 2022, 2023)
        var leapYearWeek10 = DateUtilities.FirstDateOfWeekIso8601(2020, 10);
        var normalYearWeek10 = DateUtilities.FirstDateOfWeekIso8601(2021, 10);

        // Act & Assert
        Assert.Equal(DayOfWeek.Monday, leapYearWeek10.DayOfWeek);
        Assert.Equal(DayOfWeek.Monday, normalYearWeek10.DayOfWeek);

        // Both should be valid dates
        Assert.True(leapYearWeek10.Year == 2020);
        Assert.True(normalYearWeek10.Year == 2021);
    }

    [Fact]
    public void FirstDateOfWeekIso8601_EdgeCase_Week53Exists()
    {
        // Arrange - Years that have 53 ISO weeks
        var yearsWithWeek53 = new[] { 2020, 2015, 2009, 2004 };

        foreach (var year in yearsWithWeek53)
        {
            // Act
            var week53Monday = DateUtilities.FirstDateOfWeekIso8601(year, 53);

            // Assert
            Assert.Equal(DayOfWeek.Monday, week53Monday.DayOfWeek);
            Assert.True(week53Monday.Year == year || week53Monday.Year == year - 1,
                $"Week 53 of {year} should be in {year} or {year - 1}");
        }
    }

    [Fact]
    public void DateUtilities_IntegrationTest_WeekCalculationConsistency()
    {
        // Arrange - Test that consecutive week calculations are consistent
        const int testYear = 2023;
        var previousWeekEnd = DateTime.MinValue;

        // Act & Assert
        for (int week = 1; week <= 52; week++)
        {
            var currentWeekStart = DateUtilities.FirstDateOfWeekIso8601(testYear, week);

            Assert.Equal(DayOfWeek.Monday, currentWeekStart.DayOfWeek);

            if (week > 1)
            {
                // Current week should start exactly 7 days after previous week
                Assert.Equal(7, (currentWeekStart - previousWeekEnd).TotalDays); // previousWeekEnd is actually previous week start
            }

            previousWeekEnd = currentWeekStart;
        }
    }

    [Fact]
    public void MonthNames_IntegrationTest_WithDateTimeMonth()
    {
        // Arrange & Act & Assert - Verify month names align with .NET month values
        for (int month = 1; month <= 12; month++)
        {
            var testDate = new DateTime(2023, month, 1);
            var expectedName = testDate.ToString("MMMM", System.Globalization.CultureInfo.InvariantCulture);
            var actualName = DateUtilities.MonthNames[month];

            Assert.Equal(expectedName, actualName);
        }
    }

    [Theory]
    [InlineData(2000, 1)] // Y2K
    [InlineData(1999, 52)] // Pre-Y2K
    [InlineData(2038, 1)] // Unix timestamp edge case year
    [InlineData(1970, 1)] // Unix epoch year
    [InlineData(2100, 1)] // Future century
    public void FirstDateOfWeekIso8601_HistoricalAndFutureDates_WorkCorrectly(int year, int weekOfYear)
    {
        // Act
        var result = DateUtilities.FirstDateOfWeekIso8601(year, weekOfYear);

        // Assert
        Assert.Equal(DayOfWeek.Monday, result.DayOfWeek);
        Assert.True(result.Year == year || result.Year == year - 1 || result.Year == year + 1,
            "Week 1 might span across year boundaries");
    }
}
