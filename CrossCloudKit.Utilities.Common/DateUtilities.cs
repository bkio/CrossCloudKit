// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Globalization;

namespace CrossCloudKit.Utilities.Common;

/// <summary>
/// Utility class for date and time operations.
/// </summary>
public class DateUtilities
{
    /// <summary>
    /// Gets the first date (Monday) of a specific ISO 8601 week in a given year.
    /// </summary>
    /// <param name="year">The year for which to calculate the week start date.</param>
    /// <param name="weekOfYear">The ISO 8601 week number (1-53).</param>
    /// <returns>A DateTime representing the Monday of the specified ISO 8601 week.</returns>
    /// <remarks>
    /// This method follows the ISO 8601 standard where weeks start on Monday and
    /// the first week of the year contains at least 4 days of the new year.
    /// </remarks>
    public static DateTime FirstDateOfWeekIso8601(int year, int weekOfYear)
    {
        var jan1 = new DateTime(year, 1, 1);

        var firstThursday = jan1.AddDays(DayOfWeek.Thursday - jan1.DayOfWeek);
        var firstWeek = CultureInfo.InvariantCulture.Calendar.GetWeekOfYear(firstThursday, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);

        var weekNum = weekOfYear;
        if (firstWeek == 1)
        {
            weekNum -= 1;
        }

        var result = firstThursday.AddDays(weekNum * 7);
        return result.AddDays(-3);
    }

    /// <summary>
    /// A dictionary mapping month numbers (1-12) to their English names.
    /// </summary>
    /// <value>
    /// Dictionary where keys are integers from 1 to 12 representing months,
    /// and values are the corresponding English month names.
    /// </value>
    public static readonly Dictionary<int, string> MonthNames = new()
    {
        [1] = "January",
        [2] = "February",
        [3] = "March",
        [4] = "April",
        [5] = "May",
        [6] = "June",
        [7] = "July",
        [8] = "August",
        [9] = "September",
        [10] = "October",
        [11] = "November",
        [12] = "December"
    };
}
