// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Interfaces.Enums;

/// <summary>
/// Controls whether floating-point numbers that have no fractional part (e.g. 5.0) are automatically
/// converted to integers when returned by database operations.
/// </summary>
/// <remarks>Set via <see cref="Records.DbOptions"/> and <see cref="IDatabaseService.SetOptions"/>.</remarks>
public enum DbAutoConvertRoundableFloatToInt
{
    /// <summary>Floating-point values are preserved as-is.</summary>
    No,
    /// <summary>Values like 5.0 are converted to 5 (long) in returned data.</summary>
    Yes
}
