// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Interfaces.Enums;

/// <summary>
/// Controls whether arrays returned by database operations are automatically sorted.
/// </summary>
/// <remarks>Set via <see cref="Records.DbOptions"/> and <see cref="IDatabaseService.SetOptions"/>.</remarks>
public enum DbAutoSortArrays
{
    /// <summary>Arrays are returned in their original order.</summary>
    No,
    /// <summary>Arrays are sorted before being returned.</summary>
    Yes
}
