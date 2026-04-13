// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces.Enums;

namespace CrossCloudKit.Interfaces.Records;

/// <summary>
/// Configuration options that control how a database service processes and returns data.
/// </summary>
/// <remarks>
/// Pass to <see cref="IDatabaseService.SetOptions"/> to change behaviour at runtime.
/// </remarks>
/// <example>
/// <code>
/// dbService.SetOptions(new DbOptions(
///     AutoSortArrays: DbAutoSortArrays.Yes,
///     AutoConvertRoundableFloatToInt: DbAutoConvertRoundableFloatToInt.Yes
/// ));
/// </code>
/// </example>
/// <param name="AutoSortArrays">When <see cref="DbAutoSortArrays.Yes"/>, arrays in returned items are sorted.</param>
/// <param name="AutoConvertRoundableFloatToInt">When <see cref="DbAutoConvertRoundableFloatToInt.Yes"/>, values like 5.0 are returned as integers.</param>
public sealed record DbOptions(
    DbAutoSortArrays AutoSortArrays = DbAutoSortArrays.No,
    DbAutoConvertRoundableFloatToInt AutoConvertRoundableFloatToInt = DbAutoConvertRoundableFloatToInt.No
);
