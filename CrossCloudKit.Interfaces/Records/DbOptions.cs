// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces.Enums;

namespace CrossCloudKit.Interfaces.Records;

public sealed record DbOptions(
    DbAutoSortArrays AutoSortArrays = DbAutoSortArrays.No,
    DbAutoConvertRoundableFloatToInt AutoConvertRoundableFloatToInt = DbAutoConvertRoundableFloatToInt.No
);
