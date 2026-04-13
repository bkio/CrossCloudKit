// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Interfaces.Enums;

/// <summary>
/// After performing an operation that causes a change in an item, defines what service shall return.
/// </summary>
/// <remarks>
/// Used by <see cref="IDatabaseService.PutItemAsync"/>, <see cref="IDatabaseService.UpdateItemAsync"/>,
/// and <see cref="IDatabaseService.DeleteItemAsync"/> to control whether the old or new item data is returned.
/// </remarks>
public enum DbReturnItemBehavior
{
    /// <summary>Do not return any item data (default). The operation result's Data will be null.</summary>
    DoNotReturn,
    /// <summary>Return the item as it was before the operation.</summary>
    ReturnOldValues,
    /// <summary>Return the item as it is after the operation.</summary>
    ReturnNewValues
}
