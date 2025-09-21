// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Interfaces.Enums;

/// <summary>
/// After performing an operation that causes a change in an item, defines what service shall return
/// </summary>
public enum DbReturnItemBehavior
{
    DoNotReturn,
    ReturnOldValues,
    ReturnNewValues
}
