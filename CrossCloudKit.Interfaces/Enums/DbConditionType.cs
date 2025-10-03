// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Interfaces.Enums;

public enum ConditionType
{
    AttributeEquals,
    AttributeNotEquals,
    AttributeGreater,
    AttributeGreaterOrEqual,
    AttributeLess,
    AttributeLessOrEqual,
    AttributeExists,
    AttributeNotExists,
    ArrayElementExists,
    ArrayElementNotExists
}
