// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces.Enums;
using CrossCloudKit.Utilities.Common;

namespace CrossCloudKit.Interfaces.Classes;

public abstract class DbAttributeCondition(DbAttributeConditionType conditionType, string attributeName)
{
    public DbAttributeConditionType ConditionType { get; } = conditionType;
    public string AttributeName { get; } = attributeName ?? throw new ArgumentNullException(nameof(attributeName));
}

public class DbExistenceCondition : DbAttributeCondition
{
    public DbExistenceCondition(DbAttributeConditionType conditionType, string attributeName)
        : base(conditionType, attributeName)
    {
        if (conditionType != DbAttributeConditionType.AttributeExists &&
            conditionType != DbAttributeConditionType.AttributeNotExists)
        {
            throw new ArgumentException("Invalid condition type for existence condition", nameof(conditionType));
        }
    }
}

public class DbValueCondition(DbAttributeConditionType conditionType, string attributeName, PrimitiveType value) : DbAttributeCondition(conditionType, attributeName)
{
    public PrimitiveType Value { get; } = value ?? throw new ArgumentNullException(nameof(value));
}

public class DbArrayElementCondition : DbAttributeCondition
{
    public PrimitiveType ElementValue { get; }

    public DbArrayElementCondition(DbAttributeConditionType conditionType, string attributeName, PrimitiveType elementValue)
        : base(conditionType, attributeName)
    {
        if (conditionType != DbAttributeConditionType.ArrayElementExists &&
            conditionType != DbAttributeConditionType.ArrayElementNotExists)
        {
            throw new ArgumentException("Invalid condition type for array element condition", nameof(conditionType));
        }
        ElementValue = elementValue ?? throw new ArgumentNullException(nameof(elementValue));
    }
}

