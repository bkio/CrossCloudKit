// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces.Enums;
using CrossCloudKit.Utilities.Common;

namespace CrossCloudKit.Interfaces.Classes;

public abstract class DbCondition(DbConditionType conditionType, string attributeName)
{
    public DbConditionType ConditionType { get; } = conditionType;
    public string AttributeName { get; } = attributeName ?? throw new ArgumentNullException(nameof(attributeName));

    public static implicit operator DbConditionNoCoupling(DbCondition c)
    {
        return new DbConditionNoCoupling(c);
    }
}

public class DbExistenceCondition : DbCondition
{
    public DbExistenceCondition(DbConditionType conditionType, string attributeName)
        : base(conditionType, attributeName)
    {
        if (conditionType != DbConditionType.AttributeExists &&
            conditionType != DbConditionType.AttributeNotExists)
        {
            throw new ArgumentException("Invalid condition type for existence condition", nameof(conditionType));
        }
    }
}

public class DbValueCondition(DbConditionType conditionType, string attributeName, PrimitiveType value) : DbCondition(conditionType, attributeName)
{
    public PrimitiveType Value { get; } = value ?? throw new ArgumentNullException(nameof(value));
}

public class DbArrayElementCondition : DbCondition
{
    public PrimitiveType ElementValue { get; }

    public DbArrayElementCondition(DbConditionType conditionType, string attributeName, PrimitiveType elementValue)
        : base(conditionType, attributeName)
    {
        if (conditionType != DbConditionType.ArrayElementExists &&
            conditionType != DbConditionType.ArrayElementNotExists)
        {
            throw new ArgumentException("Invalid condition type for array element condition", nameof(conditionType));
        }
        ElementValue = elementValue ?? throw new ArgumentNullException(nameof(elementValue));
    }
}
