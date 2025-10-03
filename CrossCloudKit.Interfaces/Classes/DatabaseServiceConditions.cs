// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces.Enums;
using CrossCloudKit.Utilities.Common;

namespace CrossCloudKit.Interfaces.Classes;

public abstract class Condition(ConditionType conditionType, string attributeName)
{
    public ConditionType ConditionType { get; } = conditionType;
    public string AttributeName { get; } = attributeName ?? throw new ArgumentNullException(nameof(attributeName));

    public static implicit operator SingleCondition(Condition c)
    {
        return new SingleCondition(c);
    }
}

public class ExistenceCondition : Condition
{
    public ExistenceCondition(ConditionType conditionType, string attributeName)
        : base(conditionType, attributeName)
    {
        if (conditionType != ConditionType.AttributeExists &&
            conditionType != ConditionType.AttributeNotExists)
        {
            throw new ArgumentException("Invalid condition type for existence condition", nameof(conditionType));
        }
    }
}

public class ValueCondition(ConditionType conditionType, string attributeName, PrimitiveType value) : Condition(conditionType, attributeName)
{
    public PrimitiveType Value { get; } = value ?? throw new ArgumentNullException(nameof(value));
}

public class ArrayCondition : Condition
{
    public PrimitiveType ElementValue { get; }

    public ArrayCondition(ConditionType conditionType, string attributeName, PrimitiveType elementValue)
        : base(conditionType, attributeName)
    {
        if (conditionType != ConditionType.ArrayElementExists &&
            conditionType != ConditionType.ArrayElementNotExists)
        {
            throw new ArgumentException("Invalid condition type for array element condition", nameof(conditionType));
        }
        ElementValue = elementValue ?? throw new ArgumentNullException(nameof(elementValue));
    }
}
