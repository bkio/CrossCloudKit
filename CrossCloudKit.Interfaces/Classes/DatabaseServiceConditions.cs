// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces.Enums;
using CrossCloudKit.Utilities.Common;

namespace CrossCloudKit.Interfaces.Classes;

/// <summary>
/// Abstract base class for all database/vector query conditions.
/// </summary>
/// <remarks>
/// <para>
/// <b>Do NOT construct conditions directly.</b> Use the factory methods on <see cref="IDatabaseService"/>
/// (e.g. <c>dbService.AttributeEquals("Status", new Primitive("active"))</c>) or <see cref="IVectorService"/>
/// (e.g. <c>vectorService.FieldEquals("category", new Primitive("shoes"))</c>).
/// </para>
/// <para>
/// Compose multiple conditions with <see cref="ConditionCouplingUtilities.And(ConditionCoupling, ConditionCoupling)"/>
/// and <see cref="ConditionCouplingUtilities.Or(ConditionCoupling, ConditionCoupling)"/>:
/// </para>
/// <code>
/// // Correct:
/// var cond = dbService.AttributeEquals("Status", new Primitive("active"))
///     .And(dbService.AttributeIsGreaterThan("Age", new Primitive(18L)));
///
/// // WRONG — do NOT chain builder methods:
/// // dbService.AttributeEquals(...).AttributeEquals(...)  // WILL NOT COMPILE
/// </code>
/// </remarks>
public abstract class Condition(ConditionType conditionType, string attributeName)
{
    public ConditionType ConditionType { get; } = conditionType;
    public string AttributeName { get; } = attributeName ?? throw new ArgumentNullException(nameof(attributeName));

    public static implicit operator SingleCondition(Condition c)
    {
        return new SingleCondition(c);
    }
}

/// <summary>
/// A condition that checks for the existence or non-existence of an attribute.
/// </summary>
/// <remarks>
/// Created via <see cref="IDatabaseService.AttributeExists"/> or <see cref="IDatabaseService.AttributeNotExists"/>.
/// Do not construct directly.
/// </remarks>
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

/// <summary>
/// A condition that compares an attribute against a <see cref="Primitive"/> value.
/// </summary>
/// <remarks>
/// Created via <see cref="IDatabaseService.AttributeEquals"/>, <see cref="IDatabaseService.AttributeNotEquals"/>,
/// <see cref="IDatabaseService.AttributeIsGreaterThan"/>, <see cref="IDatabaseService.AttributeIsGreaterOrEqual"/>,
/// <see cref="IDatabaseService.AttributeIsLessThan"/>, or <see cref="IDatabaseService.AttributeIsLessOrEqual"/>.
/// Do not construct directly.
/// </remarks>
public class ValueCondition(ConditionType conditionType, string attributeName, Primitive value) : Condition(conditionType, attributeName)
{
    public Primitive Value { get; } = value ?? throw new ArgumentNullException(nameof(value));
}

/// <summary>
/// A condition that checks whether an array attribute contains or does not contain a specific element.
/// </summary>
/// <remarks>
/// <para>
/// Created via <see cref="IDatabaseService.ArrayElementExists"/> or <see cref="IDatabaseService.ArrayElementNotExists"/>.
/// Do not construct directly.
/// </para>
/// <para>
/// <b>Important:</b> Array index syntax (e.g. <c>"Tags[0]"</c>) is NOT supported and will throw
/// <see cref="ArgumentException"/>. Use <c>ArrayElementExists</c> / <c>ArrayElementNotExists</c> instead.
/// </para>
/// </remarks>
public class ArrayCondition : Condition
{
    public Primitive ElementValue { get; }

    public ArrayCondition(ConditionType conditionType, string attributeName, Primitive elementValue)
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
