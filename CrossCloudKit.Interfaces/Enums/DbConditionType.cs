// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Interfaces.Enums;

/// <summary>
/// Identifies the type of condition used in database and vector service queries.
/// </summary>
/// <remarks>
/// You do not use this enum directly. Conditions are created via factory methods on
/// <see cref="IDatabaseService"/> (e.g. <c>dbService.AttributeEquals(...)</c>) or
/// <see cref="IVectorService"/> (e.g. <c>vectorService.FieldEquals(...)</c>).
/// </remarks>
public enum ConditionType
{
    /// <summary>Checks if an attribute equals a value.</summary>
    AttributeEquals,
    /// <summary>Checks if an attribute does not equal a value.</summary>
    AttributeNotEquals,
    /// <summary>Checks if an attribute is greater than a value.</summary>
    AttributeGreater,
    /// <summary>Checks if an attribute is greater than or equal to a value.</summary>
    AttributeGreaterOrEqual,
    /// <summary>Checks if an attribute is less than a value.</summary>
    AttributeLess,
    /// <summary>Checks if an attribute is less than or equal to a value.</summary>
    AttributeLessOrEqual,
    /// <summary>Checks if an attribute exists on the item.</summary>
    AttributeExists,
    /// <summary>Checks if an attribute does not exist on the item.</summary>
    AttributeNotExists,
    /// <summary>Checks if an array attribute contains a specific element.</summary>
    ArrayElementExists,
    /// <summary>Checks if an array attribute does not contain a specific element.</summary>
    ArrayElementNotExists
}
