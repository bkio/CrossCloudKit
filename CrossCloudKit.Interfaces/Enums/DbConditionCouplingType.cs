// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Interfaces.Enums;

/// <summary>
/// Represents how multiple conditions are logically combined in a <see cref="Classes.ConditionCoupling"/>.
/// </summary>
/// <remarks>
/// You do not set this directly. Use the <c>.And()</c> and <c>.Or()</c> extension methods
/// from <see cref="Classes.ConditionCouplingUtilities"/> to compose conditions.
/// </remarks>
public enum ConditionCouplingType
{
    /// <summary>No condition (always true). Represents an empty condition coupling.</summary>
    Empty,
    /// <summary>A single condition with no logical combination.</summary>
    Single,
    /// <summary>Both conditions must be true (logical AND).</summary>
    And,
    /// <summary>At least one condition must be true (logical OR).</summary>
    Or
}
