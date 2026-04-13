// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces.Enums;

namespace CrossCloudKit.Interfaces.Classes;

/// <summary>
/// Represents a tree of logically combined conditions used in database and vector queries.
/// </summary>
/// <remarks>
/// <para>
/// A <c>ConditionCoupling</c> is produced by calling <c>.And()</c> or <c>.Or()</c> on conditions
/// returned by <see cref="IDatabaseService"/> or <see cref="IVectorService"/> factory methods.
/// You never need to construct this class directly.
/// </para>
/// <para>
/// An empty <c>ConditionCoupling</c> (no conditions) is equivalent to "always true" and acts
/// as the identity element for <c>.And()</c> and <c>.Or()</c> aggregation.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// // Build conditions using service factory methods + .And() / .Or():
/// var condition = dbService.AttributeEquals("Status", new Primitive("active"))
///     .And(dbService.AttributeIsGreaterThan("Age", new Primitive(18L)));
///
/// var result = await dbService.ScanTableWithFilterAsync("Users", condition);
/// </code>
/// </example>
/// <seealso cref="ConditionCouplingUtilities"/>
public class ConditionCoupling
{
    public ConditionCoupling()
    {
        First = null;
        Second = null;
        SingleCondition = null;
        CouplingType = ConditionCouplingType.Empty;
    }

    internal ConditionCoupling(Condition first)
    {
        First = null;
        Second = null;
        SingleCondition = first;
        CouplingType = ConditionCouplingType.Single;
    }

    internal ConditionCoupling(ConditionCoupling first, ConditionCoupling? second, ConditionCouplingType type)
    {
        First = first;
        Second = second;
        SingleCondition = null;
        CouplingType = type;
    }

    public readonly ConditionCoupling? First;
    public readonly ConditionCoupling? Second;
    public readonly Condition? SingleCondition;
    public readonly ConditionCouplingType CouplingType;
}

/// <summary>An empty condition coupling that always evaluates to true.</summary>
public class EmptyCondition : ConditionCoupling;
/// <summary>A coupling wrapping a single <see cref="Condition"/>.</summary>
public class SingleCondition(Condition condition) : ConditionCoupling(condition);
/// <summary>A coupling requiring both child conditions to be true (logical AND).</summary>
public class ConditionCoupledWithAnd(ConditionCoupling first, ConditionCoupling second) : ConditionCoupling(first, second, ConditionCouplingType.And);
/// <summary>A coupling requiring at least one child condition to be true (logical OR).</summary>
public class ConditionCoupledWithOr(ConditionCoupling first, ConditionCoupling second) : ConditionCoupling(first, second, ConditionCouplingType.Or);

/// <summary>
/// Extension methods for composing <see cref="Condition"/> and <see cref="ConditionCoupling"/> instances
/// using logical AND / OR operators.
/// </summary>
/// <remarks>
/// <para><b>Usage pattern:</b></para>
/// <code>
/// // AND: all conditions must be true
/// var both = dbService.AttributeEquals("Role", new Primitive("admin"))
///     .And(dbService.AttributeEquals("Status", new Primitive("active")));
///
/// // OR: at least one condition must be true
/// var either = dbService.AttributeEquals("Role", new Primitive("admin"))
///     .Or(dbService.AttributeEquals("Role", new Primitive("moderator")));
///
/// // Complex nesting
/// var complex = dbService.AttributeEquals("Dept", new Primitive("IT"))
///     .And(
///         dbService.AttributeIsGreaterThan("Exp", new Primitive(5L))
///         .Or(dbService.ArrayElementExists("Certs", new Primitive("Senior")))
///     );
/// </code>
/// <para><b>Anti-patterns — do NOT do these:</b></para>
/// <list type="bullet">
/// <item>Do NOT chain condition builder methods: <c>dbService.AttributeEquals(...).AttributeEquals(...)</c></item>
/// <item>Do NOT use array indexing: <c>dbService.AttributeEquals("Tags[0]", ...)</c> — use <c>ArrayElementExists</c> instead</item>
/// </list>
/// </remarks>
public static class ConditionCouplingUtilities
{
    /// <summary>
    /// Combines two condition couplings with a logical AND.
    /// </summary>
    /// <param name="first">The left-hand condition.</param>
    /// <param name="second">The right-hand condition.</param>
    /// <returns>A new <see cref="ConditionCoupledWithAnd"/> requiring both conditions to be true.</returns>
    /// <example>
    /// <code>
    /// var condition = dbService.AttributeEquals("Status", new Primitive("active"))
    ///     .And(dbService.AttributeIsGreaterThan("Age", new Primitive(18L)));
    /// </code>
    /// </example>
    public static ConditionCoupling And(this ConditionCoupling first, ConditionCoupling second)
    {
        return new ConditionCoupledWithAnd(first, second);
    }
    /// <summary>
    /// Combines a single condition with a condition coupling using logical AND.
    /// </summary>
    /// <param name="first">The left-hand condition.</param>
    /// <param name="second">The right-hand condition coupling.</param>
    /// <returns>A new <see cref="ConditionCoupledWithAnd"/> requiring both conditions to be true.</returns>
    public static ConditionCoupling And(this Condition first, ConditionCoupling second)
    {
        return new ConditionCoupledWithAnd(first, second);
    }

    /// <summary>
    /// Combines two condition couplings with a logical OR.
    /// </summary>
    /// <param name="first">The left-hand condition.</param>
    /// <param name="second">The right-hand condition.</param>
    /// <returns>A new <see cref="ConditionCoupledWithOr"/> requiring at least one condition to be true.</returns>
    /// <example>
    /// <code>
    /// var condition = dbService.AttributeEquals("Role", new Primitive("admin"))
    ///     .Or(dbService.AttributeEquals("Role", new Primitive("moderator")));
    /// </code>
    /// </example>
    public static ConditionCoupling Or(this ConditionCoupling first, ConditionCoupling second)
    {
        return new ConditionCoupledWithOr(first, second);
    }
    /// <summary>
    /// Combines a single condition with a condition coupling using logical OR.
    /// </summary>
    /// <param name="first">The left-hand condition.</param>
    /// <param name="second">The right-hand condition coupling.</param>
    /// <returns>A new <see cref="ConditionCoupledWithOr"/> requiring at least one condition to be true.</returns>
    public static ConditionCoupling Or(this Condition first, ConditionCoupling second)
    {
        return new ConditionCoupledWithOr(first, second);
    }

    /// <summary>
    /// Combines all conditions with logical AND into a single coupling.
    /// </summary>
    /// <param name="conditions">The conditions to combine.</param>
    /// <returns>A coupling that is true only when all conditions are true. Returns an empty coupling if the sequence is empty.</returns>
    /// <example>
    /// <code>
    /// var conditions = new[] { "Name", "Email", "Phone" }
    ///     .Select(attr => dbService.AttributeExists(attr));
    /// var allExist = conditions.AggregateAnd();
    /// </code>
    /// </example>
    public static ConditionCoupling AggregateAnd(this IEnumerable<Condition> conditions)
    {
        return conditions.Aggregate(new ConditionCoupling(), (current, condition) => current.And(condition));
    }
    /// <summary>
    /// Combines all condition couplings with logical AND into a single coupling.
    /// </summary>
    /// <param name="conditions">The condition couplings to combine.</param>
    /// <returns>A coupling that is true only when all conditions are true.</returns>
    public static ConditionCoupling AggregateAnd(this IEnumerable<ConditionCoupling> conditions)
    {
        return conditions.Aggregate(new ConditionCoupling(), (current, condition) => current.And(condition));
    }

    /// <summary>
    /// Combines all conditions with logical OR into a single coupling.
    /// </summary>
    /// <param name="conditions">The conditions to combine.</param>
    /// <returns>A coupling that is true when at least one condition is true. Returns an empty coupling if the sequence is empty.</returns>
    /// <example>
    /// <code>
    /// var roles = new[] { "admin", "moderator", "editor" };
    /// var anyRole = roles
    ///     .Select(r => dbService.AttributeEquals("Role", new Primitive(r)))
    ///     .AggregateOr();
    /// </code>
    /// </example>
    public static ConditionCoupling AggregateOr(this IEnumerable<Condition> conditions)
    {
        ConditionCoupling? result = null;
        foreach (var condition in conditions)
        {
            var single = new SingleCondition(condition);
            result = result is null ? single : result.Or(single);
        }
        return result ?? new ConditionCoupling();
    }
    /// <summary>
    /// Combines all condition couplings with logical OR into a single coupling.
    /// </summary>
    /// <param name="conditions">The condition couplings to combine.</param>
    /// <returns>A coupling that is true when at least one condition is true.</returns>
    public static ConditionCoupling AggregateOr(this IEnumerable<ConditionCoupling> conditions)
    {
        ConditionCoupling? result = null;
        foreach (var coupling in conditions)
        {
            result = result is null ? coupling : result.Or(coupling);
        }
        return result ?? new ConditionCoupling();
    }
}
