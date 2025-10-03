// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces.Enums;

namespace CrossCloudKit.Interfaces.Classes;

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

public class EmptyCondition : ConditionCoupling;
public class SingleCondition(Condition condition) : ConditionCoupling(condition);
public class ConditionCoupledWithAnd(ConditionCoupling first, ConditionCoupling second) : ConditionCoupling(first, second, ConditionCouplingType.And);
public class ConditionCoupledWithOr(ConditionCoupling first, ConditionCoupling second) : ConditionCoupling(first, second, ConditionCouplingType.Or);

public static class ConditionCouplingUtilities
{
    public static ConditionCoupling And(this ConditionCoupling first, ConditionCoupling second)
    {
        return new ConditionCoupledWithAnd(first, second);
    }
    public static ConditionCoupling And(this Condition first, ConditionCoupling second)
    {
        return new ConditionCoupledWithAnd(first, second);
    }

    public static ConditionCoupling Or(this ConditionCoupling first, ConditionCoupling second)
    {
        return new ConditionCoupledWithOr(first, second);
    }
    public static ConditionCoupling Or(this Condition first, ConditionCoupling second)
    {
        return new ConditionCoupledWithOr(first, second);
    }

    public static ConditionCoupling AggregateAnd(this IEnumerable<Condition> conditions)
    {
        return conditions.Aggregate(new ConditionCoupling(), (current, condition) => current.And(condition));
    }
    public static ConditionCoupling AggregateAnd(this IEnumerable<ConditionCoupling> conditions)
    {
        return conditions.Aggregate(new ConditionCoupling(), (current, condition) => current.And(condition));
    }

    public static ConditionCoupling AggregateOr(this IEnumerable<Condition> conditions)
    {
        return conditions.Aggregate(new ConditionCoupling(), (current, condition) => current.Or(condition));
    }
    public static ConditionCoupling AggregateOr(this IEnumerable<ConditionCoupling> conditions)
    {
        return conditions.Aggregate(new ConditionCoupling(), (current, condition) => current.Or(condition));
    }
}
