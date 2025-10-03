// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces.Enums;

namespace CrossCloudKit.Interfaces.Classes;

public abstract class DbConditionCouplingBase(DbCondition first, DbCondition? second, DbConditionCouplingType type);
public class DbConditionNoCoupling(DbCondition condition) : DbConditionCouplingBase(condition, null, DbConditionCouplingType.None);
public class DbConditionCoupledWithAnd(DbCondition first, DbCondition second) : DbConditionCouplingBase(first, second, DbConditionCouplingType.And);
public class DbConditionCoupledWithOr(DbCondition first, DbCondition second) : DbConditionCouplingBase(first, second, DbConditionCouplingType.Or);

public static class DbConditionCouplingUtilities
{
    public static DbConditionCouplingBase And(this DbCondition first, DbCondition second)
    {
        return new DbConditionCoupledWithAnd(first, second);
    }
    public static DbConditionCouplingBase Or(this DbCondition first, DbCondition second)
    {
        return new DbConditionCoupledWithOr(first, second);
    }
}
