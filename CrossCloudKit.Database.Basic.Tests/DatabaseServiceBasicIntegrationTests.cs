// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Database.Tests.Common;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Memory.Basic;
using Xunit;

[assembly: CollectionBehavior(DisableTestParallelization = false, MaxParallelThreads = 8)]

namespace CrossCloudKit.Database.Basic.Tests;

public class DatabaseServiceBasicIntegrationTests : DatabaseServiceTestBase
{
    protected override IDatabaseService CreateDatabaseService()
    {
        return new DatabaseServiceBasic(
            "cross-cloud-kit-tests-database",
            new MemoryServiceBasic(),
            Path.GetTempPath());
    }
}
