// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces;
using CrossCloudKit.Memory.Redis.Common;
using CrossCloudKit.Memory.Tests.Common;
using CrossCloudKit.PubSub.Redis;
using Xunit;
using Xunit.Abstractions;

[assembly: CollectionBehavior(DisableTestParallelization = false, MaxParallelThreads = 8)]

namespace CrossCloudKit.Memory.Redis.Tests;

public class MemoryServiceRedisIntegrationTests(ITestOutputHelper testOutputHelper) : MemoryServiceTestBase(testOutputHelper)
{
    protected override IMemoryService CreateMemoryService()
    {
        return new MemoryServiceRedis(RedisCommonFunctionalities.GetRedisConnectionOptionsFromEnvironmentForTesting(), CreatePubSubService());
    }
    protected override IPubSubService CreatePubSubService()
    {
        return new PubSubServiceRedis(RedisCommonFunctionalities.GetRedisConnectionOptionsFromEnvironmentForTesting());
    }
}
