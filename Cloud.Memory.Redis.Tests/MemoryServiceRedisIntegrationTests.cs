// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Cloud.Interfaces;
using Cloud.Memory.Redis.Common;
using Cloud.Memory.Tests.Common;
using Cloud.PubSub.Redis;
using Xunit.Abstractions;

namespace Cloud.Memory.Redis.Tests;

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
