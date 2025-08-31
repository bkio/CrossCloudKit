// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces;
using CrossCloudKit.Memory.Redis.Common;
using CrossCloudKit.PubSub.Tests.Common;
using Xunit.Abstractions;

namespace CrossCloudKit.PubSub.Redis.Tests;

public class PubSubServiceRedisIntegrationTests(ITestOutputHelper testOutputHelper) : PubSubServiceTestBase(testOutputHelper)
{
    protected override IPubSubService CreatePubSubService()
    {
        return new PubSubServiceRedis(RedisCommonFunctionalities.GetRedisConnectionOptionsFromEnvironmentForTesting());
    }
}
