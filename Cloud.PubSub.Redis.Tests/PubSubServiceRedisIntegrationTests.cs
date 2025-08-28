// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Cloud.Interfaces;
using Cloud.Memory.Redis.Common;
using Cloud.PubSub.Tests.Common;
using Xunit.Abstractions;

namespace Cloud.PubSub.Redis.Tests;

public class PubSubServiceRedisIntegrationTests(ITestOutputHelper testOutputHelper) : PubSubServiceTestBase(testOutputHelper)
{
    protected override IPubSubService CreatePubSubService()
    {
        return new PubSubServiceRedis(RedisCommonFunctionalities.GetRedisConnectionOptionsFromEnvironmentForTesting());
    }
}
