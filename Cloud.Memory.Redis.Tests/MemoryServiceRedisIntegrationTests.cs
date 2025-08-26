// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Cloud.Interfaces;
using Cloud.Memory.Tests.Common;
using Cloud.PubSub.Redis;
using Xunit.Abstractions;

namespace Cloud.Memory.Redis.Tests;

public class MemoryServiceRedisIntegrationTests(ITestOutputHelper testOutputHelper) : MemoryServiceTestBase(testOutputHelper)
{
    private record struct RedisCredentials(string Host, int Port, string User, string Password);
    private RedisCredentials GetRedisCredentialsFromEnvironment()
    {
        var redisHost = Environment.GetEnvironmentVariable("REDIS_HOST");
        var redisPortStr = Environment.GetEnvironmentVariable("REDIS_PORT");
        var redisUser = Environment.GetEnvironmentVariable("REDIS_USER");
        var redisPassword = Environment.GetEnvironmentVariable("REDIS_PASSWORD");
        if (string.IsNullOrEmpty(redisHost)
            || string.IsNullOrEmpty(redisPortStr) || !int.TryParse(redisPortStr, out var redisPort)
            || string.IsNullOrEmpty(redisUser)
            || string.IsNullOrEmpty(redisPassword))
        {
            throw new ArgumentException("Redis credentials are not set in environment variables.");
        }
        return new RedisCredentials(redisHost, redisPort, redisUser, redisPassword);
    }

    protected override IMemoryService CreateMemoryService()
    {
        var (redisHost, redisPort, redisUser, redisPassword) = GetRedisCredentialsFromEnvironment();
        return new MemoryServiceRedis(redisHost, redisPort, redisUser, redisPassword, true, true, CreatePubSubService(), testOutputHelper.WriteLine);
    }
    protected override IPubSubService CreatePubSubService()
    {
        var (redisHost, redisPort, redisUser, redisPassword) = GetRedisCredentialsFromEnvironment();
        return new PubSubServiceRedis(redisHost, redisPort, redisUser, redisPassword, true, testOutputHelper.WriteLine);
    }
}
