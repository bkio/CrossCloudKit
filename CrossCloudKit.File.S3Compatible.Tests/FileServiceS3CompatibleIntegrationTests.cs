// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.File.Tests.Common;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Memory.Redis;
using CrossCloudKit.Memory.Redis.Common;
using CrossCloudKit.PubSub.Redis;
using Xunit.Abstractions;

namespace CrossCloudKit.File.S3Compatible.Tests;

public class FileServiceS3CompatibleIntegrationTests(ITestOutputHelper testOutputHelper) : FileServiceTestBase(testOutputHelper)
{
    private static string GetS3ServerAddress()
    {
        return Environment.GetEnvironmentVariable("S3_SERVER_ADDRESS") ?? "";
    }

    private static string GetS3AccessKey()
    {
        return Environment.GetEnvironmentVariable("S3_ACCESS_KEY") ?? "";
    }

    private static string GetS3SecretKey()
    {
        return Environment.GetEnvironmentVariable("S3_SECRET_KEY") ?? "";
    }

    private static string GetS3Region()
    {
        return Environment.GetEnvironmentVariable("S3_REGION") ?? "us-east-1";
    }

    protected override IFileService CreateFileService()
    {
        var serverAddress = GetS3ServerAddress();
        var accessKey = GetS3AccessKey();
        var secretKey = GetS3SecretKey();
        var region = GetS3Region();
        return new FileServiceS3Compatible(serverAddress, accessKey, secretKey, region, CreateMemoryService(), CreatePubSubService());
    }

    protected override IPubSubService CreatePubSubService()
    {
        return new PubSubServiceRedis(RedisCommonFunctionalities.GetRedisConnectionOptionsFromEnvironmentForTesting());
    }

    private IMemoryService CreateMemoryService()
    {
        return new MemoryServiceRedis(RedisCommonFunctionalities.GetRedisConnectionOptionsFromEnvironmentForTesting());
    }

    protected override string GetTestBucketName()
    {
        return "cross-cloud-kit-tests";
    }
}
