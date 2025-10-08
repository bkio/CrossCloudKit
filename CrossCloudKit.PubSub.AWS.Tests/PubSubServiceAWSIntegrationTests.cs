// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces;
using CrossCloudKit.PubSub.Tests.Common;
using Xunit;
using Xunit.Abstractions;

[assembly: CollectionBehavior(DisableTestParallelization = false, MaxParallelThreads = 8)]

namespace CrossCloudKit.PubSub.AWS.Tests;

public class PubSubServiceAWSIntegrationTests(ITestOutputHelper testOutputHelper) : PubSubServiceTestBase(testOutputHelper)
{
    private readonly ITestOutputHelper _testOutputHelper = testOutputHelper;

    private static string GetAWSAccessKey()
    {
        return Environment.GetEnvironmentVariable("AWS_ACCESS_KEY") ?? "";
    }

    private static string GetAWSSecretKey()
    {
        return Environment.GetEnvironmentVariable("AWS_SECRET_KEY") ?? "";
    }

    private static string GetAWSRegion()
    {
        return Environment.GetEnvironmentVariable("AWS_REGION") ?? "us-east-1";
    }

    protected override IPubSubService CreatePubSubService()
    {
        var accessKey = GetAWSAccessKey();
        var secretKey = GetAWSSecretKey();
        var region = GetAWSRegion();
        return new PubSubServiceAWS(accessKey, secretKey, region, _testOutputHelper.WriteLine);
    }
}
