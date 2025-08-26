using Cloud.File.Tests.Common;
using Cloud.Interfaces;
using Cloud.PubSub.AWS;
using Xunit.Abstractions;

namespace Cloud.File.AWS.Tests;

public class FileServiceAWSIntegrationTests(ITestOutputHelper testOutputHelper) : FileServiceTestBase(testOutputHelper)
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

    protected override IFileService CreateFileService()
    {
        var accessKey = GetAWSAccessKey();
        var secretKey = GetAWSSecretKey();
        var region = GetAWSRegion();
        return new FileServiceAWS(accessKey, secretKey, region);
    }

    protected override IPubSubService CreatePubSubService()
    {
        var accessKey = GetAWSAccessKey();
        var secretKey = GetAWSSecretKey();
        var region = GetAWSRegion();
        return new PubSubServiceAWS(accessKey, secretKey, region, _testOutputHelper.WriteLine);
    }

    protected override string GetTestBucketName()
    {
        return "cross-cloud-kit-tests";
    }
}
