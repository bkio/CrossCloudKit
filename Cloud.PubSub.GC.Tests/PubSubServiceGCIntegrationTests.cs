// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Cloud.Interfaces;
using Cloud.PubSub.Tests.Common;
using Xunit.Abstractions;

namespace Cloud.PubSub.GC.Tests;

public class PubSubServiceGCIntegrationTests(ITestOutputHelper testOutputHelper) : PubSubServiceTestBase(testOutputHelper)
{
    private readonly ITestOutputHelper _testOutputHelper = testOutputHelper;
    private const string TestProjectId = "test-project-id";

    /// <summary>
    /// Creates a pub/sub service instance for testing, using environment variables if available
    /// </summary>
    /// <param name="projectId">The Google Cloud project ID</param>
    /// <returns>A configured PubSubServiceGC instance</returns>
    private PubSubServiceGC CreatePubSubServiceForTesting(string projectId)
    {
        // First try to get Base64 encoded credentials from environment
        var base64Credentials = Environment.GetEnvironmentVariable("GOOGLE_BASE64_CREDENTIALS");
        if (!string.IsNullOrEmpty(base64Credentials))
        {
            return new PubSubServiceGC(projectId, base64Credentials, isBase64Encoded: true, _testOutputHelper.WriteLine);
        }

        // If no Base64 credentials, try JSON credentials from environment
        var jsonCredentials = Environment.GetEnvironmentVariable("GOOGLE_JSON_CREDENTIALS");
        return !string.IsNullOrEmpty(jsonCredentials) ? new PubSubServiceGC(projectId, jsonCredentials, isBase64Encoded: false, _testOutputHelper.WriteLine) :
            // If no credentials in environment, try using default credentials
            new PubSubServiceGC(projectId, CredentialType.ApplicationDefault, null, false, _testOutputHelper.WriteLine);
    }

    protected override IPubSubService CreatePubSubService()
    {
        var projectId = GetTestProjectId();
        return CreatePubSubServiceForTesting(projectId);
    }

    private string GetTestProjectId()
    {
        return Environment.GetEnvironmentVariable("GOOGLE_CLOUD_PROJECT") ?? TestProjectId;
    }
}
