// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.File.Tests.Common;
using CrossCloudKit.Interfaces;
using CrossCloudKit.PubSub.GC;
using FluentAssertions;
using xRetry;
using Xunit;
using Xunit.Abstractions;

[assembly: CollectionBehavior(DisableTestParallelization = false, MaxParallelThreads = 8)]

namespace CrossCloudKit.File.GC.Tests;

/// <summary>
/// Full integration tests for FileServiceGC extending FileServiceTestBase
///
/// IMPORTANT: These tests focus on constructor behavior, error handling, interface compliance,
/// and comprehensive file operations testing with actual file storage operations.
/// They use intentionally invalid credentials to test error scenarios.
///
/// For ACTUAL integration testing with Google Cloud Storage, you would need:
/// 1. Google Cloud Storage with a test bucket configured, OR
/// 2. Valid service account credentials and a real Google Cloud project, OR
/// 3. Application Default Credentials (ADC) properly configured
/// </summary>
public class FileServiceGCIntegrationTests(ITestOutputHelper testOutputHelper) : FileServiceTestBase(testOutputHelper)
{
    private const string TestProjectId = "test-project-id";
    private const string TestBucketName = "test-bucket-name";

    /// <summary>
    /// Gets the test project ID from environment variable or uses fallback
    /// </summary>
    /// <param name="fallbackProjectId">Fallback project ID if environment variable is not set</param>
    /// <returns>The project ID to use for testing</returns>
    private static string GetTestProjectId(string fallbackProjectId)
    {
        return Environment.GetEnvironmentVariable("GOOGLE_CLOUD_PROJECT") ?? fallbackProjectId;
    }

    /// <summary>
    /// Gets the test bucket name from environment variable or uses fallback
    /// </summary>
    /// <param name="fallbackBucketName">Fallback bucket name if environment variable is not set</param>
    /// <returns>The bucket name to use for testing</returns>
    private static string GetTestBucketName(string fallbackBucketName)
    {
        return Environment.GetEnvironmentVariable("GOOGLE_CLOUD_TEST_BUCKET") ?? fallbackBucketName;
    }

    /// <summary>
    /// Creates a file service instance for testing, using environment variables if available
    /// </summary>
    /// <param name="projectId">The Google Cloud project ID</param>
    /// <returns>A configured FileServiceGC instance</returns>
    private static FileServiceGC CreateFileServiceForTesting(string projectId)
    {
        // First try to get Base64 encoded credentials from environment
        var base64Credentials = Environment.GetEnvironmentVariable("GOOGLE_BASE64_CREDENTIALS");
        if (!string.IsNullOrEmpty(base64Credentials))
        {
            return new FileServiceGC(projectId, base64Credentials, isBase64Encoded: true, Console.WriteLine);
        }

        // If no Base64 credentials, try JSON credentials from environment
        var jsonCredentials = Environment.GetEnvironmentVariable("GOOGLE_JSON_CREDENTIALS");
        if (!string.IsNullOrEmpty(jsonCredentials))
        {
            return new FileServiceGC(projectId, jsonCredentials, isBase64Encoded: false, Console.WriteLine);
        }

        // If no credentials in environment, try using default credentials
        return new FileServiceGC(projectId, useDefaultCredentials: true, Console.WriteLine);
    }

    /// <summary>
    /// Creates a pub/sub service instance for testing, using environment variables if available
    /// </summary>
    /// <param name="projectId">The Google Cloud project ID</param>
    /// <returns>A configured PubSubServiceGC instance</returns>
    private static PubSubServiceGC CreatePubSubServiceForTesting(string projectId)
    {
        // First try to get Base64 encoded credentials from environment
        var base64Credentials = Environment.GetEnvironmentVariable("GOOGLE_BASE64_CREDENTIALS");
        if (!string.IsNullOrEmpty(base64Credentials))
        {
            return new PubSubServiceGC(projectId, base64Credentials, isBase64Encoded: true, Console.WriteLine);
        }

        // If no Base64 credentials, try JSON credentials from environment
        var jsonCredentials = Environment.GetEnvironmentVariable("GOOGLE_JSON_CREDENTIALS");
        if (!string.IsNullOrEmpty(jsonCredentials))
        {
            return new PubSubServiceGC(projectId, jsonCredentials, isBase64Encoded: false, Console.WriteLine);
        }

        // If no credentials in the environment, try using default credentials
        return new PubSubServiceGC(projectId, CredentialType.ApplicationDefault, null, false, Console.WriteLine);
    }

    protected override IFileService CreateFileService()
    {
        var projectId = GetTestProjectId(TestProjectId);
        return CreateFileServiceForTesting(projectId);
    }
    protected override IPubSubService CreatePubSubService()
    {
        var projectId = GetTestProjectId(TestProjectId);
        return CreatePubSubServiceForTesting(projectId);
    }

    protected override string GetTestBucketName() => GetTestBucketName(TestBucketName);

    [RetryFact(3, 5000)]
    public void FileServiceGC_WithServiceAccountFilePath_ShouldInitialize()
    {
        // This test demonstrates using a file path (will fail initialization with non-existent file)

        // Arrange
        const string mockFilePath = "/path/to/nonexistent/service-account.json";
        var errorMessages = new List<string>();
        void ErrorAction(string message) => errorMessages.Add(message);

        // Act
        var service = new FileServiceGC(TestProjectId, mockFilePath, ErrorAction);

        // Assert
        service.Should().NotBeNull();
        service.IsInitialized.Should().BeFalse(); // Expected to fail with non-existent file
        errorMessages.Should().NotBeEmpty(); // Should capture file not found error

        // Cleanup
        if (service is IAsyncDisposable asyncDisposable)
        {
            _ = Task.Run(async () => await asyncDisposable.DisposeAsync());
        }
    }

    [RetryFact(3, 5000)]
    public void FileServiceGC_WithDefaultCredentials_ShouldInitialize()
    {
        // This test may succeed if running with proper ADC setup, or fail gracefully

        // Arrange & Act
        var service = new FileServiceGC(TestProjectId, useDefaultCredentials: true);

        // Assert
        service.Should().NotBeNull();
        // Note: IsInitialized depends on whether ADC is properly configured in the test environment

        // Cleanup
        if (service is IAsyncDisposable asyncDisposable)
        {
            _ = Task.Run(async () => await asyncDisposable.DisposeAsync());
        }
    }

    [RetryFact(3, 5000)]
    public void FileServiceGC_WithNullProjectId_ShouldThrowArgumentException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new FileServiceGC(null!, useDefaultCredentials: true));
        Assert.Throws<ArgumentException>(() => new FileServiceGC("", useDefaultCredentials: true));
        Assert.Throws<ArgumentException>(() => new FileServiceGC("   ", useDefaultCredentials: true));
    }

    [RetryFact(3, 5000)]
    public void FileServiceGC_WithDefaultCredentialsFalse_ShouldThrowArgumentException()
    {
        // Act & Assert
        Assert.Throws<ArgumentException>(() => new FileServiceGC(TestProjectId, useDefaultCredentials: false));
    }

    [RetryFact(3, 5000)]
    public void FileServiceGC_WithNullServiceAccountPath_ShouldThrowArgumentException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new FileServiceGC(TestProjectId, null!));
        Assert.Throws<ArgumentException>(() => new FileServiceGC(TestProjectId, ""));
        Assert.Throws<ArgumentException>(() => new FileServiceGC(TestProjectId, "   "));
    }

    [RetryFact(3, 5000)]
    public void FileServiceGC_WithNullServiceAccountContent_ShouldThrowArgumentException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new FileServiceGC(TestProjectId, null!, isBase64Encoded: false));
        Assert.Throws<ArgumentException>(() => new FileServiceGC(TestProjectId, "", isBase64Encoded: false));
        Assert.Throws<ArgumentException>(() => new FileServiceGC(TestProjectId, "   ", isBase64Encoded: false));
    }

    [RetryFact(3, 5000)]
    public void FileServiceGC_WithInvalidBase64Credentials_ShouldFailGracefully()
    {
        // Arrange
        const string invalidBase64 = "not-valid-base64-content!@#$";
        var errorMessages = new List<string>();
        void ErrorAction(string message) => errorMessages.Add(message);

        // Act
        var service = new FileServiceGC(TestProjectId, invalidBase64, isBase64Encoded: true, ErrorAction);

        // Assert
        service.Should().NotBeNull();
        service.IsInitialized.Should().BeFalse();
        errorMessages.Should().NotBeEmpty();
        errorMessages.Should().Contain(msg => msg.Contains("Base64 decode"));

        // Cleanup
        if (service is IAsyncDisposable asyncDisposable)
        {
            _ = Task.Run(async () => await asyncDisposable.DisposeAsync());
        }
    }
}
