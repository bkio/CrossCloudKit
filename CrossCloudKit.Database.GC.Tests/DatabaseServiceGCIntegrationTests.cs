// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Database.Tests.Common;
using CrossCloudKit.Interfaces;
using FluentAssertions;
using xRetry;
using Xunit;

namespace CrossCloudKit.Database.GC.Tests;

/// <summary>
/// Full integration tests for DatabaseServiceGC extending DatabaseServiceTestBase
///
/// IMPORTANT: These tests focus on constructor behavior, error handling, interface compliance,
/// and comprehensive condition testing with actual database operations.
/// They use intentionally invalid credentials to test error scenarios.
///
/// For ACTUAL integration testing with Google Cloud Datastore, you would need:
/// 1. Google Cloud Datastore emulator running locally, OR
/// 2. Valid service account credentials and a real Google Cloud project, OR
/// 3. Application Default Credentials (ADC) properly configured
/// </summary>
public class DatabaseServiceGCIntegrationTests : DatabaseServiceTestBase
{
    private const string TestProjectId = "test-project-id";

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
    /// Creates a database service instance for testing, using environment variables if available
    /// </summary>
    /// <param name="projectId">The Google Cloud project ID</param>
    /// <returns>A configured DatabaseServiceGC instance</returns>
    private static DatabaseServiceGC CreateServiceForTesting(string projectId)
    {
        // First try to get Base64 encoded credentials from environment
        var base64Credentials = Environment.GetEnvironmentVariable("GOOGLE_BASE64_CREDENTIALS");
        if (!string.IsNullOrEmpty(base64Credentials))
        {
            return new DatabaseServiceGC(projectId, base64Credentials, isBase64Encoded: true, Console.WriteLine);
        }

        // If no Base64 credentials, try JSON credentials from environment
        var jsonCredentials = Environment.GetEnvironmentVariable("GOOGLE_JSON_CREDENTIALS");
        if (!string.IsNullOrEmpty(jsonCredentials))
        {
            return new DatabaseServiceGC(projectId, jsonCredentials, isBase64Encoded: false, Console.WriteLine);
        }

        // If no credentials in environment, try using default credentials
        return new DatabaseServiceGC(projectId, useDefaultCredentials: true, Console.WriteLine);
    }

    protected override IDatabaseService CreateDatabaseService()
    {
        // Use the enhanced helper that supports real credentials
        var projectId = GetTestProjectId(TestProjectId);
        return CreateServiceForTesting(projectId);
    }

    protected override string GetTestTableName() => $"test-kind-{Guid.NewGuid():N}";

    [RetryFact(3)]
    public void DatabaseServiceGC_WithServiceAccountFilePath_ShouldInitialize()
    {
        // This test demonstrates using a file path (will fail initialization with non-existent file)

        // Arrange
        const string mockFilePath = "/path/to/nonexistent/service-account.json";
        var errorMessages = new List<string>();
        void ErrorAction(string message) => errorMessages.Add(message);

        // Act
        var service = new DatabaseServiceGC(TestProjectId, mockFilePath, ErrorAction);

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

    [RetryFact(3)]
    public void DatabaseServiceGC_WithDefaultCredentials_ShouldInitialize()
    {
        // This test may succeed if running with proper ADC setup, or fail gracefully

        // Arrange & Act
        var service = new DatabaseServiceGC(TestProjectId, useDefaultCredentials: true);

        // Assert
        service.Should().NotBeNull();
        // Note: IsInitialized depends on whether ADC is properly configured in the test environment

        // Cleanup
        if (service is IAsyncDisposable asyncDisposable)
        {
            _ = Task.Run(async () => await asyncDisposable.DisposeAsync());
        }
    }

    [RetryFact(3)]
    public void DatabaseServiceGC_WithNullProjectId_ShouldThrowArgumentException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new DatabaseServiceGC(null!, useDefaultCredentials: true));
        Assert.Throws<ArgumentException>(() => new DatabaseServiceGC("", useDefaultCredentials: true));
        Assert.Throws<ArgumentException>(() => new DatabaseServiceGC("   ", useDefaultCredentials: true));
    }

    [RetryFact(3)]
    public void DatabaseServiceGC_WithDefaultCredentialsFalse_ShouldThrowArgumentException()
    {
        // Act & Assert
        Assert.Throws<ArgumentException>(() => new DatabaseServiceGC(TestProjectId, useDefaultCredentials: false));
    }

    [RetryFact(3)]
    public void DatabaseServiceGC_WithNullServiceAccountPath_ShouldThrowArgumentException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new DatabaseServiceGC(TestProjectId, null!));
        Assert.Throws<ArgumentException>(() => new DatabaseServiceGC(TestProjectId, ""));
        Assert.Throws<ArgumentException>(() => new DatabaseServiceGC(TestProjectId, "   "));
    }

    [RetryFact(3)]
    public void DatabaseServiceGC_WithNullJsonContent_ShouldThrowArgumentException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new DatabaseServiceGC(TestProjectId, null!, isBase64Encoded: false));
        Assert.Throws<ArgumentException>(() => new DatabaseServiceGC(TestProjectId, "", isBase64Encoded: false));
        Assert.Throws<ArgumentException>(() => new DatabaseServiceGC(TestProjectId, "   ", isBase64Encoded: false));
    }

    [RetryFact(3)]
    public async Task DisposeAsync_ShouldCompleteSuccessfully()
    {
        var service = CreateDatabaseService();

        // Cast to DatabaseServiceGC for DisposeAsync access
        if (service is DatabaseServiceGC gcService)
        {
            // Act & Assert - should not throw
            await gcService.DisposeAsync();

            // Multiple calls should be safe
            await gcService.DisposeAsync();
        }
    }
}
