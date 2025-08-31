// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using CrossCloudKit.Database.Tests.Common;
using CrossCloudKit.Interfaces;
using FluentAssertions;
using xRetry;
using Xunit;

namespace CrossCloudKit.Database.AWS.Tests;

/// <summary>
/// Integration tests for DatabaseServiceAWS using AWS credentials
/// </summary>
public class DatabaseServiceAWSIntegrationTests : DatabaseServiceTestBase
{
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

    protected override IDatabaseService CreateDatabaseService()
    {
        var accessKey = GetAWSAccessKey();
        var secretKey = GetAWSSecretKey();
        var region = GetAWSRegion();

        // If credentials are not provided, return a service that will fail initialization
        if (string.IsNullOrEmpty(accessKey) || string.IsNullOrEmpty(secretKey))
        {
            return new DatabaseServiceAWS("invalid-key", "invalid-secret", region);
        }

        return new DatabaseServiceAWS(accessKey, secretKey, region);
    }

    protected override async Task CleanupDatabaseAsync(string tableName)
    {
        try
        {
            var accessKey = GetAWSAccessKey();
            var secretKey = GetAWSSecretKey();
            var region = GetAWSRegion();

            if (string.IsNullOrEmpty(accessKey) || string.IsNullOrEmpty(secretKey))
            {
                return; // Skip cleanup if no credentials
            }

            using var client = new AmazonDynamoDBClient(
                new Amazon.Runtime.BasicAWSCredentials(accessKey, secretKey),
                Amazon.RegionEndpoint.GetBySystemName(region));

            await client.DeleteTableAsync(tableName);

            // Wait for table to be deleted using simple polling
            var maxWaitTime = TimeSpan.FromMinutes(2);
            var startTime = DateTime.UtcNow;

            while (DateTime.UtcNow - startTime < maxWaitTime)
            {
                try
                {
                    await client.DescribeTableAsync(tableName);
                    await Task.Delay(1000); // Wait 1 second before checking again
                }
                catch (ResourceNotFoundException)
                {
                    // Table is deleted
                    break;
                }
            }
        }
        catch (ResourceNotFoundException)
        {
            // Table doesn't exist, which is fine
        }
        catch (Exception)
        {
            // Ignore cleanup errors in tests
        }
    }

    protected override string GetTestTableName() => $"test-table-{Guid.NewGuid():N}";

    [RetryFact(3)]
    public void DatabaseServiceAWS_WithValidCredentials_ShouldInitializeSuccessfully()
    {
        // Arrange
        var accessKey = GetAWSAccessKey();
        var secretKey = GetAWSSecretKey();
        var region = GetAWSRegion();

        // Skip if no credentials provided
        if (string.IsNullOrEmpty(accessKey) || string.IsNullOrEmpty(secretKey))
        {
            return;
        }

        // Act
        var service = new DatabaseServiceAWS(accessKey, secretKey, region);

        // Assert
        service.IsInitialized.Should().BeTrue();

        // Cleanup
        service.Dispose();
    }

    [RetryFact(3)]
    public void DatabaseServiceAWS_WithInvalidCredentials_ShouldFailInitialization()
    {
        // Arrange & Act
        var service = new DatabaseServiceAWS("invalid-access-key", "invalid-secret-key", "us-east-1");

        // Assert
        service.IsInitialized.Should().BeFalse();

        // Cleanup
        service.Dispose();
    }
}
