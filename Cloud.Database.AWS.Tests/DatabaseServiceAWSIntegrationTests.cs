// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Cloud.Database.Tests.Common;
using Cloud.Interfaces;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using FluentAssertions;
using Xunit;

namespace Cloud.Database.AWS.Tests;

/// <summary>
/// Integration tests for DatabaseServiceAWS using Testcontainers DynamoDB Local
/// </summary>
public class DatabaseServiceAWSIntegrationTests : DatabaseServiceTestBase, IAsyncLifetime
{
    private IContainer? _dynamoDbContainer;
    private string _dynamoDbEndpoint = string.Empty;

    public async Task InitializeAsync()
    {
        // Start DynamoDB Local container
        _dynamoDbContainer = new ContainerBuilder()
            .WithImage("amazon/dynamodb-local:latest")
            .WithPortBinding(8000, true)
            .WithCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(8000))
            .Build();

        await _dynamoDbContainer.StartAsync();
        
        _dynamoDbEndpoint = $"http://localhost:{_dynamoDbContainer.GetMappedPublicPort(8000)}";
    }

    public async Task DisposeAsync()
    {
        if (_dynamoDbContainer != null)
        {
            await _dynamoDbContainer.DisposeAsync();
        }
    }

    protected override IDatabaseService CreateDatabaseService()
    {
        return new DatabaseServiceAWS(_dynamoDbEndpoint);
    }

    protected override async Task CleanupDatabaseAsync(string tableName)
    {
        try
        {
            using var client = new AmazonDynamoDBClient(new AmazonDynamoDBConfig
            {
                ServiceURL = _dynamoDbEndpoint
            });

            await client.DeleteTableAsync(tableName);
            
            // Wait for table to be deleted using simple polling instead of waiter
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

    [Fact]
    public void DatabaseServiceAWS_WithValidLocalEndpoint_ShouldInitializeSuccessfully()
    {
        // Arrange & Act
        var service = new DatabaseServiceAWS(_dynamoDbEndpoint);

        // Assert
        service.IsInitialized.Should().BeTrue();
        
        // Cleanup
        service.Dispose();
    }

    [Fact]
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
