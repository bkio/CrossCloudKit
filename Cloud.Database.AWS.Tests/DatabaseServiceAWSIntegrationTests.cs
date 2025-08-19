// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Cloud.Database.AWS;
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

    #region AWS-Specific Tests

    [Fact]
    public async Task CreateTableAsync_ShouldCreateTableSuccessfully()
    {
        // Arrange
        var tableName = GetTestTableName();
        using var client = new AmazonDynamoDBClient(new AmazonDynamoDBConfig
        {
            ServiceURL = _dynamoDbEndpoint
        });

        try
        {
            var request = new CreateTableRequest
            {
                TableName = tableName,
                KeySchema = new List<KeySchemaElement>
                {
                    new() { AttributeName = "Id", KeyType = KeyType.HASH }
                },
                AttributeDefinitions = new List<AttributeDefinition>
                {
                    new() { AttributeName = "Id", AttributeType = ScalarAttributeType.S }
                },
                BillingMode = BillingMode.PAY_PER_REQUEST
            };

            // Act
            var response = await client.CreateTableAsync(request);

            // Assert
            response.Should().NotBeNull();
            response.TableDescription.TableName.Should().Be(tableName);
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
        }
    }

    [Fact]
    public async Task DatabaseServiceAWS_WithValidLocalEndpoint_ShouldInitializeSuccessfully()
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

    [Fact]
    public async Task PutItemAsync_WithComplexObject_ShouldHandleNestedStructures()
    {
        // Arrange
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Utilities.Common.PrimitiveType("complex-test-key");

        var complexItem = new Newtonsoft.Json.Linq.JObject
        {
            ["Name"] = "ComplexItem",
            ["Metadata"] = new Newtonsoft.Json.Linq.JObject
            {
                ["Version"] = "1.0",
                ["Author"] = "TestUser"
            },
            ["Numbers"] = new Newtonsoft.Json.Linq.JArray { 1, 2, 3, 4, 5 },
            ["Flags"] = new Newtonsoft.Json.Linq.JObject
            {
                ["IsActive"] = true,
                ["IsVerified"] = false
            }
        };

        try
        {
            // Act
            var putResult = await service.PutItemAsync(tableName, keyName, keyValue, complexItem);
            var getResult = await service.GetItemAsync(tableName, keyName, keyValue);

            // Assert
            putResult.IsSuccessful.Should().BeTrue();
            getResult.IsSuccessful.Should().BeTrue();
            getResult.Data.Should().NotBeNull();
            getResult.Data!["Name"]?.ToString().Should().Be("ComplexItem");
            getResult.Data["Metadata"]?.Should().NotBeNull();
            getResult.Data["Numbers"]?.Should().NotBeNull();
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [Fact]
    public async Task TableBuilder_Integration_ShouldWorkWithComplexSchema()
    {
        // Arrange
        var tableName = GetTestTableName();
        using var client = new AmazonDynamoDBClient(new AmazonDynamoDBConfig
        {
            ServiceURL = _dynamoDbEndpoint
        });

        try
        {
            // Create table with GSI
            var request = new CreateTableRequest
            {
                TableName = tableName,
                KeySchema = new List<KeySchemaElement>
                {
                    new() { AttributeName = "PK", KeyType = KeyType.HASH },
                    new() { AttributeName = "SK", KeyType = KeyType.RANGE }
                },
                AttributeDefinitions = new List<AttributeDefinition>
                {
                    new() { AttributeName = "PK", AttributeType = ScalarAttributeType.S },
                    new() { AttributeName = "SK", AttributeType = ScalarAttributeType.S },
                    new() { AttributeName = "GSI1PK", AttributeType = ScalarAttributeType.S }
                },
                GlobalSecondaryIndexes = new List<GlobalSecondaryIndex>
                {
                    new()
                    {
                        IndexName = "GSI1",
                        KeySchema = new List<KeySchemaElement>
                        {
                            new() { AttributeName = "GSI1PK", KeyType = KeyType.HASH }
                        },
                        Projection = new Projection { ProjectionType = ProjectionType.ALL }
                    }
                },
                BillingMode = BillingMode.PAY_PER_REQUEST
            };

            await client.CreateTableAsync(request);

            // Wait for table to be active using simple polling
            var maxWaitTime = TimeSpan.FromMinutes(5);
            var startTime = DateTime.UtcNow;
            
            while (DateTime.UtcNow - startTime < maxWaitTime)
            {
                try
                {
                    var describeResponse = await client.DescribeTableAsync(tableName);
                    if (describeResponse.Table.TableStatus == TableStatus.ACTIVE)
                        break;
                }
                catch
                {
                    // Continue waiting
                }
                await Task.Delay(2000); // Wait 2 seconds before checking again
            }

            var service = CreateDatabaseService();
            var keyValue = new Utilities.Common.PrimitiveType("test-pk#test-sk");
            var item = new Newtonsoft.Json.Linq.JObject
            {
                ["GSI1PK"] = "gsi-value",
                ["Data"] = "test-data"
            };

            // Act
            var result = await service.PutItemAsync(tableName, "PK", keyValue, item);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            
            // Cleanup
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
        }
    }

    [Fact]
    public async Task BatchOperations_WithMultipleItems_ShouldHandleEfficiently()
    {
        // Arrange
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";

        // Create multiple items for batch testing
        var keyValues = Enumerable.Range(1, 25)
            .Select(i => new Utilities.Common.PrimitiveType($"batch-key-{i}"))
            .ToArray();

        try
        {
            // Setup - Put multiple items
            foreach (var keyValue in keyValues)
            {
                var item = new Newtonsoft.Json.Linq.JObject
                {
                    ["Name"] = $"BatchItem-{keyValue.AsString}",
                    ["Value"] = keyValue.AsString.GetHashCode()
                };
                await service.PutItemAsync(tableName, keyName, keyValue, item);
            }

            // Act - Get multiple items
            var result = await service.GetItemsAsync(tableName, keyName, keyValues);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().NotBeNull();
            result.Data!.Count.Should().Be(keyValues.Length);
            
            // Verify each item
            foreach (var item in result.Data)
            {
                item["Name"]?.ToString().Should().StartWith("BatchItem-");
                item[keyName]?.ToString().Should().StartWith("batch-key-");
            }
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [Fact]
    public async Task ConditionalOperations_WithAttributeNotExists_ShouldWorkCorrectly()
    {
        // Arrange
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Utilities.Common.PrimitiveType("conditional-test");

        try
        {
            var item = new Newtonsoft.Json.Linq.JObject
            {
                ["Name"] = "ConditionalItem",
                ["Status"] = "active"
            };

            // Setup
            await service.PutItemAsync(tableName, keyName, keyValue, item);

            // Test with attribute not exists condition
            var condition = service.BuildAttributeNotExistsCondition("NewAttribute");
            var updateData = new Newtonsoft.Json.Linq.JObject
            {
                ["NewAttribute"] = "NewValue"
            };

            // Act
            var result = await service.UpdateItemAsync(tableName, keyName, keyValue, updateData, 
                condition: condition);

            // Assert
            result.IsSuccessful.Should().BeTrue();

            // Verify the update happened
            var getResult = await service.GetItemAsync(tableName, keyName, keyValue);
            getResult.Data!["NewAttribute"]?.ToString().Should().Be("NewValue");
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    #endregion

    #region Performance Tests

    [Fact]
    public async Task ParallelOperations_ShouldHandleConcurrentRequests()
    {
        // Arrange
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        const int concurrentOperations = 10;

        try
        {
            // Act - Perform parallel put operations
            var tasks = Enumerable.Range(1, concurrentOperations)
                .Select(async i =>
                {
                    var keyValue = new Utilities.Common.PrimitiveType($"parallel-key-{i}");
                    var item = new Newtonsoft.Json.Linq.JObject
                    {
                        ["Name"] = $"ParallelItem-{i}",
                        ["ThreadId"] = Thread.CurrentThread.ManagedThreadId
                    };
                    return await service.PutItemAsync(tableName, keyName, keyValue, item);
                });

            var results = await Task.WhenAll(tasks);

            // Assert
            results.Should().HaveCount(concurrentOperations);
            results.Should().OnlyContain(r => r.IsSuccessful);

            // Verify all items were created
            var scanResult = await service.ScanTableAsync(tableName, [keyName]);
            scanResult.Data!.Count.Should().Be(concurrentOperations);
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    #endregion

    #region Error Scenarios

    [Fact]
    public async Task Operations_OnNonExistentTable_ShouldHandleGracefully()
    {
        // Arrange
        var service = CreateDatabaseService();
        var nonExistentTable = "non-existent-table-" + Guid.NewGuid();
        var keyName = "Id";
        var keyValue = new Utilities.Common.PrimitiveType("test-key");

        try
        {
            // Act & Assert - Different operations should handle non-existent tables gracefully
            var getResult = await service.GetItemAsync(nonExistentTable, keyName, keyValue);
            getResult.IsSuccessful.Should().BeFalse();

            var scanResult = await service.ScanTableAsync(nonExistentTable, [keyName]);
            scanResult.IsSuccessful.Should().BeFalse();

            var existsResult = await service.ItemExistsAsync(nonExistentTable, keyName, keyValue);
            existsResult.IsSuccessful.Should().BeFalse();
        }
        finally
        {
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [Fact]
    public async Task LargeItem_ShouldHandleWithinDynamoDBLimits()
    {
        // Arrange
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Utilities.Common.PrimitiveType("large-item-key");

        // Create a large item (but within DynamoDB's 400KB limit)
        var largeData = new string('A', 100_000); // 100KB string
        var largeItem = new Newtonsoft.Json.Linq.JObject
        {
            ["Name"] = "LargeItem",
            ["LargeData"] = largeData,
            ["Metadata"] = new Newtonsoft.Json.Linq.JObject
            {
                ["Size"] = largeData.Length,
                ["Created"] = DateTime.UtcNow
            }
        };

        try
        {
            // Act
            var putResult = await service.PutItemAsync(tableName, keyName, keyValue, largeItem);
            var getResult = await service.GetItemAsync(tableName, keyName, keyValue);

            // Assert
            putResult.IsSuccessful.Should().BeTrue();
            getResult.IsSuccessful.Should().BeTrue();
            getResult.Data.Should().NotBeNull();
            getResult.Data!["LargeData"]?.ToString().Should().HaveLength(100_000);
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    #endregion
}
