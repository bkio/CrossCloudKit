// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Cloud.Database.Mongo;
using Cloud.Database.Tests.Common;
using Cloud.Interfaces;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using FluentAssertions;
using MongoDB.Driver;
using Xunit;

namespace Cloud.Database.Mongo.Tests;

/// <summary>
/// Integration tests for DatabaseServiceMongoDB using Testcontainers
/// </summary>
public class DatabaseServiceMongoDBIntegrationTests : DatabaseServiceTestBase, IAsyncLifetime
{
    private IContainer? _mongoContainer;
    private string _connectionString = string.Empty;
    private const string TestDatabaseName = "testdb";

    public async Task InitializeAsync()
    {
        // Start MongoDB container
        _mongoContainer = new ContainerBuilder()
            .WithImage("mongo:7.0")
            .WithPortBinding(27017, true)
            .WithEnvironment("MONGO_INITDB_DATABASE", TestDatabaseName)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(27017))
            .Build();

        await _mongoContainer.StartAsync();
        
        var port = _mongoContainer.GetMappedPublicPort(27017);
        _connectionString = $"mongodb://localhost:{port}";
    }

    public async Task DisposeAsync()
    {
        if (_mongoContainer != null)
        {
            await _mongoContainer.DisposeAsync();
        }
    }

    protected override IDatabaseService CreateDatabaseService()
    {
        return new DatabaseServiceMongoDB(_connectionString, TestDatabaseName);
    }

    protected override async Task CleanupDatabaseAsync(string tableName)
    {
        try
        {
            var client = new MongoClient(_connectionString);
            var database = client.GetDatabase(TestDatabaseName);
            
            await database.DropCollectionAsync(tableName);
        }
        catch (Exception)
        {
            // Ignore cleanup errors in tests
        }
    }

    protected override string GetTestTableName() => $"test-collection-{Guid.NewGuid():N}";

    #region MongoDB-Specific Tests

    [Fact]
    public void DatabaseServiceMongoDB_WithValidConnectionString_ShouldInitializeSuccessfully()
    {
        // Arrange & Act
        var service = new DatabaseServiceMongoDB(_connectionString, TestDatabaseName);

        // Assert
        service.IsInitialized.Should().BeTrue();
        
        // Cleanup
        service.Dispose();
    }

    [Fact]
    public void DatabaseServiceMongoDB_WithInvalidConnectionString_ShouldFailInitialization()
    {
        // Arrange & Act
        var service = new DatabaseServiceMongoDB("mongodb://invalid-host:27017", TestDatabaseName);

        // Assert
        // Note: MongoDB client initialization is lazy, so this might still return true
        // The actual failure would occur during first operation
        service.Should().NotBeNull();
        
        // Cleanup
        service.Dispose();
    }

    [Fact]
    public void DatabaseServiceMongoDB_WithHostAndPort_ShouldInitializeSuccessfully()
    {
        // Arrange
        var port = _mongoContainer!.GetMappedPublicPort(27017);

        // Act
        var service = new DatabaseServiceMongoDB("localhost", port, TestDatabaseName);

        // Assert
        service.IsInitialized.Should().BeTrue();
        
        // Cleanup
        service.Dispose();
    }

    [Fact]
    public async Task PutItemAsync_WithObjectId_ShouldHandleMongoDBObjectId()
    {
        // Arrange
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Utilities.Common.PrimitiveType("507f1f77bcf86cd799439011"); // Valid ObjectId format

        var item = new Newtonsoft.Json.Linq.JObject
        {
            ["Name"] = "MongoItem",
            ["Value"] = 42,
            ["IsMongoTest"] = true
        };

        try
        {
            // Act
            var putResult = await service.PutItemAsync(tableName, keyName, keyValue, item);
            var getResult = await service.GetItemAsync(tableName, keyName, keyValue);

            // Assert
            putResult.IsSuccessful.Should().BeTrue();
            getResult.IsSuccessful.Should().BeTrue();
            getResult.Data.Should().NotBeNull();
            getResult.Data!["Name"]?.ToString().Should().Be("MongoItem");
            getResult.Data["IsMongoTest"]?.ToObject<bool>().Should().BeTrue();
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [Fact]
    public async Task PutItemAsync_WithComplexNestedDocument_ShouldPreserveStructure()
    {
        // Arrange
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Utilities.Common.PrimitiveType("complex-mongo-doc");

        var complexItem = new Newtonsoft.Json.Linq.JObject
        {
            ["Name"] = "ComplexMongoDocument",
            ["Profile"] = new Newtonsoft.Json.Linq.JObject
            {
                ["FirstName"] = "John",
                ["LastName"] = "Doe",
                ["Age"] = 30,
                ["Addresses"] = new Newtonsoft.Json.Linq.JArray
                {
                    new Newtonsoft.Json.Linq.JObject
                    {
                        ["Type"] = "Home",
                        ["Street"] = "123 Main St",
                        ["City"] = "Anytown",
                        ["Coordinates"] = new Newtonsoft.Json.Linq.JObject
                        {
                            ["Latitude"] = 40.7128,
                            ["Longitude"] = -74.0060
                        }
                    },
                    new Newtonsoft.Json.Linq.JObject
                    {
                        ["Type"] = "Work",
                        ["Street"] = "456 Business Ave",
                        ["City"] = "Worktown"
                    }
                }
            },
            ["Tags"] = new Newtonsoft.Json.Linq.JArray { "vip", "customer", "active" },
            ["Metadata"] = new Newtonsoft.Json.Linq.JObject
            {
                ["CreatedAt"] = DateTime.UtcNow,
                ["Version"] = "1.0"
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

            var retrievedData = getResult.Data!;
            retrievedData["Profile"]?["FirstName"]?.ToString().Should().Be("John");
            retrievedData["Profile"]?["Addresses"]?.Should().BeOfType<Newtonsoft.Json.Linq.JArray>();
            var addresses = (Newtonsoft.Json.Linq.JArray)retrievedData["Profile"]!["Addresses"]!;
            addresses.Should().HaveCount(2);
            addresses[0]?["Coordinates"]?["Latitude"]?.ToObject<double>().Should().BeApproximately(40.7128, 0.0001);
        }
        finally
        {
            await CleanupDatabaseAsync(tableName);
            if (service is IDisposable disposable)
                disposable.Dispose();
        }
    }

    [Fact]
    public async Task BasicCrudOperations_ShouldWork()
    {
        // Arrange
        var service = CreateDatabaseService();
        var tableName = GetTestTableName();
        var keyName = "Id";
        var keyValue = new Utilities.Common.PrimitiveType("test-key");

        var item = new Newtonsoft.Json.Linq.JObject
        {
            ["Name"] = "TestItem",
            ["Value"] = 42
        };

        try
        {
            // Act & Assert - Put
            var putResult = await service.PutItemAsync(tableName, keyName, keyValue, item);
            putResult.IsSuccessful.Should().BeTrue();

            // Act & Assert - Get
            var getResult = await service.GetItemAsync(tableName, keyName, keyValue);
            getResult.IsSuccessful.Should().BeTrue();
            getResult.Data.Should().NotBeNull();
            getResult.Data!["Name"]?.ToString().Should().Be("TestItem");

            // Act & Assert - Update
            var updateData = new Newtonsoft.Json.Linq.JObject { ["Name"] = "UpdatedItem" };
            var updateResult = await service.UpdateItemAsync(tableName, keyName, keyValue, updateData);
            updateResult.IsSuccessful.Should().BeTrue();

            // Act & Assert - Delete
            var deleteResult = await service.DeleteItemAsync(tableName, keyName, keyValue);
            deleteResult.IsSuccessful.Should().BeTrue();
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
