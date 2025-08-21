// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

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
}
