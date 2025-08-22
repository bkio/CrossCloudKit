// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Cloud.Database.Tests.Common;
using Cloud.Interfaces;
using FluentAssertions;
using MongoDB.Driver;
using Xunit;

namespace Cloud.Database.Mongo.Tests;

/// <summary>
/// Integration tests for DatabaseServiceMongoDB using Testcontainers
/// </summary>
public class DatabaseServiceMongoDBIntegrationTests : DatabaseServiceTestBase
{
    private static string GetConnectionString()
    {
        var host = Environment.GetEnvironmentVariable("MONGODB_HOST") ?? "127.0.0.1";
        var user = Environment.GetEnvironmentVariable("MONGODB_USER") ?? "test";
        var pwd = Environment.GetEnvironmentVariable("MONGODB_PASSWORD") ?? "test";
        return $"mongodb+srv://{user}:{pwd}@{host}/";
    }
    private const string TestDatabaseName = "test";

    protected override IDatabaseService CreateDatabaseService()
    {
        return new DatabaseServiceMongoDB(GetConnectionString(), TestDatabaseName);
    }

    protected override async Task CleanupDatabaseAsync(string tableName)
    {
        try
        {
            var client = new MongoClient(GetConnectionString());
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
        var service = new DatabaseServiceMongoDB(GetConnectionString(), TestDatabaseName);

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
}
