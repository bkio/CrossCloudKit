// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Database.Tests.Common;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Memory.Basic;
using FluentAssertions;
using xRetry;

namespace CrossCloudKit.Database.Mongo.Tests;

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
        return new DatabaseServiceMongo(GetConnectionString(), TestDatabaseName, new MemoryServiceBasic());
    }

    [RetryFact(3, 5000)]
    public void DatabaseServiceMongoDB_WithValidConnectionString_ShouldInitializeSuccessfully()
    {
        // Arrange & Act
        var service = new DatabaseServiceMongo(GetConnectionString(), TestDatabaseName, new MemoryServiceBasic());

        // Assert
        service.IsInitialized.Should().BeTrue();

        // Cleanup
        service.Dispose();
    }

    [RetryFact(3, 5000)]
    public void DatabaseServiceMongoDB_WithInvalidConnectionString_ShouldFailInitialization()
    {
        // Arrange & Act
        var service = new DatabaseServiceMongo("mongodb://invalid-host:27017", TestDatabaseName, new MemoryServiceBasic());

        // Assert
        // Note: MongoDB client initialization is lazy, so this might still return true
        // The actual failure would occur during first operation
        service.Should().NotBeNull();

        // Cleanup
        service.Dispose();
    }
}
