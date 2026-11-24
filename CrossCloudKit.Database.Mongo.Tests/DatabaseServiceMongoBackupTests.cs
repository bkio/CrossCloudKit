// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.File.Basic;
using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Memory.Basic;
using CrossCloudKit.PubSub.Basic;
using CrossCloudKit.Utilities.Common;
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json.Linq;
using xRetry;
using Xunit;

namespace CrossCloudKit.Database.Mongo.Tests;

/// <summary>
/// Backup and restore against a real MongoDB. Each test uses its own database: a restore drops and recreates every
/// table in the backup, which must not touch the shared test database other tests use in parallel.
/// </summary>
[Collection(MongoTestCollection.Name)]
public class DatabaseServiceMongoBackupTests : IAsyncDisposable
{
    private readonly string _connectionString = Environment.GetEnvironmentVariable("MONGODB_CONNECTION_STRING") ?? "mongodb://test:test@127.0.0.1/";
    private readonly string _databaseName = $"backup-test-{Guid.NewGuid():N}";
    private readonly string _basePath = Path.Combine(Path.GetTempPath(), $"DatabaseServiceMongoBackupTests_{Guid.NewGuid():N}");
    private readonly MemoryServiceBasic _memoryService;
    private readonly PubSubServiceBasic _pubSubService;
    private readonly FileServiceBasic _fileService;
    private readonly DatabaseServiceMongo _databaseService;

    public DatabaseServiceMongoBackupTests()
    {
        _memoryService = new MemoryServiceBasic(null, _basePath);
        _pubSubService = new PubSubServiceBasic(_basePath);
        _fileService = new FileServiceBasic(_memoryService, _pubSubService, _basePath);
        _databaseService = new DatabaseServiceMongo(_connectionString, _databaseName, _memoryService);
    }

    [RetryFact(3, 5000)]
    public async Task TakeBackupThenRestore_WithIntegerKeys_ShouldKeepTheKeysIntegers()
    {
        // Arrange: a table keyed by integers (as ReflectiveForms stores its entity ids)
        await using var backupService = new DatabaseServiceBackup(_databaseService, _fileService, _pubSubService, "test-backup-bucket", "backups/");
        const string tableName = "IntegerKeyed";
        var key75 = new DbKey("id", new Primitive(75));
        (await _databaseService.PutItemAsync(tableName, key75, new JObject { ["Name"] = "Seventy-five" })).IsSuccessful.Should().BeTrue();
        (await _databaseService.PutItemAsync(tableName, new DbKey("id", new Primitive(76)), new JObject { ["Name"] = "Seventy-six" })).IsSuccessful.Should().BeTrue();

        var backupResult = await backupService.TakeBackup();
        backupResult.IsSuccessful.Should().BeTrue(backupResult.ErrorMessage);
        backupResult.Data.Should().NotBeNull();
        await _databaseService.DropTableAsync(tableName);

        // Act
        var restoreResult = await backupService.RestoreBackupAsync(backupResult.Data!);

        // Assert: found again with the original integer key, and stored as an integer, not as "75"
        restoreResult.IsSuccessful.Should().BeTrue(restoreResult.ErrorMessage);
        var restored = await _databaseService.GetItemAsync(tableName, key75);
        restored.IsSuccessful.Should().BeTrue(restored.ErrorMessage);
        restored.Data.Should().NotBeNull("the item must be found with the integer key it was stored with");
        restored.Data!["Name"]!.Value<string>().Should().Be("Seventy-five");

        var stored = await new MongoClient(_connectionString).GetDatabase(_databaseName).GetCollection<BsonDocument>(tableName)
            .Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();
        stored.Should().HaveCount(2);
        stored.Should().OnlyContain(d => d["id"].IsNumeric, "restored keys must keep the type they were backed up with");
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await new MongoClient(_connectionString).DropDatabaseAsync(_databaseName);
        }
        catch (MongoException)
        {
            // Not allowed or not reachable: the database is empty test data either way.
        }
        _databaseService.Dispose();
        await _fileService.DisposeAsync();
        await _pubSubService.DisposeAsync();
        await _memoryService.DisposeAsync();
        if (Directory.Exists(_basePath))
            Directory.Delete(_basePath, recursive: true);
        GC.SuppressFinalize(this);
    }
}
