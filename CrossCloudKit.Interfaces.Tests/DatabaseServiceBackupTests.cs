// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;
using CrossCloudKit.Database.Basic;
using CrossCloudKit.File.Basic;
using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Memory.Basic;
using CrossCloudKit.PubSub.Basic;
using CrossCloudKit.Utilities.Common;
using FluentAssertions;
using Newtonsoft.Json.Linq;
using xRetry;
using Xunit;
using Xunit.Abstractions;

namespace CrossCloudKit.Interfaces.Tests;

public class DatabaseServiceBackupTests : IAsyncDisposable
{
    private readonly DatabaseServiceBasic _databaseService;
    private readonly FileServiceBasic _fileService;
    private readonly MemoryServiceBasic _memoryService;
    private readonly PubSubServiceBasic _pubSubService;
    private readonly string _basePath;
    private readonly string _backupBucketName;

    private readonly ITestOutputHelper _testOutputHelper;

    public DatabaseServiceBackupTests(ITestOutputHelper testOutputHelper)
    {
        _basePath = Path.Combine(Path.GetTempPath(), $"DatabaseServiceBackupTests_{Guid.NewGuid():N}");
        _backupBucketName = "test-backup-bucket";

        // Initialize Basic services
        _memoryService = new MemoryServiceBasic(null, _basePath);
        _pubSubService = new PubSubServiceBasic(_basePath);
        _fileService = new FileServiceBasic(_memoryService, _pubSubService, _basePath);
        _databaseService = new DatabaseServiceBasic("test-db", _memoryService, _basePath);

        // Verify all services are initialized
        _databaseService.IsInitialized.Should().BeTrue();
        _fileService.IsInitialized.Should().BeTrue();
        _memoryService.IsInitialized.Should().BeTrue();
        _pubSubService.IsInitialized.Should().BeTrue();

        _testOutputHelper = testOutputHelper;
    }

    [RetryFact(3, 5000)]
    public async Task Constructor_WithValidServices_ShouldInitializeSuccessfully()
    {
        // Arrange & Act
        await using var backupService = new DatabaseServiceBackup(
            _databaseService,
            _fileService,
            _backupBucketName,
            _pubSubService,
            cronExpression: "0 0 1 1 *", // Only run once a year for test
            backupRootPath: "backups"
        );

        // Assert - No exception should be thrown, service should be created
        backupService.Should().NotBeNull();
    }

    [RetryFact(3, 5000)]
    public async Task GetBackupFileCursorsAsync_WhenNoBackupsExist_ShouldReturnEmpty()
    {
        // Arrange
        await using var backupService = CreateBackupService();
        var cursors = new List<DbBackupFileCursor>();

        // Act
        await foreach (var cursor in backupService.GetBackupFileCursorsAsync())
        {
            cursors.Add(cursor);
        }

        // Assert
        cursors.Should().BeEmpty();
    }

    [RetryFact(3, 5000)]
    public async Task GetBackupFileCursorsAsync_WithExistingBackups_ShouldReturnCursors()
    {
        // Arrange
        await using var backupService = CreateBackupService();

        // Create some test backup files
        using var stream1 = new MemoryStream(System.Text.Encoding.UTF8.GetBytes("test backup 1"));
        await _fileService.UploadFileAsync(
            new StringOrStream(stream1, stream1.Length),
            _backupBucketName,
            "backups/2023-01-01-12-00-00.json"
        );

        using var stream2 = new MemoryStream(System.Text.Encoding.UTF8.GetBytes("test backup 2"));
        await _fileService.UploadFileAsync(
            new StringOrStream(stream2, stream2.Length),
            _backupBucketName,
            "backups/2023-01-02-12-00-00.json"
        );

        var cursors = new List<DbBackupFileCursor>();

        // Act
        await foreach (var cursor in backupService.GetBackupFileCursorsAsync())
        {
            cursors.Add(cursor);
        }

        // Assert
        cursors.Should().HaveCount(2);
        cursors.Select(c => c.FileName).Should().Contain(new[]
        {
            "2023-01-01-12-00-00.json",
            "2023-01-02-12-00-00.json"
        });
    }

    [RetryFact(3, 5000)]
    public async Task RestoreBackupAsync_WithValidBackup_ShouldRestoreSuccessfully()
    {
        // Arrange
        await using var backupService = CreateBackupService();

        // Create test data in database
        var testTableName = "TestUsers";
        var testKey = new DbKey("Id", new PrimitiveType("user1"));
        var testItem = new JObject
        {
            ["Name"] = "John Doe",
            ["Email"] = "john@example.com",
            ["Age"] = 30
        };

        await _databaseService.PutItemAsync(testTableName, testKey, testItem);

        // Create backup data structure
        var backupData = new JArray
        {
            new JObject
            {
                ["table_name"] = testTableName,
                ["key_name"] = testKey.Name,
                ["items"] = new JArray
                {
                    new JObject
                    {
                        ["Id"] = "user1",
                        ["Name"] = "John Doe",
                        ["Email"] = "john@example.com",
                        ["Age"] = 30
                    }
                }
            }
        };

        var backupFileName = "test-backup.json";
        using var backupStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(backupData.ToString()));
        await _fileService.UploadFileAsync(
            new StringOrStream(backupStream, backupStream.Length),
            _backupBucketName,
            $"backups/{backupFileName}"
        );

        // Get cursor from the backup service
        var cursors = new List<DbBackupFileCursor>();
        await foreach (var cursor in backupService.GetBackupFileCursorsAsync())
        {
            if (cursor.FileName == backupFileName)
            {
                cursors.Add(cursor);
                break;
            }
        }
        cursors.Should().HaveCount(1);
        var testCursor = cursors[0];

        // Delete original data to test restore
        await _databaseService.DropTableAsync(testTableName);

        // Act
        var result = await backupService.RestoreBackupAsync(testCursor);

        // Assert
        result.Should().NotBeNull();
        result.IsSuccessful.Should().BeTrue();

        // Verify data was restored
        var restoredItem = await _databaseService.GetItemAsync(testTableName, testKey);
        restoredItem.IsSuccessful.Should().BeTrue();
        restoredItem.Data.Should().NotBeNull();
        restoredItem.Data!["Name"]!.Value<string>().Should().Be("John Doe");
        restoredItem.Data!["Email"]!.Value<string>().Should().Be("john@example.com");
        restoredItem.Data!["Age"]!.Value<int>().Should().Be(30);
    }

    [RetryFact(3, 5000)]
    public async Task RestoreBackupAsync_WithMultipleTables_ShouldRestoreAllTables()
    {
        // Arrange
        await using var backupService = CreateBackupService();

        // Create backup data with multiple tables
        var backupData = new JArray
        {
            new JObject
            {
                ["table_name"] = "Users",
                ["key_name"] = "Id",
                ["items"] = new JArray
                {
                    new JObject { ["Id"] = "user1", ["Name"] = "John" },
                    new JObject { ["Id"] = "user2", ["Name"] = "Jane" }
                }
            },
            new JObject
            {
                ["table_name"] = "Products",
                ["key_name"] = "ProductId",
                ["items"] = new JArray
                {
                    new JObject { ["ProductId"] = "prod1", ["Name"] = "Product A" },
                    new JObject { ["ProductId"] = "prod2", ["Name"] = "Product B" }
                }
            }
        };

        var backupFileName = "multi-table-backup.json";
        using var backupStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(backupData.ToString()));
        await _fileService.UploadFileAsync(
            new StringOrStream(backupStream, backupStream.Length),
            _backupBucketName,
            $"backups/{backupFileName}"
        );

        // Get cursor from the backup service
        var cursors = new List<DbBackupFileCursor>();
        await foreach (var cursor in backupService.GetBackupFileCursorsAsync())
        {
            if (cursor.FileName == backupFileName)
            {
                cursors.Add(cursor);
                break;
            }
        }
        cursors.Should().HaveCount(1);
        var testCursor = cursors[0];

        // Act
        var restoreResult = await backupService.RestoreBackupAsync(testCursor);

        // Assert
        if (!restoreResult.IsSuccessful)
            _testOutputHelper.WriteLine($"Restore failed with: {restoreResult.ErrorMessage}");
        restoreResult.IsSuccessful.Should().BeTrue();

        // Verify both tables were restored
        var user1 = await _databaseService.GetItemAsync("Users", new DbKey("Id", new PrimitiveType("user1")));
        var user2 = await _databaseService.GetItemAsync("Users", new DbKey("Id", new PrimitiveType("user2")));
        var prod1 = await _databaseService.GetItemAsync("Products", new DbKey("ProductId", new PrimitiveType("prod1")));
        var prod2 = await _databaseService.GetItemAsync("Products", new DbKey("ProductId", new PrimitiveType("prod2")));

        user1.IsSuccessful.Should().BeTrue();
        user2.IsSuccessful.Should().BeTrue();
        prod1.IsSuccessful.Should().BeTrue();
        prod2.IsSuccessful.Should().BeTrue();

        user1.Data!["Name"]!.Value<string>().Should().Be("John");
        user2.Data!["Name"]!.Value<string>().Should().Be("Jane");
        prod1.Data!["Name"]!.Value<string>().Should().Be("Product A");
        prod2.Data!["Name"]!.Value<string>().Should().Be("Product B");
    }

    [RetryFact(3, 5000)]
    public async Task RestoreBackupAsync_WithInvalidBackupData_ShouldReturnFailure()
    {
        // Arrange
        await using var backupService = CreateBackupService();

        // Create invalid backup data (missing key in items)
        var invalidBackupData = new JArray
        {
            new JObject
            {
                ["table_name"] = "Users",
                ["key_name"] = "Id",
                ["items"] = new JArray
                {
                    new JObject { ["Name"] = "John" } // Missing Id key
                }
            }
        };

        var backupFileName = "invalid-backup.json";
        using var backupStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(invalidBackupData.ToString()));
        await _fileService.UploadFileAsync(
            new StringOrStream(backupStream, backupStream.Length),
            _backupBucketName,
            $"backups/{backupFileName}"
        );

        // Get cursor from the backup service
        var cursors = new List<DbBackupFileCursor>();
        await foreach (var cursor in backupService.GetBackupFileCursorsAsync())
        {
            if (cursor.FileName == backupFileName)
            {
                cursors.Add(cursor);
                break;
            }
        }
        cursors.Should().HaveCount(1);
        var testCursor = cursors[0];

        // Act
        var result = await backupService.RestoreBackupAsync(testCursor);

        // Assert
        result.IsSuccessful.Should().BeFalse();
        result.ErrorMessage.Should().Contain("Invalid items");
    }

    [RetryFact(3, 5000)]
    public async Task RestoreBackupAsync_WithNonExistentBackup_ShouldReturnFailure()
    {
        // Arrange
        await using var backupService = CreateBackupService();

        // Create a backup file first, get its cursor, then delete the file
        var backupFileName = "temp-backup.json";
        using var backupStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes("[]"));
        await _fileService.UploadFileAsync(
            new StringOrStream(backupStream, backupStream.Length),
            _backupBucketName,
            $"backups/{backupFileName}"
        );

        // Get cursor from the backup service
        var cursors = new List<DbBackupFileCursor>();
        await foreach (var cursor in backupService.GetBackupFileCursorsAsync())
        {
            if (cursor.FileName == backupFileName)
            {
                cursors.Add(cursor);
                break;
            }
        }
        cursors.Should().HaveCount(1);
        var testCursor = cursors[0];

        // Now delete the file to make it non-existent
        await _fileService.DeleteFileAsync(_backupBucketName, $"backups/{backupFileName}");

        // Act
        var result = await backupService.RestoreBackupAsync(testCursor);

        // Assert
        result.IsSuccessful.Should().BeFalse();
    }

    [RetryFact(3, 5000)]
    public async Task RestoreBackupAsync_WithDuplicateTableNames_ShouldReturnFailure()
    {
        // Arrange
        await using var backupService = CreateBackupService();

        // Create backup data with duplicate table names
        var backupData = new JArray
        {
            new JObject
            {
                ["table_name"] = "Users",
                ["key_name"] = "Id",
                ["items"] = new JArray
                {
                    new JObject { ["Id"] = "user1", ["Name"] = "John" }
                }
            },
            new JObject
            {
                ["table_name"] = "Users", // Duplicate table name
                ["key_name"] = "Id",
                ["items"] = new JArray
                {
                    new JObject { ["Id"] = "user2", ["Name"] = "Jane" }
                }
            }
        };

        var backupFileName = "duplicate-backup.json";
        using var backupStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(backupData.ToString()));
        await _fileService.UploadFileAsync(
            new StringOrStream(backupStream, backupStream.Length),
            _backupBucketName,
            $"backups/{backupFileName}"
        );

        // Get cursor from the backup service
        var cursors = new List<DbBackupFileCursor>();
        await foreach (var cursor in backupService.GetBackupFileCursorsAsync())
        {
            if (cursor.FileName == backupFileName)
            {
                cursors.Add(cursor);
                break;
            }
        }
        cursors.Should().HaveCount(1);
        var testCursor = cursors[0];

        // Act
        var result = await backupService.RestoreBackupAsync(testCursor);

        // Assert
        result.IsSuccessful.Should().BeFalse();
        result.ErrorMessage.Should().Contain("duplicate detected");
    }

    [RetryFact(3, 5000)]
    public async Task RestoreBackupAsync_OverwritesExistingData_ShouldSucceed()
    {
        // Arrange
        await using var backupService = CreateBackupService();

        // Create existing data
        var tableName = "Users";
        var key = new DbKey("Id", new PrimitiveType("user1"));
        var existingItem = new JObject
        {
            ["Name"] = "Original Name",
            ["Email"] = "original@example.com"
        };
        await _databaseService.PutItemAsync(tableName, key, existingItem);

        // Create backup data with different values
        var backupData = new JArray
        {
            new JObject
            {
                ["table_name"] = tableName,
                ["key_name"] = "Id",
                ["items"] = new JArray
                {
                    new JObject
                    {
                        ["Id"] = "user1",
                        ["Name"] = "Backup Name",
                        ["Email"] = "backup@example.com"
                    }
                }
            }
        };

        var backupFileName = "overwrite-backup.json";
        using var backupStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(backupData.ToString()));
        await _fileService.UploadFileAsync(
            new StringOrStream(backupStream, backupStream.Length),
            _backupBucketName,
            $"backups/{backupFileName}"
        );

        // Get cursor from the backup service
        var cursors = new List<DbBackupFileCursor>();
        await foreach (var cursor in backupService.GetBackupFileCursorsAsync())
        {
            if (cursor.FileName == backupFileName)
            {
                cursors.Add(cursor);
                break;
            }
        }
        cursors.Should().HaveCount(1);
        var testCursor = cursors[0];

        // Act
        var result = await backupService.RestoreBackupAsync(testCursor);

        // Assert
        result.IsSuccessful.Should().BeTrue();

        // Verify data was overwritten
        var restoredItem = await _databaseService.GetItemAsync(tableName, key);
        restoredItem.IsSuccessful.Should().BeTrue();
        restoredItem.Data!["Name"]!.Value<string>().Should().Be("Backup Name");
        restoredItem.Data!["Email"]!.Value<string>().Should().Be("backup@example.com");
    }

    [RetryFact(3, 5000)]
    public async Task Constructor_WithCronExpression_ShouldAcceptValidCronExpressions()
    {
        // Arrange & Act
        await using var dailyBackup = new DatabaseServiceBackup(
            _databaseService,
            _fileService,
            _backupBucketName,
            _pubSubService,
            cronExpression: "0 1 * * *" // Daily at 1 AM
        );

        await using var weeklyBackup = new DatabaseServiceBackup(
            _databaseService,
            _fileService,
            _backupBucketName + "2",
            _pubSubService,
            cronExpression: "0 0 * * 0" // Weekly on Sunday
        );

        // Assert - No exceptions should be thrown
        dailyBackup.Should().NotBeNull();
        weeklyBackup.Should().NotBeNull();
    }

    [RetryFact(3, 5000)]
    public void Constructor_WithInvalidCronExpression_ShouldThrowException()
    {
        // Act & Assert
        var action = () => new DatabaseServiceBackup(
            _databaseService,
            _fileService,
            _backupBucketName,
            _pubSubService,
            cronExpression: "invalid cron"
        );

        action.Should().Throw<Exception>()
            .Where(ex => ex.GetType().Name.Contains("Cron") || ex.Message.Contains("cron") || ex.Message.Contains("invalid"));
    }

    [RetryFact(3, 5000)]
    public async Task Constructor_WithCustomBackupRootPath_ShouldUseCorrectPath()
    {
        // Arrange
        var customPath = "custom/backup/path";
        await using var backupService = new DatabaseServiceBackup(
            _databaseService,
            _fileService,
            _backupBucketName,
            _pubSubService,
            cronExpression: "0 0 1 1 *",
            backupRootPath: customPath
        );

        // Create a test backup file under the custom path
        using var testStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes("test backup"));
        await _fileService.UploadFileAsync(
            new StringOrStream(testStream, testStream.Length),
            _backupBucketName,
            $"{customPath}/test-backup.json"
        );

        var cursors = new List<DbBackupFileCursor>();

        // Act
        await foreach (var cursor in backupService.GetBackupFileCursorsAsync())
        {
            cursors.Add(cursor);
        }

        // Assert
        cursors.Should().HaveCount(1);
        cursors[0].FileName.Should().Be("test-backup.json");
    }

    [RetryFact(3, 5000)]
    public async Task RestoreBackupAsync_WithEmptyItems_ShouldSucceed()
    {
        // Arrange
        await using var backupService = CreateBackupService();

        // Create backup data with empty items array
        var backupData = new JArray
        {
            new JObject
            {
                ["table_name"] = "EmptyTable",
                ["key_name"] = "Id",
                ["items"] = new JArray() // Empty items
            }
        };

        var backupFileName = "empty-backup.json";
        using var backupStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(backupData.ToString()));
        await _fileService.UploadFileAsync(
            new StringOrStream(backupStream, backupStream.Length),
            _backupBucketName,
            $"backups/{backupFileName}"
        );

        // Get cursor from the backup service
        var cursors = new List<DbBackupFileCursor>();
        await foreach (var cursor in backupService.GetBackupFileCursorsAsync())
        {
            if (cursor.FileName == backupFileName)
            {
                cursors.Add(cursor);
                break;
            }
        }
        cursors.Should().HaveCount(1);
        var testCursor = cursors[0];

        // Act
        var result = await backupService.RestoreBackupAsync(testCursor);

        // Assert
        result.IsSuccessful.Should().BeTrue();

        // Verify table exists but is empty
        var scanResult = await _databaseService.ScanTableAsync("EmptyTable");
        scanResult.IsSuccessful.Should().BeTrue();
        scanResult.Data.Items.Should().BeEmpty();
    }

    [RetryFact(3, 5000)]
    public async Task GetBackupFileCursorsAsync_WithPagination_ShouldReturnPagedResults()
    {
        // Arrange
        await using var backupService = CreateBackupService();

        // Create multiple backup files
        for (int i = 1; i <= 15; i++)
        {
            using var backupStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes($"backup {i}"));
            await _fileService.UploadFileAsync(
                new StringOrStream(backupStream, backupStream.Length),
                _backupBucketName,
                $"backups/backup-{i:D2}.json"
            );
        }

        var cursors = new List<DbBackupFileCursor>();

        // Act - Use small page size to test pagination
        await foreach (var cursor in backupService.GetBackupFileCursorsAsync(pageSize: 5))
        {
            cursors.Add(cursor);
        }

        // Assert
        cursors.Should().HaveCount(15);
        cursors.Select(c => c.FileName).Should().Contain("backup-01.json");
        cursors.Select(c => c.FileName).Should().Contain("backup-15.json");
    }

    [RetryFact(3, 5000)]
    public async Task TakeBackup_ThenRestore_WithMultipleTables_ShouldPreserveAllData()
    {
        // Arrange
        await using var backupService = CreateBackupService();

        // Create test data in multiple tables
        var usersTable = "Users";
        var productsTable = "Products";
        var ordersTable = "Orders";

        // Users table data
        var user1Key = new DbKey("Id", new PrimitiveType("user1"));
        var user1Data = new JObject
        {
            ["Id"] = "user1",
            ["Name"] = "John Doe",
            ["Email"] = "john@example.com",
            ["Age"] = 30,
            ["IsActive"] = true
        };

        var user2Key = new DbKey("Id", new PrimitiveType("user2"));
        var user2Data = new JObject
        {
            ["Id"] = "user2",
            ["Name"] = "Jane Smith",
            ["Email"] = "jane@example.com",
            ["Age"] = 25,
            ["IsActive"] = false
        };

        // Products table data
        var product1Key = new DbKey("ProductId", new PrimitiveType("prod1"));
        var product1Data = new JObject
        {
            ["ProductId"] = "prod1",
            ["Name"] = "Laptop",
            ["Price"] = 999.99,
            ["Category"] = "Electronics",
            ["InStock"] = true
        };

        var product2Key = new DbKey("ProductId", new PrimitiveType("prod2"));
        var product2Data = new JObject
        {
            ["ProductId"] = "prod2",
            ["Name"] = "Mouse",
            ["Price"] = 25.50,
            ["Category"] = "Accessories",
            ["InStock"] = false
        };

        // Orders table data
        var order1Key = new DbKey("OrderId", new PrimitiveType("order1"));
        var order1Data = new JObject
        {
            ["OrderId"] = "order1",
            ["UserId"] = "user1",
            ["ProductId"] = "prod1",
            ["Quantity"] = 1,
            ["Total"] = 999.99
        };

        // Insert all test data
        await _databaseService.PutItemAsync(usersTable, user1Key, user1Data);
        await _databaseService.PutItemAsync(usersTable, user2Key, user2Data);
        await _databaseService.PutItemAsync(productsTable, product1Key, product1Data);
        await _databaseService.PutItemAsync(productsTable, product2Key, product2Data);
        await _databaseService.PutItemAsync(ordersTable, order1Key, order1Data);

        // Act - Take backup
        var backupResult = await backupService.TakeBackup();

        // Assert - Backup should succeed and return cursor
        backupResult.IsSuccessful.Should().BeTrue();
        backupResult.Data.Should().NotBeNull();
        var backupCursor = backupResult.Data!;
        backupCursor.FileName.Should().NotBeEmpty();

        // Verify backup file exists
        var cursors = new List<DbBackupFileCursor>();
        await foreach (var cursor in backupService.GetBackupFileCursorsAsync())
        {
            cursors.Add(cursor);
        }
        cursors.Should().Contain(c => c.FileName == backupCursor.FileName);

        // Clear database to test restore
        await _databaseService.DropTableAsync(usersTable);
        await _databaseService.DropTableAsync(productsTable);
        await _databaseService.DropTableAsync(ordersTable);

        // Verify tables are gone
        var user1BeforeRestore = await _databaseService.GetItemAsync(usersTable, user1Key);
        user1BeforeRestore.IsSuccessful.Should().BeTrue();
        user1BeforeRestore.Data.Should().BeNull();

        // Act - Restore from backup
        var restoreResult = await backupService.RestoreBackupAsync(backupCursor);

        // Assert - Restore should succeed
        if (!restoreResult.IsSuccessful)
            _testOutputHelper.WriteLine($"Restore failed with: {restoreResult.ErrorMessage}");
        restoreResult.IsSuccessful.Should().BeTrue();

        // Verify all data was restored correctly
        // Users table
        var restoredUser1 = await _databaseService.GetItemAsync(usersTable, user1Key);
        var restoredUser2 = await _databaseService.GetItemAsync(usersTable, user2Key);

        restoredUser1.IsSuccessful.Should().BeTrue();
        restoredUser2.IsSuccessful.Should().BeTrue();

        restoredUser1.Data!["Name"]!.Value<string>().Should().Be("John Doe");
        restoredUser1.Data!["Email"]!.Value<string>().Should().Be("john@example.com");
        restoredUser1.Data!["Age"]!.Value<int>().Should().Be(30);
        restoredUser1.Data!["IsActive"]!.Value<bool>().Should().BeTrue();

        restoredUser2.Data!["Name"]!.Value<string>().Should().Be("Jane Smith");
        restoredUser2.Data!["Email"]!.Value<string>().Should().Be("jane@example.com");
        restoredUser2.Data!["Age"]!.Value<int>().Should().Be(25);
        restoredUser2.Data!["IsActive"]!.Value<bool>().Should().BeFalse();

        // Products table
        var restoredProduct1 = await _databaseService.GetItemAsync(productsTable, product1Key);
        var restoredProduct2 = await _databaseService.GetItemAsync(productsTable, product2Key);

        restoredProduct1.IsSuccessful.Should().BeTrue();
        restoredProduct2.IsSuccessful.Should().BeTrue();

        restoredProduct1.Data!["Name"]!.Value<string>().Should().Be("Laptop");
        restoredProduct1.Data!["Price"]!.Value<double>().Should().Be(999.99);
        restoredProduct1.Data!["Category"]!.Value<string>().Should().Be("Electronics");
        restoredProduct1.Data!["InStock"]!.Value<bool>().Should().BeTrue();

        restoredProduct2.Data!["Name"]!.Value<string>().Should().Be("Mouse");
        restoredProduct2.Data!["Price"]!.Value<double>().Should().Be(25.50);
        restoredProduct2.Data!["Category"]!.Value<string>().Should().Be("Accessories");
        restoredProduct2.Data!["InStock"]!.Value<bool>().Should().BeFalse();

        // Orders table
        var restoredOrder1 = await _databaseService.GetItemAsync(ordersTable, order1Key);

        restoredOrder1.IsSuccessful.Should().BeTrue();
        restoredOrder1.Data!["UserId"]!.Value<string>().Should().Be("user1");
        restoredOrder1.Data!["ProductId"]!.Value<string>().Should().Be("prod1");
        restoredOrder1.Data!["Quantity"]!.Value<int>().Should().Be(1);
        restoredOrder1.Data!["Total"]!.Value<double>().Should().Be(999.99);

        // Verify all tables have correct item counts
        var usersCount = await _databaseService.ScanTableAsync(usersTable);
        var productsCount = await _databaseService.ScanTableAsync(productsTable);
        var ordersCount = await _databaseService.ScanTableAsync(ordersTable);

        usersCount.IsSuccessful.Should().BeTrue();
        productsCount.IsSuccessful.Should().BeTrue();
        ordersCount.IsSuccessful.Should().BeTrue();

        usersCount.Data.Items.Should().HaveCount(2);
        productsCount.Data.Items.Should().HaveCount(2);
        ordersCount.Data.Items.Should().HaveCount(1);
    }

    [RetryFact(3, 5000)]
    public async Task TakeBackup_WithEmptyDatabase_ShouldReturnNull()
    {
        // Arrange
        await using var backupService = CreateBackupService();

        // Ensure database is empty - drop any existing tables
        var existingTables = await _databaseService.GetTableNamesAsync();
        if (existingTables.IsSuccessful && existingTables.Data.Count > 0)
        {
            foreach (var tableName in existingTables.Data)
            {
                await _databaseService.DropTableAsync(tableName);
            }
        }

        // Act
        var backupResult = await backupService.TakeBackup();

        // Assert
        backupResult.IsSuccessful.Should().BeTrue();
        backupResult.Data.Should().BeNull();
    }

    [RetryFact(3, 5000)]
    public async Task TakeBackup_WithEmptyTables_ShouldReturnNull()
    {
        // Arrange
        await using var backupService = CreateBackupService();

        // Create tables but don't add any data
        var testTable = "EmptyTable";
        var testKey = new DbKey("Id", new PrimitiveType("test"));
        var testData = new JObject { ["Id"] = "test", ["Name"] = "Test" };

        // Insert and then delete to create empty table
        await _databaseService.PutItemAsync(testTable, testKey, testData);
        await _databaseService.DeleteItemAsync(testTable, testKey);

        // Act
        var backupResult = await backupService.TakeBackup();

        // Assert
        backupResult.IsSuccessful.Should().BeTrue();
        backupResult.Data.Should().BeNull();
    }

    private DatabaseServiceBackup CreateBackupService(string cronExpression = "0 0 1 1 *") // Once a year to avoid automatic execution during tests
    {
        return new DatabaseServiceBackup(
            _databaseService,
            _fileService,
            _backupBucketName,
            _pubSubService,
            cronExpression: cronExpression,
            backupRootPath: "backups"
        );
    }

    [RetryFact(3, 5000)]
    public async Task TakeBackup_WithDropTablesAfterBackup_ShouldDropTablesAfterSuccessfulBackup()
    {
        // Arrange
        await using var backupService = CreateBackupService();

        var tableName = "TestTable";
        var key = new DbKey("Id", new PrimitiveType("test1"));
        var data = new JObject
        {
            ["Id"] = "test1",
            ["Name"] = "Test Data",
            ["Value"] = 42
        };

        await _databaseService.PutItemAsync(tableName, key, data);

        // Verify data exists before backup
        var beforeBackup = await _databaseService.GetItemAsync(tableName, key);
        beforeBackup.IsSuccessful.Should().BeTrue();
        beforeBackup.Data.Should().NotBeNull();

        // Act - Take backup with cleanup
        var backupResult = await backupService.TakeBackup(dropTablesAfterBackup: true);

        // Assert
        backupResult.IsSuccessful.Should().BeTrue();
        backupResult.Data.Should().NotBeNull();

        // Verify table was dropped after backup
        var afterBackup = await _databaseService.GetItemAsync(tableName, key);
        afterBackup.IsSuccessful.Should().BeTrue();
        afterBackup.Data.Should().BeNull();

        // Verify backup file was created and contains data
        var cursors = new List<DbBackupFileCursor>();
        await foreach (var cursor in backupService.GetBackupFileCursorsAsync())
        {
            if (cursor.FileName == backupResult.Data!.FileName)
            {
                cursors.Add(cursor);
                break;
            }
        }
        cursors.Should().HaveCount(1);
    }

    [RetryFact(3, 5000)]
    public async Task RestoreBackupAsync_WithFullCleanUpBeforeRestoration_ShouldDropAllExistingTables()
    {
        // Arrange
        await using var backupService = CreateBackupService();

        // Create existing data in multiple tables
        var existingTable1 = "ExistingTable1";
        var existingTable2 = "ExistingTable2";

        await _databaseService.PutItemAsync(existingTable1, new DbKey("Id", new PrimitiveType("existing1")), new JObject { ["Id"] = "existing1", ["Data"] = "old" });
        await _databaseService.PutItemAsync(existingTable2, new DbKey("Id", new PrimitiveType("existing2")), new JObject { ["Id"] = "existing2", ["Data"] = "old" });

        // Create backup data with different table
        var backupData = new JArray
        {
            new JObject
            {
                ["table_name"] = "NewTable",
                ["key_name"] = "Id",
                ["items"] = new JArray
                {
                    new JObject { ["Id"] = "new1", ["Data"] = "new" }
                }
            }
        };

        var backupFileName = "cleanup-test-backup.json";
        using var backupStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(backupData.ToString()));
        await _fileService.UploadFileAsync(
            new StringOrStream(backupStream, backupStream.Length),
            _backupBucketName,
            $"backups/{backupFileName}"
        );

        var cursors = new List<DbBackupFileCursor>();
        await foreach (var cursor in backupService.GetBackupFileCursorsAsync())
        {
            if (cursor.FileName == backupFileName)
            {
                cursors.Add(cursor);
                break;
            }
        }
        cursors.Should().HaveCount(1);
        var testCursor = cursors[0];

        // Act - Restore with full cleanup
        var result = await backupService.RestoreBackupAsync(testCursor, fullCleanUpBeforeRestoration: true);

        // Assert
        result.IsSuccessful.Should().BeTrue();

        // Verify existing tables were dropped
        var existing1After = await _databaseService.GetItemAsync(existingTable1, new DbKey("Id", new PrimitiveType("existing1")));
        var existing2After = await _databaseService.GetItemAsync(existingTable2, new DbKey("Id", new PrimitiveType("existing2")));

        existing1After.IsSuccessful.Should().BeTrue();
        existing2After.IsSuccessful.Should().BeTrue();
        existing1After.Data.Should().BeNull();
        existing2After.Data.Should().BeNull();

        // Verify new table was created
        var newItemAfter = await _databaseService.GetItemAsync("NewTable", new DbKey("Id", new PrimitiveType("new1")));
        newItemAfter.IsSuccessful.Should().BeTrue();
        newItemAfter.Data.Should().NotBeNull();
        newItemAfter.Data!["Data"]!.Value<string>().Should().Be("new");
    }

    [RetryFact(3, 5000)]
    public async Task DatabaseServiceMigration_Migrate_ShouldTransferDataBetweenDatabases()
    {
        // Arrange - Create destination database
        var destinationBasePath = Path.Combine(Path.GetTempPath(), $"DatabaseServiceBackupTests_Dest_{Guid.NewGuid():N}");
        var destMemoryService = new MemoryServiceBasic(null, destinationBasePath);
        var destPubSubService = new PubSubServiceBasic(destinationBasePath);
        var destFileService = new FileServiceBasic(destMemoryService, destPubSubService, destinationBasePath);
        var destDatabaseService = new DatabaseServiceBasic("dest-db", destMemoryService, destinationBasePath);

        try
        {
            // Verify destination services are initialized
            destDatabaseService.IsInitialized.Should().BeTrue();
            destFileService.IsInitialized.Should().BeTrue();
            destMemoryService.IsInitialized.Should().BeTrue();
            destPubSubService.IsInitialized.Should().BeTrue();

            // Create test data in source database
            var tableName = "MigrationTest";
            var key1 = new DbKey("Id", new PrimitiveType("migrate1"));
            var key2 = new DbKey("Id", new PrimitiveType("migrate2"));
            var data1 = new JObject { ["Id"] = "migrate1", ["Name"] = "Source Item 1", ["Value"] = 100 };
            var data2 = new JObject { ["Id"] = "migrate2", ["Name"] = "Source Item 2", ["Value"] = 200 };

            await _databaseService.PutItemAsync(tableName, key1, data1);
            await _databaseService.PutItemAsync(tableName, key2, data2);

            var migrationBucketName = "migration-work-bucket";

            // Act - Perform migration
            var migrationResult = await DatabaseServiceMigration.MigrateAsync(
                _databaseService,
                destDatabaseService,
                _fileService,
                _pubSubService,
                migrationBucketName
            );

            // Assert
            migrationResult.IsSuccessful.Should().BeTrue();

            // Verify data exists in destination database
            var destItem1 = await destDatabaseService.GetItemAsync(tableName, key1);
            var destItem2 = await destDatabaseService.GetItemAsync(tableName, key2);

            destItem1.IsSuccessful.Should().BeTrue();
            destItem2.IsSuccessful.Should().BeTrue();

            destItem1.Data!["Name"]!.Value<string>().Should().Be("Source Item 1");
            destItem1.Data!["Value"]!.Value<int>().Should().Be(100);
            destItem2.Data!["Name"]!.Value<string>().Should().Be("Source Item 2");
            destItem2.Data!["Value"]!.Value<int>().Should().Be(200);

            // Verify data still exists in source database (since cleanUpSourceDatabaseAfterMigrate = false)
            var sourceItem1 = await _databaseService.GetItemAsync(tableName, key1);
            var sourceItem2 = await _databaseService.GetItemAsync(tableName, key2);

            sourceItem1.IsSuccessful.Should().BeTrue();
            sourceItem2.IsSuccessful.Should().BeTrue();
            sourceItem1.Data.Should().NotBeNull();
            sourceItem2.Data.Should().NotBeNull();
        }
        finally
        {
            // Cleanup destination services
            await destFileService.DisposeAsync();
            await destMemoryService.DisposeAsync();
            await destPubSubService.DisposeAsync();
            destDatabaseService.Dispose();

            if (Directory.Exists(destinationBasePath))
            {
                await Task.Delay(100);
                Directory.Delete(destinationBasePath, recursive: true);
            }
        }
    }

    [RetryFact(3, 5000)]
    public async Task DatabaseServiceMigration_Migrate_WithCleanUpSource_ShouldDeleteSourceAfterMigration()
    {
        // Arrange - Create destination database
        var destinationBasePath = Path.Combine(Path.GetTempPath(), $"DatabaseServiceBackupTests_Dest_{Guid.NewGuid():N}");
        var destMemoryService = new MemoryServiceBasic(null, destinationBasePath);
        var destPubSubService = new PubSubServiceBasic(destinationBasePath);
        var destFileService = new FileServiceBasic(destMemoryService, destPubSubService, destinationBasePath);
        var destDatabaseService = new DatabaseServiceBasic("dest-db", destMemoryService, destinationBasePath);

        try
        {
            // Create test data in source database
            var tableName = "MigrationCleanupTest";
            var key = new DbKey("Id", new PrimitiveType("cleanup1"));
            var data = new JObject { ["Id"] = "cleanup1", ["Name"] = "Test Item", ["Value"] = 42 };

            await _databaseService.PutItemAsync(tableName, key, data);

            var migrationBucketName = "migration-cleanup-bucket";

            // Act - Perform migration with source cleanup
            var migrationResult = await DatabaseServiceMigration.MigrateAsync(
                _databaseService,
                destDatabaseService,
                _fileService,
                _pubSubService,
                migrationBucketName,
                cleanUpSourceDatabaseAfterMigrate: true
            );

            // Assert
            migrationResult.IsSuccessful.Should().BeTrue();

            // Verify data exists in destination database
            var destItem = await destDatabaseService.GetItemAsync(tableName, key);
            destItem.IsSuccessful.Should().BeTrue();
            destItem.Data!["Name"]!.Value<string>().Should().Be("Test Item");

            // Verify data was deleted from source database
            var sourceItem = await _databaseService.GetItemAsync(tableName, key);
            sourceItem.IsSuccessful.Should().BeTrue();
            sourceItem.Data.Should().BeNull();
        }
        finally
        {
            // Cleanup destination services
            await destFileService.DisposeAsync();
            await destMemoryService.DisposeAsync();
            await destPubSubService.DisposeAsync();
            destDatabaseService.Dispose();

            if (Directory.Exists(destinationBasePath))
            {
                await Task.Delay(100);
                Directory.Delete(destinationBasePath, recursive: true);
            }
        }
    }

    [RetryFact(3, 5000)]
    public async Task DatabaseServiceMigration_Migrate_WithEmptySource_ShouldReturnNotFound()
    {
        // Arrange - Create destination database
        var destinationBasePath = Path.Combine(Path.GetTempPath(), $"DatabaseServiceBackupTests_Dest_{Guid.NewGuid():N}");
        var destMemoryService = new MemoryServiceBasic(null, destinationBasePath);
        var destPubSubService = new PubSubServiceBasic(destinationBasePath);
        var destFileService = new FileServiceBasic(destMemoryService, destPubSubService, destinationBasePath);
        var destDatabaseService = new DatabaseServiceBasic("dest-db", destMemoryService, destinationBasePath);

        try
        {
            // Ensure source database is empty
            var existingTables = await _databaseService.GetTableNamesAsync();
            if (existingTables.IsSuccessful && existingTables.Data.Count > 0)
            {
                foreach (var tableName in existingTables.Data)
                {
                    await _databaseService.DropTableAsync(tableName);
                }
            }

            var migrationBucketName = "migration-empty-bucket";

            // Act - Attempt migration with empty source
            var migrationResult = await DatabaseServiceMigration.MigrateAsync(
                _databaseService,
                destDatabaseService,
                _fileService,
                _pubSubService,
                migrationBucketName
            );

            // Assert
            migrationResult.IsSuccessful.Should().BeFalse();
            migrationResult.StatusCode.Should().Be(HttpStatusCode.NotFound);
            migrationResult.ErrorMessage.Should().Contain("No data found to migrate");
        }
        finally
        {
            // Cleanup destination services
            await destFileService.DisposeAsync();
            await destMemoryService.DisposeAsync();
            await destPubSubService.DisposeAsync();
            destDatabaseService.Dispose();

            if (Directory.Exists(destinationBasePath))
            {
                await Task.Delay(100);
                Directory.Delete(destinationBasePath, recursive: true);
            }
        }
    }

    [RetryFact(3, 5000)]
    public async Task ManualBackupConstructor_ShouldNotStartBackgroundTask()
    {
        // Arrange & Act - Create backup service without cron expression (manual mode)
        await using var backupService = new DatabaseServiceBackup(
            _databaseService,
            _fileService,
            _pubSubService,
            _backupBucketName,
            "manual-backups"
        );

        // Assert - Service should be created without background tasks
        backupService.Should().NotBeNull();

        // Verify manual backup works
        var tableName = "ManualTest";
        var key = new DbKey("Id", new PrimitiveType("manual1"));
        var data = new JObject { ["Id"] = "manual1", ["Data"] = "Manual backup test" };

        await _databaseService.PutItemAsync(tableName, key, data);

        var backupResult = await backupService.TakeBackup();
        backupResult.IsSuccessful.Should().BeTrue();
        backupResult.Data.Should().NotBeNull();
    }

    [RetryFact(3, 5000)]
    public async Task TakeBackup_ConcurrentCalls_ShouldBeSafeWithMutex()
    {
        // Arrange
        await using var backupService = CreateBackupService();

        // Create test data
        var tableName = "ConcurrentTest";
        for (int i = 1; i <= 5; i++)
        {
            var key = new DbKey("Id", new PrimitiveType($"item{i}"));
            var data = new JObject { ["Id"] = $"item{i}", ["Data"] = $"Item {i}" };
            await _databaseService.PutItemAsync(tableName, key, data);
        }

        // Act - Make concurrent backup calls
        var tasks = new List<Task<OperationResult<DbBackupFileCursor?>>>();
        for (int i = 0; i < 3; i++)
        {
            tasks.Add(backupService.TakeBackup());
        }

        var results = await Task.WhenAll(tasks);

        // Assert - All operations should complete, but due to mutex, some might fail or succeed
        results.Should().HaveCount(3);

        // At least one should succeed
        var successfulResults = results.Where(r => r.IsSuccessful).ToList();
        successfulResults.Should().NotBeEmpty();

        // Successful results should have backup cursors
        foreach (var result in successfulResults)
        {
            result.Data.Should().NotBeNull();
            result.Data!.FileName.Should().NotBeEmpty();
        }
    }

    [RetryFact(3, 5000)]
    public async Task RestoreBackupAsync_WithMalformedJson_ShouldReturnFailure()
    {
        // Arrange
        await using var backupService = CreateBackupService();

        var backupFileName = "malformed-backup.json";
        var malformedJson = "{ invalid json structure without proper closing";

        using var backupStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(malformedJson));
        await _fileService.UploadFileAsync(
            new StringOrStream(backupStream, backupStream.Length),
            _backupBucketName,
            $"backups/{backupFileName}"
        );

        var cursors = new List<DbBackupFileCursor>();
        await foreach (var cursor in backupService.GetBackupFileCursorsAsync())
        {
            if (cursor.FileName == backupFileName)
            {
                cursors.Add(cursor);
                break;
            }
        }
        cursors.Should().HaveCount(1);
        var testCursor = cursors[0];

        // Act
        var result = await backupService.RestoreBackupAsync(testCursor);

        // Assert
        result.IsSuccessful.Should().BeFalse();
        result.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
    }

    [RetryFact(3, 5000)]
    public async Task TakeBackup_WithLargeDataset_ShouldHandleEfficiently()
    {
        // Arrange
        await using var backupService = CreateBackupService();

        var tableName = "LargeDataTest";
        var itemCount = 100; // Reasonable size for test

        // Create large dataset
        var insertTasks = new List<Task>();
        for (int i = 1; i <= itemCount; i++)
        {
            var key = new DbKey("Id", new PrimitiveType($"item{i:D3}"));
            var data = new JObject
            {
                ["Id"] = $"item{i:D3}",
                ["Name"] = $"Item Number {i}",
                ["Description"] = $"This is a detailed description for item {i} which contains enough text to make the backup file reasonably sized.",
                ["Value"] = i * 10,
                ["Category"] = $"Category {i % 5}",
                ["IsActive"] = i % 2 == 0,
                ["Timestamp"] = DateTimeOffset.Now.ToString()
            };
            insertTasks.Add(_databaseService.PutItemAsync(tableName, key, data));
        }

        await Task.WhenAll(insertTasks);

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Act
        var backupResult = await backupService.TakeBackup();

        stopwatch.Stop();

        // Assert
        backupResult.IsSuccessful.Should().BeTrue();
        backupResult.Data.Should().NotBeNull();

        _testOutputHelper.WriteLine($"Backup of {itemCount} items took {stopwatch.ElapsedMilliseconds}ms");

        // Verify all data can be restored
        await _databaseService.DropTableAsync(tableName);

        var restoreResult = await backupService.RestoreBackupAsync(backupResult.Data!);
        restoreResult.IsSuccessful.Should().BeTrue();

        // Verify count of restored items
        var scanResult = await _databaseService.ScanTableAsync(tableName);
        scanResult.IsSuccessful.Should().BeTrue();
        scanResult.Data.Items.Should().HaveCount(itemCount);
    }

    [RetryFact(3, 5000)]
    public async Task GetBackupFileCursorsAsync_WithCancellation_ShouldRespectCancellationToken()
    {
        // Arrange
        await using var backupService = CreateBackupService();

        // Create some backup files
        for (int i = 1; i <= 10; i++)
        {
            using var backupStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes($"backup {i}"));
            await _fileService.UploadFileAsync(
                new StringOrStream(backupStream, backupStream.Length),
                _backupBucketName,
                $"backups/backup-{i:D2}.json"
            );
        }

        using var cts = new CancellationTokenSource();
        var cursors = new List<DbBackupFileCursor>();

        // Act - Cancel after a short delay
        var enumerationTask = Task.Run(async () =>
        {
            await foreach (var cursor in backupService.GetBackupFileCursorsAsync(pageSize: 2, cancellationToken: cts.Token))
            {
                cursors.Add(cursor);
                await Task.Delay(10, cts.Token); // Small delay to allow cancellation
            }
        });

        await Task.Delay(50); // Let it start
        await cts.CancelAsync();

        // Assert - Should handle cancellation gracefully
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => enumerationTask);
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            // Dispose services first
            await _fileService.DisposeAsync();
            await _memoryService.DisposeAsync();
            await _pubSubService.DisposeAsync();
            _databaseService.Dispose();
        }
        catch (Exception)
        {
            // Ignore disposal errors
        }

        try
        {
            // Clean up test data after disposing services
            if (Directory.Exists(_basePath))
            {
                // Give services time to cleanup before deleting directory
                await Task.Delay(100);

                Directory.Delete(_basePath, recursive: true);
            }
        }
        catch (Exception)
        {
            // Ignore cleanup errors
        }
    }
}
