// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Database.Basic;
using CrossCloudKit.File.Basic;
using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Memory.Basic;
using CrossCloudKit.PubSub.Basic;
using CrossCloudKit.Utilities.Common;
using FluentAssertions;
using Newtonsoft.Json.Linq;
using xRetry;
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
