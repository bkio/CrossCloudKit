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
using Xunit;

namespace CrossCloudKit.Interfaces.Tests;

public class DatabaseServiceBackupTests : IAsyncDisposable
{
    private readonly DatabaseServiceBasic _databaseService;
    private readonly FileServiceBasic _fileService;
    private readonly MemoryServiceBasic _memoryService;
    private readonly PubSubServiceBasic _pubSubService;
    private readonly string _basePath;
    private readonly string _backupBucketName;

    public DatabaseServiceBackupTests()
    {
        _basePath = Path.Combine(Path.GetTempPath(), $"DatabaseServiceBackupTests_{Guid.NewGuid():N}");
        _backupBucketName = "test-backup-bucket";

        // Initialize Basic services
        _memoryService = new MemoryServiceBasic();
        _pubSubService = new PubSubServiceBasic();
        _fileService = new FileServiceBasic(_memoryService, _pubSubService, _basePath);
        _databaseService = new DatabaseServiceBasic("test-db", _memoryService, _basePath);

        // Verify all services are initialized
        _databaseService.IsInitialized.Should().BeTrue();
        _fileService.IsInitialized.Should().BeTrue();
        _memoryService.IsInitialized.Should().BeTrue();
        _pubSubService.IsInitialized.Should().BeTrue();
    }

    [Fact]
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

    [Fact]
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

    [Fact]
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

    [Fact]
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

    [Fact]
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
        var result = await backupService.RestoreBackupAsync(testCursor);

        // Assert
        result.IsSuccessful.Should().BeTrue();

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

    [Fact]
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

    [Fact]
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

    [Fact]
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

    [Fact]
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

    [Fact]
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

    [Fact]
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

    [Fact]
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

    [Fact]
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

    [Fact]
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

    [Fact]
    public async Task CronBasedBackup_ShouldCreateBackupAndAllowRestore()
    {
        // Arrange - Get current time and calculate cron expression for 1 minute later
        var now = DateTime.Now;
        var backupTime = now.AddMinutes(1);
        var cronExpression = $"{backupTime.Second} {backupTime.Minute} {backupTime.Hour} {backupTime.Day} {backupTime.Month} *";

        // Create test data before backup runs
        var testTableName = "CronBackupTest";
        var testKey1 = new DbKey("Id", new PrimitiveType("user1"));
        var testKey2 = new DbKey("Id", new PrimitiveType("user2"));
        var testItem1 = new JObject
        {
            ["Name"] = "John Doe",
            ["Email"] = "john@example.com",
            ["Age"] = 30
        };
        var testItem2 = new JObject
        {
            ["Name"] = "Jane Smith",
            ["Email"] = "jane@example.com",
            ["Age"] = 25
        };

        await _databaseService.PutItemAsync(testTableName, testKey1, testItem1);
        await _databaseService.PutItemAsync(testTableName, testKey2, testItem2);

        // Create backup service with cron expression for 1 minute from now
        await using var backupService = new DatabaseServiceBackup(
            _databaseService,
            _fileService,
            _backupBucketName,
            _pubSubService,
            cronExpression: cronExpression,
            backupRootPath: "backups"
        );

        // Wait for backup to be created (wait up to 2 minutes for backup to complete)
        var timeout = DateTime.Now.AddMinutes(2);
        var backupFound = false;
        DbBackupFileCursor? createdBackupCursor = null;

        while (DateTime.Now < timeout && !backupFound)
        {
            await Task.Delay(5000); // Check every 5 seconds

            var cursors = new List<DbBackupFileCursor>();
            await foreach (var cursor in backupService.GetBackupFileCursorsAsync())
            {
                cursors.Add(cursor);
            }

            // Look for a backup file created around the expected time
            var expectedTimeWindow = backupTime.AddMinutes(-1); // 1 minute window
            foreach (var cursor in cursors)
            {
                // Backup file name format: yyyy-MM-dd-HH-mm-ss.json
                if (DateTime.TryParseExact(cursor.FileName.Replace(".json", ""), "yyyy-MM-dd-HH-mm-ss",
                    null, System.Globalization.DateTimeStyles.None, out var backupFileTime))
                {
                    if (backupFileTime >= expectedTimeWindow && backupFileTime <= backupTime.AddMinutes(1))
                    {
                        createdBackupCursor = cursor;
                        backupFound = true;
                        break;
                    }
                }
            }
        }

        // Assert backup was created
        backupFound.Should().BeTrue("Backup should have been created by the cron scheduler");
        createdBackupCursor.Should().NotBeNull();

        // Act - Test restore functionality with the created backup
        // First, delete the original data to test restore
        await _databaseService.DropTableAsync(testTableName);

        var restoreResult = await backupService.RestoreBackupAsync(createdBackupCursor!);

        // Assert restore was successful
        restoreResult.IsSuccessful.Should().BeTrue();

        // Verify data was restored correctly
        var restoredItem1 = await _databaseService.GetItemAsync(testTableName, testKey1);
        var restoredItem2 = await _databaseService.GetItemAsync(testTableName, testKey2);

        restoredItem1.IsSuccessful.Should().BeTrue();
        restoredItem2.IsSuccessful.Should().BeTrue();

        restoredItem1.Data.Should().NotBeNull();
        restoredItem2.Data.Should().NotBeNull();

        restoredItem1.Data!["Name"]!.Value<string>().Should().Be("John Doe");
        restoredItem1.Data!["Email"]!.Value<string>().Should().Be("john@example.com");
        restoredItem1.Data!["Age"]!.Value<int>().Should().Be(30);

        restoredItem2.Data!["Name"]!.Value<string>().Should().Be("Jane Smith");
        restoredItem2.Data!["Email"]!.Value<string>().Should().Be("jane@example.com");
        restoredItem2.Data!["Age"]!.Value<int>().Should().Be(25);
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
