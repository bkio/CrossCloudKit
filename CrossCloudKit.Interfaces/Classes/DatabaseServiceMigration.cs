// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;

namespace CrossCloudKit.Interfaces.Classes;

/// <summary>
/// Provides database migration functionality for CrossCloudKit database services.
/// This static utility class enables seamless data migration between different database instances
/// by leveraging the backup and restore capabilities of DatabaseServiceBackup.
/// </summary>
/// <remarks>
/// The migration process is atomic and uses distributed mutex locking to ensure data consistency.
/// The class creates temporary backup files during migration and handles cleanup operations.
/// All migration operations publish events through the PubSub service for monitoring and logging.
///
/// Migration workflow:
/// <list type="number">
/// <item><description>Create a backup of the source database</description></item>
/// <item><description>Optionally clean up the source database after backup</description></item>
/// <item><description>Optionally clean up the destination database before restore</description></item>
/// <item><description>Restore the backup to the destination database</description></item>
/// </list>
///
/// This class is particularly useful for:
/// <list type="bullet">
/// <item><description>Moving data between different database providers (e.g., from DynamoDB to CosmosDB)</description></item>
/// <item><description>Database upgrades and maintenance operations</description></item>
/// <item><description>Creating database replicas or copies</description></item>
/// <item><description>Environment migrations (e.g., dev to prod)</description></item>
/// </list>
/// </remarks>
public static class DatabaseServiceMigration
{
    /// <summary>
    /// Migrates all data from a source database to a destination database using backup and restore operations.
    /// </summary>
    /// <param name="sourceDatabase">The source database service to migrate data from. Must derive from DatabaseServiceBase.</param>
    /// <param name="destinationDatabase">The destination database service to migrate data to. Must derive from DatabaseServiceBase.</param>
    /// <param name="fileService">The file service used to store temporary backup files during migration.</param>
    /// <param name="pubsubService">The PubSub service used for publishing migration operation events.</param>
    /// <param name="backupWorkBucketName">The name of the bucket or container where temporary backup files will be stored during migration.</param>
    /// <param name="cleanUpSourceDatabaseAfterMigrate">
    /// Whether to drop all tables in the source database after successful migration.
    /// <b>WARNING: This is a destructive operation that cannot be undone.</b>
    /// </param>
    /// <param name="cleanUpDestinationDatabaseBeforeMigrate">
    /// Whether to drop all existing tables in the destination database before migration.
    /// <b>WARNING: This will permanently delete all existing data in the destination database.</b>
    /// </param>
    /// <param name="errorMessageAction">Optional callback for handling errors that occur during migration operations.</param>
    /// <returns>
    /// An OperationResult&lt;bool&gt; indicating the success or failure of the migration operation.
    /// Returns true on successful migration, or an error result with details if the operation fails.
    /// </returns>
    /// <exception cref="ArgumentException">Thrown when either database service does not derive from DatabaseServiceBase.</exception>
    /// <exception cref="InvalidOperationException">Thrown when any of the required services are not properly initialized.</exception>
    /// <remarks>
    /// This method performs a complete database migration in the following steps:
    /// <list type="number">
    /// <item><description>Creates a DatabaseServiceBackup instance for the source database</description></item>
    /// <item><description>Takes a backup of all source database tables and data</description></item>
    /// <item><description>Optionally drops all source database tables if cleanUpSourceDatabaseAfterMigrate is true</description></item>
    /// <item><description>Creates a DatabaseServiceBackup instance for the destination database</description></item>
    /// <item><description>Optionally drops all destination database tables if cleanUpDestinationDatabaseBeforeMigrate is true</description></item>
    /// <item><description>Restores the backup data to the destination database</description></item>
    /// </list>
    ///
    /// <b>IMPORTANT CONSIDERATIONS:</b>
    /// <list type="bullet">
    /// <item><description>The migration is atomic - either all data is migrated successfully, or the operation fails entirely</description></item>
    /// <item><description>Distributed mutex locks prevent concurrent backup/restore operations during migration</description></item>
    /// <item><description>Temporary backup files are created in the specified bucket and cleaned up automatically</description></item>
    /// <item><description>The destination database schema will be recreated to match the source database structure</description></item>
    /// <item><description>All migration events are published through the PubSub service for monitoring</description></item>
    /// <item><description>If the source database is empty, the migration will fail with NotFound status</description></item>
    /// </list>
    ///
    /// <b>DESTRUCTIVE OPERATIONS WARNING:</b>
    /// Both cleanup parameters can permanently delete database content:
    /// <list type="bullet">
    /// <item><description>cleanUpSourceDatabaseAfterMigrate will delete all source database tables after successful backup</description></item>
    /// <item><description>cleanUpDestinationDatabaseBeforeMigrate will delete all destination database tables before restore</description></item>
    /// </list>
    /// Use these options with extreme caution and ensure you have proper backups.
    ///
    /// For large databases, the migration process may take considerable time and network bandwidth.
    /// Monitor the errorMessageAction callback for progress updates and error handling.
    /// </remarks>
    public static async Task<OperationResult<bool>> MigrateAsync(
        IDatabaseService sourceDatabase,
        IDatabaseService destinationDatabase,
        IFileService fileService,
        IPubSubService pubsubService,
        string backupWorkBucketName,
        bool cleanUpSourceDatabaseAfterMigrate = false,
        bool cleanUpDestinationDatabaseBeforeMigrate = false,
        Action<Exception>? errorMessageAction = null)
    {
        await using var oldDatabaseBackupService = new DatabaseServiceBackup(
            sourceDatabase,
            fileService,
            pubsubService,
            backupWorkBucketName,
            errorMessageAction: errorMessageAction);

        var cursorResult = await oldDatabaseBackupService.TakeBackup(cleanUpSourceDatabaseAfterMigrate);
        if (!cursorResult.IsSuccessful)
            return OperationResult<bool>.Failure(cursorResult.ErrorMessage, cursorResult.StatusCode);

        if (cursorResult.Data == null)
            return OperationResult<bool>.Failure("No data found to migrate.", HttpStatusCode.NotFound);

        await using var newDatabaseBackupService = new DatabaseServiceBackup(
            destinationDatabase,
            fileService,
            pubsubService,
            backupWorkBucketName,
            errorMessageAction: errorMessageAction);

        return await newDatabaseBackupService.RestoreBackupAsync(
            cursorResult.Data,
            cleanUpDestinationDatabaseBeforeMigrate);
    }
}
