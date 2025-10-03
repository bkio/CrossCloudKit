// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using Cronos;
using CrossCloudKit.Interfaces.Enums;
using CrossCloudKit.Interfaces.Records;
using CrossCloudKit.Utilities.Common;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CrossCloudKit.Interfaces.Classes;

/// <summary>
/// Represents a cursor for navigating backup files stored in the file service.
/// Used to identify and access specific backup files for restoration operations.
/// </summary>
public sealed class DbBackupFileCursor
{
    internal DbBackupFileCursor(string fileName)
    {
        FileName = fileName;
    }

    /// <summary>
    /// Gets the name of the backup file without the root path prefix.
    /// </summary>
    public string FileName { get; }
}

/// <summary>
/// Provides automated database backup and restoration capabilities for CrossCloudKit database services.
/// This service runs scheduled backups using cron expressions and stores backup files in any supported file service.
/// Supports point-in-time restoration and distributed mutex locking to prevent concurrent operations.
/// </summary>
/// <remarks>
/// The backup service creates JSON files containing complete database snapshots with table structure and data.
/// All backup and restore operations are protected by distributed mutex locks to ensure data consistency.
/// The service publishes events through the PubSub service during backup and restore operations.
/// </remarks>
// ReSharper disable once ClassNeverInstantiated.Global
public class DatabaseServiceBackup: IAsyncDisposable
{
    /// <summary>
    /// Initializes a new instance of the DatabaseServiceBackup class with specified services and configuration.
    /// </summary>
    /// <param name="databaseService">The database service to back up. Must derive from DatabaseServiceBase.</param>
    /// <param name="fileService">The file service used to store backup files.</param>
    /// <param name="backupBucketName">The name of the bucket or container where backup files will be stored.</param>
    /// <param name="pubsubService">The PubSub service used for publishing backup operation events.</param>
    /// <param name="cronExpression">The cron expression defining the backup schedule. Defaults to "0 1 * * *" (daily at 1:00 AM).</param>
    /// <param name="timeZoneInfo">The time zone for interpreting the cron expression. Defaults to UTC.</param>
    /// <param name="backupRootPath">The root path within the bucket where backups will be stored. Defaults to empty string.</param>
    /// <param name="errorMessageAction">Optional callback for handling errors that occur during backup operations.</param>
    /// <exception cref="ArgumentException">Thrown when the database service does not derive from DatabaseServiceBase.</exception>
    /// <exception cref="InvalidOperationException">Thrown when any of the required services are not properly initialized or registration fails.</exception>
    public DatabaseServiceBackup(
        IDatabaseService databaseService,
        IFileService fileService,
        string backupBucketName,
        IPubSubService pubsubService,
        string cronExpression = "0 1 * * *",
        TimeZoneInfo? timeZoneInfo = null,
        string backupRootPath = "",
        Action<Exception>? errorMessageAction = null) : this(
            databaseService,
            fileService,
            pubsubService,
            backupBucketName,
            backupRootPath,
            errorMessageAction)
    {
        _cronExpression = CronExpression.Parse(cronExpression);
        _timeZone = timeZoneInfo ?? TimeZoneInfo.Utc;

        _backgroundTask = Task.Run(async () => await RunBackgroundTaskAsync());
    }

    /// <summary>
    /// Initializes a new instance of the DatabaseServiceBackup class for manual backup and restore operations only.
    /// This constructor does not set up automatic scheduled backups and is intended for on-demand backup operations.
    /// </summary>
    /// <param name="databaseService">The database service to back up. Must derive from DatabaseServiceBase.</param>
    /// <param name="fileService">The file service used to store backup files.</param>
    /// <param name="pubsubService">The PubSub service used for publishing backup operation events.</param>
    /// <param name="backupBucketName">The name of the bucket or container where backup files will be stored.</param>
    /// <param name="backupRootPath">The root path within the bucket where backups will be stored. Defaults to empty string.</param>
    /// <param name="errorMessageAction">Optional callback for handling errors that occur during backup operations.</param>
    /// <exception cref="ArgumentException">Thrown when the database service does not derive from DatabaseServiceBase.</exception>
    /// <exception cref="InvalidOperationException">Thrown when any of the required services are not properly initialized or registration fails.</exception>
    /// <remarks>
    /// This constructor is designed for scenarios where you want full control over backup timing and don't need automatic scheduled backups.
    /// Unlike the other constructor, this one does not:
    /// <list type="bullet">
    /// <item><description>Parse or store cron expressions</description></item>
    /// <item><description>Create background tasks for automatic scheduling</description></item>
    /// <item><description>Handle time zone conversions for scheduling</description></item>
    /// </list>
    ///
    /// Use this constructor when you want to trigger backups manually using the <see cref="TakeBackup"/> method,
    /// typically in response to specific application events, user actions, or custom scheduling logic.
    ///
    /// All backup and restore operations still use distributed mutex locking and publish events through the PubSub service.
    /// </remarks>
    // ReSharper disable once MemberCanBePrivate.Global
    public DatabaseServiceBackup(
        IDatabaseService databaseService,
        IFileService fileService,
        IPubSubService pubsubService,
        string backupBucketName,
        string backupRootPath = "",
        Action<Exception>? errorMessageAction = null)
    {
        if (databaseService is not DatabaseServiceBase castedDb)
            throw new ArgumentException("Invalid database service. Must derive from DatabaseServiceBase", nameof(databaseService));

        _databaseService = castedDb;
        _fileService = fileService;
        _pubsubService = pubsubService;

        var initResult = InitializationCheck();
        if (!initResult.IsSuccessful)
            throw new InvalidOperationException($"Initialization check failed with: {initResult.ErrorMessage} ({initResult.StatusCode})");

        _backupBucketName = backupBucketName;
        _backupRootPath = backupRootPath.Length > 0 && !backupRootPath.EndsWith('/') ? $"{backupRootPath}/" : backupRootPath;

        _errorMessageAction = errorMessageAction;

        var regResult = _databaseService.RegisterBackupSystem(_pubsubService, _errorMessageAction, _cts.Token).GetAwaiter().GetResult();
        if (!regResult.IsSuccessful)
            throw new InvalidOperationException($"RegisterBackupSystem failed with: {regResult.ErrorMessage} ({regResult.StatusCode})");
    }

    /// <summary>
    /// Asynchronously lists all available backup files as cursors for restoration operations.
    /// </summary>
    /// <param name="pageSize">The number of files to retrieve per page during enumeration. Defaults to 1000.</param>
    /// <param name="cancellationToken">A cancellation token to cancel the enumeration operation.</param>
    /// <returns>An async enumerable of DbBackupFileCursor objects representing available backup files.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the service is not properly initialized or file listing fails.</exception>
    /// <remarks>
    /// This method uses pagination internally to efficiently handle large numbers of backup files.
    /// The returned cursors can be used with RestoreBackupAsync to perform point-in-time restoration.
    /// </remarks>
    public async IAsyncEnumerable<DbBackupFileCursor> GetBackupFileCursorsAsync(
        int pageSize = 1000,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var initResult = InitializationCheck();
        if (!initResult.IsSuccessful)
            throw new InvalidOperationException(initResult.ErrorMessage);

        string? continuationToken = null;
        do
        {
            var listResult = await _fileService.ListFilesAsync(
                _backupBucketName,
                new FileListOptions
                {
                    ContinuationToken = continuationToken,
                    MaxResults = pageSize,
                    Prefix = _backupRootPath
                },
                cancellationToken);

            if (!listResult.IsSuccessful)
                throw new InvalidOperationException(listResult.ErrorMessage);

            foreach (var file in listResult.Data.FileKeys)
            {
                cancellationToken.ThrowIfCancellationRequested();

                yield return new DbBackupFileCursor(
                    file[_backupRootPath.Length..]);
            }

            continuationToken = listResult.Data.NextContinuationToken;

        } while (!string.IsNullOrEmpty(continuationToken));
    }

    /// <summary>
    /// Manually triggers a database backup operation outside of the scheduled cron-based backups.
    /// </summary>
    /// <param name="dropTablesAfterBackup"><b>(DANGEROUS!)</b> Whether to drop all tables after backup is done.</param>
    /// <returns>
    /// An OperationResult&lt;DbBackupFileCursor?&gt; containing:
    /// <list type="bullet">
    /// <item><description>A DbBackupFileCursor when backup is successfully created and contains data</description></item>
    /// <item><description>null when operation succeeds but there is no data to back up (empty database)</description></item>
    /// <item><description>An error result with details if the operation fails</description></item>
    /// </list>
    /// </returns>
    /// <remarks>
    /// This method provides on-demand backup functionality that bypasses the automatic scheduling.
    /// The backup operation includes:
    /// <list type="bullet">
    /// <item><description>Acquiring a distributed mutex lock to prevent concurrent operations</description></item>
    /// <item><description>Scanning all tables in the database</description></item>
    /// <item><description>Serializing table data to JSON format</description></item>
    /// <item><description>Uploading the backup file to the configured file service (only if data exists)</description></item>
    /// <item><description>Publishing backup events through the PubSub service</description></item>
    /// </list>
    /// The backup file is stored with a timestamp-based filename (yyyy-MM-dd-HH-mm-ss.json) in the configured backup location.
    /// If the database contains no tables or all tables are empty, no backup file is created and null is returned.
    /// If any service dependencies are not initialized, the operation will fail with a ServiceUnavailable status.
    /// </remarks>
    public async Task<OperationResult<DbBackupFileCursor?>> TakeBackup(bool dropTablesAfterBackup = false)
    {
        DbBackupFileCursor? result;
        try
        {
            result = await BackupOperation(dropTablesAfterBackup);
        }
        catch (Exception e)
        {
            return OperationResult<DbBackupFileCursor?>.Failure(e.Message, HttpStatusCode.InternalServerError);
        }
        return OperationResult<DbBackupFileCursor?>.Success(result);
    }

    /// <summary>
    /// Restores the database from a specified backup file, replacing all existing data.
    /// </summary>
    /// <param name="backupFileCursor">The backup file cursor identifying which backup to restore.</param>
    /// <param name="fullCleanUpBeforeRestoration">Whether to drop all existing tables before the restore operation.</param>
    /// <returns>An OperationResult indicating the success or failure of the restoration operation.</returns>
    /// <remarks>
    /// This operation performs a complete database restoration by:
    /// <list type="number">
    /// <item><description>Downloading and parsing the backup file</description></item>
    /// <item><description>Validating all data integrity</description></item>
    /// <item><description>Acquiring a distributed mutex lock to prevent concurrent operations</description></item>
    /// <item><description>Dropping existing tables and recreating them with backup data</description></item>
    /// <item><description>Publishing restoration events through the PubSub service</description></item>
    /// </list>
    ///
    /// <b>WARNING: This operation is destructive and will completely replace all existing database content if table names match.</b>
    /// <b>(if <see cref="fullCleanUpBeforeRestoration"/> is true, all existing database content will be dropped before the operation)</b>
    ///
    /// The operation is atomic - either all tables are restored successfully, or the operation fails entirely.
    /// </remarks>
    public async Task<OperationResult<bool>> RestoreBackupAsync(DbBackupFileCursor backupFileCursor, bool fullCleanUpBeforeRestoration = false)
    {
        if (_disposed)
            return OperationResult<bool>.Failure("Database backup service is disposed.", HttpStatusCode.ServiceUnavailable);

        var initResult = InitializationCheck();
        if (!initResult.IsSuccessful)
            return initResult;

        Dictionary<string, RestoreTableRecord> tables = [];
        try
        {
            await using var mStream = new MemoryTributary();
            await _fileService.DownloadFileAsync(
                _backupBucketName,
                $"{_backupRootPath}{backupFileCursor.FileName}",
                new StringOrStream(mStream, 0, Encoding.UTF8),
                null,
                _cts.Token);

            var jsonAsString = Encoding.UTF8.GetString(mStream.ToArray());
            var asJsonArray = JArray.Parse(jsonAsString);

            foreach (var jTok in asJsonArray)
            {
                var asJObject = (JObject)jTok;

                var tableName = asJObject.Value<string>(TableNameJsonKey).NotNull();
                var keyName = asJObject.Value<string>(KeyNameJsonKey).NotNull();
                var items = asJObject.Value<JArray>(ItemsJsonKey).NotNull();

                var invalidItemsNo = 0;
                Parallel.ForEach(items, item =>
                {
                    try
                    {
                        var asObj = (JObject)item;
                        if (!asObj.TryGetTypedValue(keyName, out string? _))
                        {
                            Interlocked.Increment(ref invalidItemsNo);
                        }
                    }
                    catch (Exception)
                    {
                        Interlocked.Increment(ref invalidItemsNo);
                    }
                });
                if (invalidItemsNo > 0)
                {
                    return OperationResult<bool>.Failure($"Invalid items ({invalidItemsNo}) found in table {tableName}.", HttpStatusCode.BadRequest);
                }

                if (!tables.TryAdd(tableName, new RestoreTableRecord(
                    tableName,
                    keyName,
                    items)))
                {
                    return OperationResult<bool>.Failure($"Failed to add table to restore; duplicate detected for table name {tableName}", HttpStatusCode.Conflict);
                }
            }
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure(ex.Message, HttpStatusCode.InternalServerError);
        }

        await using var mutex = await CreateBackupMutexScopeAsync(_databaseService.MemoryService);

        OperationResult<bool> opEndResult;
        OperationResult<bool> opStartResult = await _databaseService.BackupOrRestoreOperationStarts(_pubsubService, _cts.Token);
        if (!opStartResult.IsSuccessful) return opStartResult;
        try
        {
            if (fullCleanUpBeforeRestoration)
            {
                var tableNamesResult = await _databaseService.GetTableNamesCoreAsync(_cts.Token);
                if (!tableNamesResult.IsSuccessful)
                    throw new InvalidOperationException($"GetTableNamesAsync failed with: {tableNamesResult.ErrorMessage}");
                var tableNames = tableNamesResult.Data;
                if (tableNames.Count > 0)
                {
                    await Task.WhenAll(tableNames.Select(tableName => Task.Run(async () =>
                    {
                        var dropResult = await _databaseService.DropTableCoreAsync(tableName, _cts.Token);
                        if (!dropResult.IsSuccessful)
                        {
                            _errorMessageAction?.Invoke(new InvalidOperationException($"Warning: Drop operation for table {tableName} failed before backup with: {dropResult.ErrorMessage} ({dropResult.StatusCode})"));
                        }
                    })));
                }
            }

            var errors = new ConcurrentBag<string>();

            await Task.WhenAll(tables.Values.Select(async tableData =>
            {
                var dropResult = await _databaseService.DropTableCoreAsync(tableData.TableName, _cts.Token);
                if (!dropResult.IsSuccessful)
                {
                    errors.Add(dropResult.ErrorMessage);
                }

                await Task.WhenAll(tableData.Items.Select(async item =>
                {
                    var putResult = await _databaseService.PutItemCoreAsync(
                        tableData.TableName,
                        new DbKey(tableData.KeyName,
                            new PrimitiveType(
                            item[tableData.KeyName].NotNull().Value<string>().NotNull())),
                        (JObject)item,
                        DbReturnItemBehavior.DoNotReturn,
                        true,
                        _cts.Token);

                    if (!putResult.IsSuccessful)
                        errors.Add(putResult.ErrorMessage);
                }));
            }));

            if (!errors.IsEmpty)
                return OperationResult<bool>.Failure(
                    $"Operation -drop old and create new- failed: {string.Join(Environment.NewLine, errors)}",
                    HttpStatusCode.InternalServerError);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure(ex.Message, HttpStatusCode.InternalServerError);
        }
        finally
        {
            opEndResult = await _databaseService.BackupOrRestoreOperationEnded(_pubsubService, _cts.Token);
        }
        return opEndResult;
    }
    private record RestoreTableRecord(string TableName, string KeyName, JArray Items);

    /// <summary>
    /// Creates a distributed mutex scope for coordinating backup and restore operations across multiple processes.
    /// </summary>
    /// <param name="memoryService">The memory service used for distributed locking.</param>
    /// <returns>A MemoryScopeMutex that can be used to coordinate backup/restore operations.</returns>
    /// <remarks>
    /// This mutex ensures that only one backup or restore operation can run at a time across all instances
    /// of the backup service. The mutex has a 5-minute time-to-live to prevent indefinite locks in case
    /// of process failures. This method is used internally by both backup and restore operations.
    /// </remarks>
    internal static async Task<MemoryScopeMutex> CreateBackupMutexScopeAsync(IMemoryService memoryService)
    {
        return await MemoryScopeMutex.CreateScopeAsync(
            memoryService,
            BackupMemoryMutexScope,
            BackupMemoryMutexKeyName,
            BackupMutexTimeToLive());
    }

    private readonly CancellationTokenSource _cts = new();
    private readonly Task? _backgroundTask;

    private readonly string _backupRootPath;
    private readonly CronExpression? _cronExpression;
    private readonly TimeZoneInfo? _timeZone;
    private readonly DatabaseServiceBase _databaseService;
    private readonly IFileService _fileService;
    private readonly IPubSubService _pubsubService;
    private readonly string _backupBucketName;
    private readonly Action<Exception>? _errorMessageAction;

    private bool _disposed;

    /// <summary>
    /// Background task that runs continuously while the service instance is alive
    /// </summary>
    private async Task RunBackgroundTaskAsync(int errorRetryCount = 0)
    {
        try
        {
            while (!_cts.IsCancellationRequested)
            {
                var next = _cronExpression.NotNull().GetNextOccurrence(DateTimeOffset.Now, _timeZone.NotNull());
                if (next.HasValue)
                {
                    var delay = next.Value - DateTimeOffset.Now;
                    if (delay.TotalMilliseconds > 0)
                        await Task.Delay(delay, _cts.Token);
                }
                else return;

                if (_cts.Token.IsCancellationRequested) continue;
                try
                {
                    await BackupOperation(false);
                }
                catch (Exception ex)
                {
                    _errorMessageAction?.Invoke(ex);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when cancellation is requested
        }
        catch (Exception e)
        {
            _errorMessageAction?.Invoke(e);

            if (errorRetryCount == 10)
            {
                _errorMessageAction?.Invoke(new Exception("Error retry count exceeded. Giving up."));
                return;
            }
            await Task.Delay(TimeSpan.FromSeconds(1), _cts.Token);
            await RunBackgroundTaskAsync(errorRetryCount + 1);
        }
    }

    private const string TableNameJsonKey = "table_name";
    private const string KeyNameJsonKey = "key_name";
    private const string ItemsJsonKey = "items";

    private async Task<DbBackupFileCursor?> BackupOperation(bool cleanUpAfter)
    {
        ObjectDisposedException.ThrowIf(_disposed, nameof(DatabaseServiceBackup));

        var initResult = InitializationCheck();
        if (!initResult.IsSuccessful)
            throw new InvalidOperationException($"Initialization check failed with: {initResult.ErrorMessage} ({initResult.StatusCode})");

        var finalArray = new JArray();
        {
            await using var mutex = await CreateBackupMutexScopeAsync(_databaseService.MemoryService);

            OperationResult<bool> opEndResult;
            OperationResult<bool> opStartResult = await _databaseService.BackupOrRestoreOperationStarts(_pubsubService, _cts.Token);
            if (!opStartResult.IsSuccessful)
                throw new InvalidOperationException($"BackupOrRestoreOperationStarts failed with: {opStartResult.ErrorMessage} ({opStartResult.StatusCode})");
            try
            {
                var tableNamesResult = await _databaseService.GetTableNamesCoreAsync(_cts.Token);
                if (!tableNamesResult.IsSuccessful)
                    throw new InvalidOperationException($"GetTableNamesAsync failed with: {tableNamesResult.ErrorMessage}");
                var tableNames = tableNamesResult.Data;
                if (tableNames.Count == 0)
                    return null;

                await Task.WhenAll(tableNames.Select(tableName => Task.Run(async () =>
                {
                    var scanResult = await _databaseService.ScanTableCoreAsync(tableName, _cts.Token);
                    if (!scanResult.IsSuccessful) throw new InvalidOperationException($"ScanTableAsync failed with: {scanResult.ErrorMessage}");
                    if (scanResult.Data.Items.Count == 0) return;

                    string? keyName = null;
                    var itemsJArray = new JArray();
                    foreach (var item in scanResult.Data.Items)
                    {
                        keyName = scanResult.Data.Keys.FirstOrDefault(key => item.ContainsKey(key));

                        itemsJArray.Add(item);
                    }

                    if (keyName == null)
                    {
                        throw new InvalidOperationException($"Key name not found in the table {tableName}");
                    }

                    lock (finalArray)
                    {
                        finalArray.Add(new JObject { [TableNameJsonKey] = tableName, [KeyNameJsonKey] = keyName, [ItemsJsonKey] = itemsJArray });
                    }
                })));

                if (cleanUpAfter)
                {
                    await Task.WhenAll(tableNames.Select(tableName => Task.Run(async () =>
                    {
                        var dropResult = await _databaseService.DropTableCoreAsync(tableName, _cts.Token);
                        if (!dropResult.IsSuccessful)
                        {
                            _errorMessageAction?.Invoke(new InvalidOperationException($"Warning: Backup was successful, however drop operation for table {tableName} failed after backup with: {dropResult.ErrorMessage} ({dropResult.StatusCode})"));
                        }
                    })));
                }
            }
            finally
            {
                opEndResult = await _databaseService.BackupOrRestoreOperationEnded(_pubsubService, _cts.Token);
            }
            if (!opEndResult.IsSuccessful)
                throw new InvalidOperationException($"BackupOrRestoreOperationEnded failed with: {opEndResult.ErrorMessage} ({opEndResult.StatusCode})");
        }

        if (finalArray.Count == 0)
            return null;

        var compiled = finalArray.ToString(Formatting.None);

        await using var mStream = new MemoryTributary(Encoding.UTF8.GetBytes(compiled));

        var fileName = $"{DateTime.UtcNow:yyyy-MM-dd-HH-mm-ss}.json";

        var uploadResult = await _fileService.UploadFileAsync(
            new StringOrStream(mStream, mStream.Length, Encoding.UTF8),
            _backupBucketName,
            $"{_backupRootPath}{fileName}",
            cancellationToken: _cts.Token);
        if (!uploadResult.IsSuccessful)
            throw new InvalidOperationException($"UploadFileAsync failed with: {uploadResult.ErrorMessage}");
        return new DbBackupFileCursor(fileName);
    }

    private static readonly IMemoryScope BackupMemoryMutexScope = new MemoryScopeLambda("CrossCloudKit.Interfaces.Classes.DatabaseServiceBackup");
    private const string BackupMemoryMutexKeyName = "db-backup-mutex";
    private static TimeSpan BackupMutexTimeToLive() => TimeSpan.FromMinutes(5);

    private OperationResult<bool> InitializationCheck()
    {
        if (_databaseService is not { IsInitialized: true })
            return OperationResult<bool>.Failure("Database service is not initialized.", HttpStatusCode.ServiceUnavailable);
        if (_fileService is not { IsInitialized: true })
            return OperationResult<bool>.Failure("File service is not initialized.", HttpStatusCode.ServiceUnavailable);
        return _pubsubService is not { IsInitialized: true }
            ? OperationResult<bool>.Failure("Pub/Sub service is not initialized.", HttpStatusCode.ServiceUnavailable)
            : OperationResult<bool>.Success(true);
    }

    /// <summary>
    /// Asynchronously disposes of the backup service, stopping the background task and releasing resources.
    /// </summary>
    /// <returns>A ValueTask representing the asynchronous disposal operation.</returns>
    /// <remarks>
    /// This method gracefully shuts down the background backup task by:
    /// 1. Cancelling the background task using the cancellation token
    /// 2. Waiting up to 5 seconds for the background task to complete
    /// 3. Disposing of the cancellation token source
    /// 4. Marking the service as disposed
    ///
    /// Any exceptions during disposal are silently ignored to prevent issues during cleanup.
    /// Once disposed, the service cannot be reused and will reject further operations.
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        try
        {
            // Cancel the background task
            await _cts.CancelAsync();

            // Wait for the background task to complete (with timeout)
            await _backgroundTask.NotNull().WaitAsync(TimeSpan.FromSeconds(5));
        }
        catch
        {
            // Ignore exceptions during disposal
        }
        finally
        {
            try
            {
                _cts.Dispose();
            }
            catch (Exception)
            {
                // ignored
            }

            _disposed = true;

            GC.SuppressFinalize(this);
        }
    }
}
