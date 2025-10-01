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

public sealed class DbBackupFileCursor
{
    internal DbBackupFileCursor(string fileName)
    {
        FileName = fileName;
    }
    public string FileName { get; }
}

// ReSharper disable once ClassNeverInstantiated.Global
public class DatabaseServiceBackup: IAsyncDisposable
{
    public DatabaseServiceBackup(
        IDatabaseService databaseService,
        IFileService fileService,
        string backupBucketName,
        IPubSubService pubsubService,
        string cronExpression = "0 1 * * *",
        TimeZoneInfo? timeZoneInfo = null,
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

        _cronExpression = CronExpression.Parse(cronExpression);
        _timeZone = timeZoneInfo ?? TimeZoneInfo.Utc;

        var regResult = _databaseService.RegisterBackupSystem(_pubsubService, _errorMessageAction, _cts.Token).GetAwaiter().GetResult();
        if (!regResult.IsSuccessful)
            throw new InvalidOperationException($"RegisterBackupSystem failed with: {regResult.ErrorMessage} ({regResult.StatusCode})");

        _backgroundTask = Task.Run(async () => await RunBackgroundTaskAsync());
    }

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

    public async Task<OperationResult<bool>> RestoreBackupAsync(DbBackupFileCursor backupFileCursor)
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
                $"{_backupBucketName}{backupFileCursor.FileName}",
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
            var errors = new ConcurrentBag<string>();

            var tasks = tables.Values.Select(async tableData =>
            {
                var dropResult = await _databaseService.DropTableAsync(tableData.TableName, _cts.Token);
                if (!dropResult.IsSuccessful)
                {
                    errors.Add(dropResult.ErrorMessage);
                }

                var innerTasks = tableData.Items.Select(async item =>
                {
                    var putResult = await _databaseService.PutItemAsync(
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
                });

                await Task.WhenAll(innerTasks);
            });

            await Task.WhenAll(tasks);

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
    private readonly CronExpression _cronExpression;
    private readonly TimeZoneInfo _timeZone;
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
                var next = _cronExpression.GetNextOccurrence(DateTimeOffset.Now, _timeZone);
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
                    await BackupOperation();
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

    private async Task BackupOperation()
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
                var tableNamesResult = await _databaseService.GetTableNamesAsync(_cts.Token);
                if (!tableNamesResult.IsSuccessful)
                    throw new InvalidOperationException($"GetTableNamesAsync failed with: {tableNamesResult.ErrorMessage}");
                var tableNames = tableNamesResult.Data;
                if (tableNames.Count == 0)
                    return;

                foreach (var tableName in tableNames)
                {
                    var scanResult = await _databaseService.ScanTableAsync(tableName, _cts.Token);
                    if (!scanResult.IsSuccessful)
                        throw new InvalidOperationException($"ScanTableAsync failed with: {scanResult.ErrorMessage}");
                    if (scanResult.Data.Items.Count == 0)
                        continue;

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

                    finalArray.Add(new JObject
                    {
                        [TableNameJsonKey] = tableName,
                        [KeyNameJsonKey] = keyName,
                        [ItemsJsonKey] = itemsJArray
                    });
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
            return;

        var compiled = finalArray.ToString(Formatting.None);
        var backupPath = $"{_backupRootPath}/{DateTime.UtcNow:yyyy-MM-dd-HH-mm-ss}.json";

        await using var mStream = new MemoryTributary(Encoding.UTF8.GetBytes(compiled));

        var uploadResult = await _fileService.UploadFileAsync(
            new StringOrStream(mStream, mStream.Length, Encoding.UTF8),
            _backupBucketName,
            backupPath,
            cancellationToken: _cts.Token);
        if (!uploadResult.IsSuccessful)
            throw new InvalidOperationException($"UploadFileAsync failed with: {uploadResult.ErrorMessage}");
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
