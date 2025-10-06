// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;
using CrossCloudKit.Interfaces.Enums;
using CrossCloudKit.Interfaces.Records;
using CrossCloudKit.Utilities.Common;
using Newtonsoft.Json.Linq;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("CrossCloudKit.Database.Tests.Common")]

namespace CrossCloudKit.Interfaces.Classes;

public abstract class DatabaseServiceBase(IMemoryService memoryService, string? databaseNameIfApplicable = null) : IDatabaseService
{
    internal readonly IMemoryService MemoryService = memoryService;

    private static JToken FromPrimitiveToJToken(Primitive primitive)
    {
        return primitive.Kind switch
        {
            PrimitiveKind.Double => primitive.AsDouble,
            PrimitiveKind.Boolean => primitive.AsBoolean,
            PrimitiveKind.Integer => primitive.AsInteger,
            PrimitiveKind.ByteArray => Convert.ToBase64String(primitive.AsByteArray),
            PrimitiveKind.String => primitive.AsString,
            _ => primitive.ToString()
        };
    }

    protected static void AddKeyToJson(JObject destination, string keyName, Primitive keyValue)
    {
        destination[keyName] = FromPrimitiveToJToken(keyValue);
    }

    /// <inheritdoc />
    public abstract bool IsInitialized { get; }

    public void SetOptions(DbOptions newOptions)
    {
        Options = newOptions ?? throw new ArgumentNullException(nameof(newOptions));
    }
    protected DbOptions Options { get; private set; } = new();

    /// <inheritdoc />
    public async Task<OperationResult<bool>> ItemExistsAsync(
        string tableName,
        DbKey key,
        ConditionCoupling? conditions = null,
        CancellationToken cancellationToken = default)
    {
        await EnsureReadyForOperation();
        await using var mutex = await CreateEntityMutexAsync(tableName, key, cancellationToken);
        return await ItemExistsCoreAsync(tableName, key, conditions, cancellationToken);
    }

    /// <inheritdoc cref="ItemExistsAsync"/>
    protected abstract Task<OperationResult<bool>> ItemExistsCoreAsync(
        string tableName,
        DbKey key,
        ConditionCoupling? conditions = null,
        CancellationToken cancellationToken = default);

    /// <inheritdoc />
    public async Task<OperationResult<JObject?>> GetItemAsync(
        string tableName,
        DbKey key,
        string[]? attributesToRetrieve = null,
        CancellationToken cancellationToken = default)
    {
        await EnsureReadyForOperation();
        await using var mutex = await CreateEntityMutexAsync(tableName, key, cancellationToken);
        return await GetItemCoreAsync(tableName, key, attributesToRetrieve, cancellationToken);
    }

    /// <inheritdoc cref="GetItemAsync"/>
    protected abstract Task<OperationResult<JObject?>> GetItemCoreAsync(
        string tableName,
        DbKey key,
        string[]? attributesToRetrieve = null,
        CancellationToken cancellationToken = default);

    /// <inheritdoc />
    public async Task<OperationResult<IReadOnlyList<JObject>>> GetItemsAsync(
        string tableName,
        DbKey[] keys,
        string[]? attributesToRetrieve = null,
        CancellationToken cancellationToken = default)
    {
        await EnsureReadyForOperation();
        await using var mutex = await CreateEntityMutexesAsync(tableName, keys, cancellationToken);
        return await GetItemsCoreAsync(tableName, keys, attributesToRetrieve, cancellationToken);
    }

    /// <inheritdoc cref="GetItemsAsync"/>
    protected abstract Task<OperationResult<IReadOnlyList<JObject>>> GetItemsCoreAsync(
        string tableName,
        DbKey[] keys,
        string[]? attributesToRetrieve = null,
        CancellationToken cancellationToken = default);

    /// <inheritdoc />
    public async Task<OperationResult<JObject?>> PutItemAsync(
        string tableName,
        DbKey key,
        JObject item,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        bool overwriteIfExists = false,
        CancellationToken cancellationToken = default)
    {
        await EnsureReadyForOperation();
        await using var mutex = await CreateEntityMutexAsync(tableName, key, cancellationToken);
        return await PutItemCoreAsync(tableName, key, item, returnBehavior, overwriteIfExists, cancellationToken);
    }

    /// <inheritdoc cref="PutItemAsync"/>
    protected internal abstract Task<OperationResult<JObject?>> PutItemCoreAsync(
        string tableName,
        DbKey key,
        JObject item,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        bool overwriteIfExists = false,
        CancellationToken cancellationToken = default);

    /// <inheritdoc />
    public async Task<OperationResult<JObject?>> UpdateItemAsync(
        string tableName,
        DbKey key,
        JObject updateData,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        ConditionCoupling? conditions = null,
        CancellationToken cancellationToken = default)
    {
        await EnsureReadyForOperation();
        await using var mutex = await CreateEntityMutexAsync(tableName, key, cancellationToken);
        return await UpdateItemCoreAsync(tableName, key, updateData, returnBehavior, conditions, cancellationToken);
    }

    /// <inheritdoc cref="UpdateItemAsync"/>
    protected abstract Task<OperationResult<JObject?>> UpdateItemCoreAsync(
        string tableName,
        DbKey key,
        JObject updateData,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        ConditionCoupling? conditions = null,
        CancellationToken cancellationToken = default);

    /// <inheritdoc />
    public async Task<OperationResult<JObject?>> DeleteItemAsync(
        string tableName,
        DbKey key,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        ConditionCoupling? conditions = null,
        CancellationToken cancellationToken = default)
    {
        await EnsureReadyForOperation();
        await using var mutex = await CreateEntityMutexAsync(tableName, key, cancellationToken);
        return await DeleteItemCoreAsync(tableName, key, returnBehavior, conditions, cancellationToken);
    }

    /// <inheritdoc cref="DeleteItemAsync"/>
    protected abstract Task<OperationResult<JObject?>> DeleteItemCoreAsync(
        string tableName,
        DbKey key,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        ConditionCoupling? conditions = null,
        CancellationToken cancellationToken = default);

    /// <inheritdoc />
    public async Task<OperationResult<JObject?>> AddElementsToArrayAsync(
        string tableName,
        DbKey key,
        string arrayAttributeName,
        Primitive[] elementsToAdd,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        ConditionCoupling? conditions = null,
        CancellationToken cancellationToken = default)
    {
        await EnsureReadyForOperation();
        await using var mutex = await CreateEntityMutexAsync(tableName, key, cancellationToken);
        return await AddElementsToArrayCoreAsync(tableName, key, arrayAttributeName, elementsToAdd, returnBehavior, conditions, false, cancellationToken);
    }

    /// <inheritdoc cref="AddElementsToArrayAsync"/>
    protected abstract Task<OperationResult<JObject?>> AddElementsToArrayCoreAsync(
        string tableName,
        DbKey key,
        string arrayAttributeName,
        Primitive[] elementsToAdd,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        ConditionCoupling? conditions = null,
        bool isCalledFromPostInsert = false,
        CancellationToken cancellationToken = default);

    /// <inheritdoc />
    public async Task<OperationResult<JObject?>> RemoveElementsFromArrayAsync(
        string tableName,
        DbKey key,
        string arrayAttributeName,
        Primitive[] elementsToRemove,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        ConditionCoupling? conditions = null,
        CancellationToken cancellationToken = default)
    {
        await EnsureReadyForOperation();
        await using var mutex = await CreateEntityMutexAsync(tableName, key, cancellationToken);
        return await RemoveElementsFromArrayCoreAsync(tableName, key, arrayAttributeName, elementsToRemove, returnBehavior, conditions, cancellationToken);
    }

    /// <inheritdoc cref="RemoveElementsFromArrayAsync"/>
    protected abstract Task<OperationResult<JObject?>> RemoveElementsFromArrayCoreAsync(
        string tableName,
        DbKey key,
        string arrayAttributeName,
        Primitive[] elementsToRemove,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        ConditionCoupling? conditions = null,
        CancellationToken cancellationToken = default);

    /// <inheritdoc />
    public async Task<OperationResult<double>> IncrementAttributeAsync(
        string tableName,
        DbKey key,
        string numericAttributeName,
        double incrementValue,
        ConditionCoupling? conditions = null,
        CancellationToken cancellationToken = default)
    {
        await EnsureReadyForOperation();
        await using var mutex = await CreateEntityMutexAsync(tableName, key, cancellationToken);
        return await IncrementAttributeCoreAsync(tableName, key, numericAttributeName, incrementValue, conditions, cancellationToken);
    }

    /// <inheritdoc cref="IncrementAttributeAsync"/>
    protected abstract Task<OperationResult<double>> IncrementAttributeCoreAsync(
        string tableName,
        DbKey key,
        string numericAttributeName,
        double incrementValue,
        ConditionCoupling? conditions = null,
        CancellationToken cancellationToken = default);

    /// <inheritdoc />
    public async Task<OperationResult<(IReadOnlyList<string> Keys, IReadOnlyList<JObject> Items)>> ScanTableAsync(
        string tableName,
        CancellationToken cancellationToken = default)
    {
        await EnsureReadyForOperation();
        await using var mutex = await CreateMasterMutexAsync(tableName, cancellationToken);
        return await ScanTableCoreAsync(tableName, cancellationToken);
    }

    /// <inheritdoc cref="ScanTableAsync"/>
    protected internal abstract Task<OperationResult<(IReadOnlyList<string> Keys, IReadOnlyList<JObject> Items)>> ScanTableCoreAsync(
        string tableName,
        CancellationToken cancellationToken = default);

    /// <inheritdoc />
    public async Task<OperationResult<(IReadOnlyList<string>? Keys, IReadOnlyList<JObject> Items, string? NextPageToken, long? TotalCount)>> ScanTablePaginatedAsync(
        string tableName,
        int pageSize,
        string? pageToken = null,
        CancellationToken cancellationToken = default)
    {
        await EnsureReadyForOperation();
        await using var mutex = await CreateMasterMutexAsync(tableName, cancellationToken);
        return await ScanTablePaginatedCoreAsync(tableName, pageSize, pageToken, cancellationToken);
    }

    /// <inheritdoc cref="ScanTablePaginatedAsync"/>
    protected abstract Task<OperationResult<(IReadOnlyList<string>? Keys, IReadOnlyList<JObject> Items, string? NextPageToken, long? TotalCount)>> ScanTablePaginatedCoreAsync(
        string tableName,
        int pageSize,
        string? pageToken = null,
        CancellationToken cancellationToken = default);

    /// <inheritdoc />
    public async Task<OperationResult<(IReadOnlyList<string> Keys, IReadOnlyList<JObject> Items)>> ScanTableWithFilterAsync(
        string tableName,
        ConditionCoupling filterConditions,
        CancellationToken cancellationToken = default)
    {
        await EnsureReadyForOperation();
        await using var mutex = await CreateMasterMutexAsync(tableName, cancellationToken);
        return await ScanTableWithFilterCoreAsync(tableName, filterConditions, cancellationToken);
    }

    /// <inheritdoc cref="ScanTableWithFilterAsync"/>
    protected abstract Task<OperationResult<(IReadOnlyList<string> Keys, IReadOnlyList<JObject> Items)>> ScanTableWithFilterCoreAsync(
        string tableName,
        ConditionCoupling filterConditions,
        CancellationToken cancellationToken = default);

    /// <inheritdoc />
    public async Task<OperationResult<(IReadOnlyList<string>? Keys, IReadOnlyList<JObject> Items, string? NextPageToken, long? TotalCount)>> ScanTableWithFilterPaginatedAsync(
        string tableName,
        ConditionCoupling filterConditions,
        int pageSize,
        string? pageToken = null,
        CancellationToken cancellationToken = default)
    {
        await EnsureReadyForOperation();
        await using var mutex = await CreateMasterMutexAsync(tableName, cancellationToken);
        return await ScanTableWithFilterPaginatedCoreAsync(tableName, filterConditions, pageSize, pageToken, cancellationToken);
    }

    /// <inheritdoc cref="ScanTableWithFilterPaginatedAsync"/>
    protected abstract Task<OperationResult<(IReadOnlyList<string>? Keys, IReadOnlyList<JObject> Items, string? NextPageToken, long? TotalCount)>> ScanTableWithFilterPaginatedCoreAsync(
        string tableName,
        ConditionCoupling filterConditions,
        int pageSize,
        string? pageToken = null,
        CancellationToken cancellationToken = default);

    /// <inheritdoc />
    public async Task<OperationResult<IReadOnlyList<string>>> GetTableNamesAsync(CancellationToken cancellationToken = default)
    {
        await EnsureReadyForOperation();
        return await GetTableNamesCoreAsync(cancellationToken);
    }

    /// <inheritdoc cref="GetTableNamesAsync"/>
    protected internal abstract Task<OperationResult<IReadOnlyList<string>>> GetTableNamesCoreAsync(CancellationToken cancellationToken = default);

    /// <inheritdoc />
    public async Task<OperationResult<bool>> DropTableAsync(string tableName, CancellationToken cancellationToken = default)
    {
        await EnsureReadyForOperation();
        await using var mutex = await CreateMasterMutexAsync(tableName, cancellationToken);
        return await DropTableCoreAsync(tableName, cancellationToken);
    }

    /// <inheritdoc cref="DropTableAsync"/>
    protected internal abstract Task<OperationResult<bool>> DropTableCoreAsync(string tableName, CancellationToken cancellationToken = default);

    /// <inheritdoc />
    public abstract Condition AttributeExists(string attributeName);

    /// <inheritdoc />
    public abstract Condition AttributeNotExists(string attributeName);

    /// <inheritdoc />
    public abstract Condition AttributeEquals(string attributeName, Primitive value);

    /// <inheritdoc />
    public abstract Condition AttributeNotEquals(string attributeName, Primitive value);

    /// <inheritdoc />
    public abstract Condition AttributeIsGreaterThan(string attributeName, Primitive value);

    /// <inheritdoc />
    public abstract Condition AttributeIsGreaterOrEqual(string attributeName, Primitive value);

    /// <inheritdoc />
    public abstract Condition AttributeIsLessThan(string attributeName, Primitive value);

    /// <inheritdoc />
    public abstract Condition AttributeIsLessOrEqual(string attributeName, Primitive value);

    /// <inheritdoc />
    public abstract Condition ArrayElementExists(string attributeName, Primitive elementValue);

    /// <inheritdoc />
    public abstract Condition ArrayElementNotExists(string attributeName, Primitive elementValue);

    /// <inheritdoc />
    public async Task<OperationResult<IReadOnlyList<string>>> GetTableKeysAsync(
        string tableName,
        CancellationToken cancellationToken = default)
    {
        await EnsureReadyForOperation();
        return await GetTableKeysCoreAsync(tableName, cancellationToken);
    }

    /// <inheritdoc cref="GetTableKeysAsync"/>
    protected async Task<OperationResult<IReadOnlyList<string>>> GetTableKeysCoreAsync(
        string tableName,
        CancellationToken cancellationToken = default)
    {
        var systemKey = new DbKey(SystemTableKeyName, tableName);

        await using var mutex = await CreateEntityMutexAsync(SystemTableName, systemKey, cancellationToken); //System table lock

        var getResult = await GetItemCoreAsync(
            SystemTableName,
            systemKey,
            [SystemTableKeysAttributeName],
            cancellationToken);
        if (!getResult.IsSuccessful)
            return OperationResult<IReadOnlyList<string>>.Failure(getResult.ErrorMessage, getResult.StatusCode);
        if (getResult.Data == null
            || !getResult.Data.TryGetTypedValue(SystemTableKeysAttributeName, out List<string> result))
            result = [];
        return OperationResult<IReadOnlyList<string>>.Success(result);
    }
    protected async Task<OperationResult<bool>> PostInsertItemAsync(
        string tableName,
        DbKey key,
        CancellationToken cancellationToken)
    {
        if (tableName == SystemTableName)
            return OperationResult<bool>.Success(true);

        var systemKey = new DbKey(SystemTableKeyName, tableName);

        await using var mutex = await CreateEntityMutexAsync(SystemTableName, systemKey, cancellationToken); //System table lock

        var addElementResult = await AddElementsToArrayCoreAsync(
            SystemTableName,
            systemKey,
            SystemTableKeysAttributeName,
            [new Primitive(key.Name)],
            DbReturnItemBehavior.DoNotReturn,
            new ArrayCondition(
                ConditionType.ArrayElementNotExists,
                SystemTableKeysAttributeName,
                key.Name),
            true,
            cancellationToken);
        if (addElementResult.IsSuccessful || addElementResult.StatusCode == HttpStatusCode.PreconditionFailed)
            return OperationResult<bool>.Success(true);
        return OperationResult<bool>.Failure(addElementResult.ErrorMessage, addElementResult.StatusCode);
    }
    protected async Task<OperationResult<bool>> PostDropTableAsync(
        string tableName,
        CancellationToken cancellationToken)
    {
        if (tableName == SystemTableName)
            return OperationResult<bool>.Success(true);

        var systemKey = new DbKey(SystemTableKeyName, tableName);

        await using var mutex = await CreateEntityMutexAsync(SystemTableName, systemKey, cancellationToken); //System table lock

        var deleteResult = await DeleteItemCoreAsync(
            SystemTableName,
            systemKey,
            DbReturnItemBehavior.ReturnNewValues,
            null,
            cancellationToken);
        if (!deleteResult.IsSuccessful)
            return OperationResult<bool>.Failure(deleteResult.ErrorMessage, deleteResult.StatusCode);

        if (deleteResult.Data == null ||
            !deleteResult.Data.TryGetTypedValue(SystemTableKeysAttributeName, out List<string> keys)
            || keys.Count == 0)
        {
            return await DropTableCoreAsync(SystemTableName, cancellationToken);
        }
        return OperationResult<bool>.Success(true);
    }
    protected async Task<OperationResult<bool>> AttributeNamesSanityCheck(
        DbKey key,
        string tableName,
        JObject item,
        CancellationToken cancellationToken)
    {
        if (tableName == SystemTableName)
            return OperationResult<bool>.Success(true);

        var conditions = new ConditionCoupling();
        var conditionNo = 0;
        foreach (var attr in item)
        {
            if (attr.Key == key.Name) continue;
            conditions = conditions.And(
                ArrayElementNotExists(
                    SystemTableKeysAttributeName,
                    new Primitive(attr.Key)));
            conditionNo++;
        }
        if (conditionNo == 0)
            return OperationResult<bool>.Success(true);

        var systemKey = new DbKey(SystemTableKeyName, tableName);

        await using var mutex = await CreateEntityMutexAsync(SystemTableName, systemKey, cancellationToken); //System table lock

        var ifKeyAttrUsedSanityCheckResult = await ItemExistsCoreAsync(
            SystemTableName,
            systemKey,
            conditions,
            cancellationToken);

        return !ifKeyAttrUsedSanityCheckResult.IsSuccessful
                ? ifKeyAttrUsedSanityCheckResult.StatusCode == HttpStatusCode.NotFound
                    ? OperationResult<bool>.Success(true)
                    : OperationResult<bool>.Failure(ifKeyAttrUsedSanityCheckResult.ErrorMessage, ifKeyAttrUsedSanityCheckResult.StatusCode)
                : OperationResult<bool>.Success(true);
    }

    protected const string SystemTableKeyName = "table";
    private const string SystemTableKeysAttributeName = "keys";
    protected const string SystemTableNamePrefix = "cross-cloud-kit-database-system-table";
    internal static string SystemTableNamePostfix { private get; set; }  = "";
    protected static string SystemTableName => SystemTableNamePrefix + SystemTableNamePostfix;

    private async Task EnsureReadyForOperation()
    {
        if (_backupInActionMemoryService.Value)
        {
            await using var mutex = await DatabaseServiceBackup.CreateBackupMutexScopeAsync(MemoryService);
        }
    }

    internal async Task<OperationResult<bool>> RegisterBackupSystem(
        IPubSubService pubsubService,
        Action<Exception>? onError,
        CancellationToken cancellationToken)
    {
        return await pubsubService.SubscribeAsync(BackupCheckTopicName, (_, message) =>
        {
            if (cancellationToken.IsCancellationRequested) return Task.CompletedTask;

            switch (message)
            {
                case BackupCheckPubSubStartedMessage:
                {
                    _backupInActionMemoryService.SetValue(true);
                    break;
                }
                case BackupCheckPubSubEndedMessage:
                {
                    _backupInActionMemoryService.SetValue(false);
                    break;
                }
            }
            return Task.CompletedTask;
        }, onError, cancellationToken);
    }

    private const string BackupCheckTopicName = "CrossCloudKit.Interfaces.Classes.DatabaseServiceBase:BackupCheck";
    private const string BackupCheckPubSubStartedMessage = "started";
    private const string BackupCheckPubSubEndedMessage = "ended";

    internal async Task<OperationResult<bool>> BackupOrRestoreOperationStarts(
        IPubSubService pubsubService,
        CancellationToken cancellationToken)
    {
        var publishResult = await pubsubService.PublishAsync(BackupCheckTopicName, BackupCheckPubSubStartedMessage, cancellationToken);
        if (!publishResult.IsSuccessful) return publishResult;

        _backupInActionMemoryService.SetValue(true);

        // Wait for 10 seconds to allow other services to realize the backup is in progress.
        await Task.Delay(10000, cancellationToken);

        return OperationResult<bool>.Success(true);
    }
    internal async Task<OperationResult<bool>> BackupOrRestoreOperationEnded(
        IPubSubService pubsubService,
        CancellationToken cancellationToken)
    {
        var publishResult = await pubsubService.PublishAsync(BackupCheckTopicName, BackupCheckPubSubEndedMessage, cancellationToken);
        if (!publishResult.IsSuccessful) return publishResult;

        _backupInActionMemoryService.SetValue(false);
        return OperationResult<bool>.Success(true);
    }
    private readonly Atomicable<bool> _backupInActionMemoryService = new(false, ThreadSafetyMode.MultipleProducers);

    private async Task<MemoryScopeMutex> CreateMasterMutexAsync(
        string tableName,
        CancellationToken cancellationToken)
    {
        return await MemoryScopeMutex.CreateMasterScopeAsync(
            MemoryService,
            new MemoryScopeLambda($"{MutexScopeConst}{_mutexScopePrefix}{tableName}"),
            TimeSpan.FromMinutes(1),
            cancellationToken);
    }
    private async Task<MemoryScopeMutex> CreateEntityMutexAsync(
        string tableName,
        DbKey key,
        CancellationToken cancellationToken)
    {
        return await MemoryScopeMutex.CreateEntityScopeAsync(
            MemoryService,
            new MemoryScopeLambda($"{MutexScopeConst}{_mutexScopePrefix}{tableName}"),
            $"{key.Name}:{key.Value}",
            TimeSpan.FromMinutes(1),
            cancellationToken);
    }
    private async Task<CompositeAsyncDisposable> CreateEntityMutexesAsync(
        string tableName,
        DbKey[] keys,
        CancellationToken cancellationToken)
    {
        var orderedKeys = keys
            .OrderBy(k => k.Name.ToString(), StringComparer.Ordinal)
            .ThenBy(k => k.Value.ToString(), StringComparer.Ordinal)
            .ToArray();
        var acquiredLocks = new List<MemoryScopeMutex>();

        try
        {
            foreach (var key in orderedKeys)
            {
                var mutex = await MemoryScopeMutex.CreateEntityScopeAsync(
                    MemoryService,
                    new MemoryScopeLambda($"{MutexScopeConst}{_mutexScopePrefix}{tableName}"),
                    $"{key.Name}:{key.Value}",
                    TimeSpan.FromMinutes(1),
                    cancellationToken);

                acquiredLocks.Add(mutex);
            }

            // Return a composite disposable that releases all acquired locks
            return new CompositeAsyncDisposable(acquiredLocks);
        }
        catch
        {
            foreach (var mutex in acquiredLocks)
                await mutex.DisposeAsync();
            throw;
        }
    }
    private const string MutexScopeConst = "CrossCloudKit.Interfaces.Classes.DatabaseServiceBase:";
    private readonly string _mutexScopePrefix = databaseNameIfApplicable != null ? $"{databaseNameIfApplicable}:" : "";

    private sealed class CompositeAsyncDisposable(IReadOnlyList<IAsyncDisposable> resources) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            foreach (var resource in resources)
                await resource.DisposeAsync();
        }
    }
}
