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

    private static JToken FromPrimitiveTypeToJToken(PrimitiveType primitive)
    {
        return primitive.Kind switch
        {
            PrimitiveTypeKind.Double => primitive.AsDouble,
            PrimitiveTypeKind.Boolean => primitive.AsBoolean,
            PrimitiveTypeKind.Integer => primitive.AsInteger,
            PrimitiveTypeKind.ByteArray => Convert.ToBase64String(primitive.AsByteArray),
            PrimitiveTypeKind.String => primitive.AsString,
            _ => primitive.ToString()
        };
    }

    protected static void AddKeyToJson(JObject destination, string keyName, PrimitiveType keyValue)
    {
        destination[keyName] = FromPrimitiveTypeToJToken(keyValue);
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
        IEnumerable<DbAttributeCondition>? conditions = null,
        CancellationToken cancellationToken = default)
    {
        await EnsureReadyForOperation();
        await using var mutex = await CreateMutexScopeAsync(tableName, cancellationToken);
        return await ItemExistsCoreAsync(tableName, key, conditions, cancellationToken);
    }

    /// <inheritdoc cref="ItemExistsAsync"/>
    protected abstract Task<OperationResult<bool>> ItemExistsCoreAsync(
        string tableName,
        DbKey key,
        IEnumerable<DbAttributeCondition>? conditions = null,
        CancellationToken cancellationToken = default);

    /// <inheritdoc />
    public async Task<OperationResult<JObject?>> GetItemAsync(
        string tableName,
        DbKey key,
        string[]? attributesToRetrieve = null,
        CancellationToken cancellationToken = default)
    {
        await EnsureReadyForOperation();
        await using var mutex = await CreateMutexScopeAsync(tableName, cancellationToken);
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
        await using var mutex = await CreateMutexScopeAsync(tableName, cancellationToken);
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
        await using var mutex = await CreateMutexScopeAsync(tableName, cancellationToken);
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
        IEnumerable<DbAttributeCondition>? conditions = null,
        CancellationToken cancellationToken = default)
    {
        await EnsureReadyForOperation();
        await using var mutex = await CreateMutexScopeAsync(tableName, cancellationToken);
        return await UpdateItemCoreAsync(tableName, key, updateData, returnBehavior, conditions, cancellationToken);
    }

    /// <inheritdoc cref="UpdateItemAsync"/>
    protected abstract Task<OperationResult<JObject?>> UpdateItemCoreAsync(
        string tableName,
        DbKey key,
        JObject updateData,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        IEnumerable<DbAttributeCondition>? conditions = null,
        CancellationToken cancellationToken = default);

    /// <inheritdoc />
    public async Task<OperationResult<JObject?>> DeleteItemAsync(
        string tableName,
        DbKey key,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        IEnumerable<DbAttributeCondition>? conditions = null,
        CancellationToken cancellationToken = default)
    {
        await EnsureReadyForOperation();
        await using var mutex = await CreateMutexScopeAsync(tableName, cancellationToken);
        return await DeleteItemCoreAsync(tableName, key, returnBehavior, conditions, cancellationToken);
    }

    /// <inheritdoc cref="DeleteItemAsync"/>
    protected abstract Task<OperationResult<JObject?>> DeleteItemCoreAsync(
        string tableName,
        DbKey key,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        IEnumerable<DbAttributeCondition>? conditions = null,
        CancellationToken cancellationToken = default);

    /// <inheritdoc />
    public async Task<OperationResult<JObject?>> AddElementsToArrayAsync(
        string tableName,
        DbKey key,
        string arrayAttributeName,
        PrimitiveType[] elementsToAdd,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        IEnumerable<DbAttributeCondition>? conditions = null,
        CancellationToken cancellationToken = default)
    {
        await EnsureReadyForOperation();
        await using var mutex = await CreateMutexScopeAsync(tableName, cancellationToken);
        return await AddElementsToArrayCoreAsync(tableName, key, arrayAttributeName, elementsToAdd, returnBehavior, conditions, false, cancellationToken);
    }

    /// <inheritdoc cref="AddElementsToArrayAsync"/>
    protected abstract Task<OperationResult<JObject?>> AddElementsToArrayCoreAsync(
        string tableName,
        DbKey key,
        string arrayAttributeName,
        PrimitiveType[] elementsToAdd,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        IEnumerable<DbAttributeCondition>? conditions = null,
        bool isCalledFromPostInsert = false,
        CancellationToken cancellationToken = default);

    /// <inheritdoc />
    public async Task<OperationResult<JObject?>> RemoveElementsFromArrayAsync(
        string tableName,
        DbKey key,
        string arrayAttributeName,
        PrimitiveType[] elementsToRemove,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        IEnumerable<DbAttributeCondition>? conditions = null,
        CancellationToken cancellationToken = default)
    {
        await EnsureReadyForOperation();
        await using var mutex = await CreateMutexScopeAsync(tableName, cancellationToken);
        return await RemoveElementsFromArrayCoreAsync(tableName, key, arrayAttributeName, elementsToRemove, returnBehavior, conditions, cancellationToken);
    }

    /// <inheritdoc cref="RemoveElementsFromArrayAsync"/>
    protected abstract Task<OperationResult<JObject?>> RemoveElementsFromArrayCoreAsync(
        string tableName,
        DbKey key,
        string arrayAttributeName,
        PrimitiveType[] elementsToRemove,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        IEnumerable<DbAttributeCondition>? conditions = null,
        CancellationToken cancellationToken = default);

    /// <inheritdoc />
    public async Task<OperationResult<double>> IncrementAttributeAsync(
        string tableName,
        DbKey key,
        string numericAttributeName,
        double incrementValue,
        IEnumerable<DbAttributeCondition>? conditions = null,
        CancellationToken cancellationToken = default)
    {
        await EnsureReadyForOperation();
        await using var mutex = await CreateMutexScopeAsync(tableName, cancellationToken);
        return await IncrementAttributeCoreAsync(tableName, key, numericAttributeName, incrementValue, conditions, cancellationToken);
    }

    /// <inheritdoc cref="IncrementAttributeAsync"/>
    protected abstract Task<OperationResult<double>> IncrementAttributeCoreAsync(
        string tableName,
        DbKey key,
        string numericAttributeName,
        double incrementValue,
        IEnumerable<DbAttributeCondition>? conditions = null,
        CancellationToken cancellationToken = default);

    /// <inheritdoc />
    public async Task<OperationResult<(IReadOnlyList<string> Keys, IReadOnlyList<JObject> Items)>> ScanTableAsync(
        string tableName,
        CancellationToken cancellationToken = default)
    {
        await EnsureReadyForOperation();
        await using var mutex = await CreateMutexScopeAsync(tableName, cancellationToken);
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
        await using var mutex = await CreateMutexScopeAsync(tableName, cancellationToken);
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
        IEnumerable<DbAttributeCondition> filterConditions,
        CancellationToken cancellationToken = default)
    {
        await EnsureReadyForOperation();
        await using var mutex = await CreateMutexScopeAsync(tableName, cancellationToken);
        return await ScanTableWithFilterCoreAsync(tableName, filterConditions, cancellationToken);
    }

    /// <inheritdoc cref="ScanTableWithFilterAsync"/>
    protected abstract Task<OperationResult<(IReadOnlyList<string> Keys, IReadOnlyList<JObject> Items)>> ScanTableWithFilterCoreAsync(
        string tableName,
        IEnumerable<DbAttributeCondition> filterConditions,
        CancellationToken cancellationToken = default);

    /// <inheritdoc />
    public async Task<OperationResult<(IReadOnlyList<string>? Keys, IReadOnlyList<JObject> Items, string? NextPageToken, long? TotalCount)>> ScanTableWithFilterPaginatedAsync(
        string tableName,
        IEnumerable<DbAttributeCondition> filterConditions,
        int pageSize,
        string? pageToken = null,
        CancellationToken cancellationToken = default)
    {
        await EnsureReadyForOperation();
        await using var mutex = await CreateMutexScopeAsync(tableName, cancellationToken);
        return await ScanTableWithFilterPaginatedCoreAsync(tableName, filterConditions, pageSize, pageToken, cancellationToken);
    }

    /// <inheritdoc cref="ScanTableWithFilterPaginatedAsync"/>
    protected abstract Task<OperationResult<(IReadOnlyList<string>? Keys, IReadOnlyList<JObject> Items, string? NextPageToken, long? TotalCount)>> ScanTableWithFilterPaginatedCoreAsync(
        string tableName,
        IEnumerable<DbAttributeCondition> filterConditions,
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
        await using var mutex = await CreateMutexScopeAsync(tableName, cancellationToken);
        return await DropTableCoreAsync(tableName, cancellationToken);
    }

    /// <inheritdoc cref="DropTableAsync"/>
    protected internal abstract Task<OperationResult<bool>> DropTableCoreAsync(string tableName, CancellationToken cancellationToken = default);

    /// <inheritdoc />
    public abstract DbAttributeCondition BuildAttributeExistsCondition(string attributeName);

    /// <inheritdoc />
    public abstract DbAttributeCondition BuildAttributeNotExistsCondition(string attributeName);

    /// <inheritdoc />
    public abstract DbAttributeCondition BuildAttributeEqualsCondition(string attributeName, PrimitiveType value);

    /// <inheritdoc />
    public abstract DbAttributeCondition BuildAttributeNotEqualsCondition(string attributeName, PrimitiveType value);

    /// <inheritdoc />
    public abstract DbAttributeCondition BuildAttributeGreaterCondition(string attributeName, PrimitiveType value);

    /// <inheritdoc />
    public abstract DbAttributeCondition BuildAttributeGreaterOrEqualCondition(string attributeName, PrimitiveType value);

    /// <inheritdoc />
    public abstract DbAttributeCondition BuildAttributeLessCondition(string attributeName, PrimitiveType value);

    /// <inheritdoc />
    public abstract DbAttributeCondition BuildAttributeLessOrEqualCondition(string attributeName, PrimitiveType value);

    /// <inheritdoc />
    public abstract DbAttributeCondition BuildArrayElementExistsCondition(string attributeName, PrimitiveType elementValue);

    /// <inheritdoc />
    public abstract DbAttributeCondition BuildArrayElementNotExistsCondition(string attributeName, PrimitiveType elementValue);

    /// <inheritdoc />
    public async Task<OperationResult<IReadOnlyList<string>>> GetTableKeysAsync(
        string tableName,
        CancellationToken cancellationToken = default)
    {
        await EnsureReadyForOperation();
        await using var mutex = await CreateMutexScopeAsync(tableName, cancellationToken);
        return await GetTableKeysCoreAsync(tableName, cancellationToken);
    }

    /// <inheritdoc cref="GetTableKeysAsync"/>
    protected async Task<OperationResult<IReadOnlyList<string>>> GetTableKeysCoreAsync(
        string tableName,
        CancellationToken cancellationToken = default)
    {
        await using var mutex = await CreateMutexScopeAsync(SystemTableName, cancellationToken); //System table lock

        var getResult = await GetItemCoreAsync(
            SystemTableName,
            new DbKey(SystemTableKeyName, tableName),
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

        await using var mutex = await CreateMutexScopeAsync(SystemTableName, cancellationToken); //System table lock

        var addElementResult = await AddElementsToArrayCoreAsync(
            SystemTableName,
            new DbKey(SystemTableKeyName, tableName),
            SystemTableKeysAttributeName,
            [new PrimitiveType(key.Name)],
            DbReturnItemBehavior.DoNotReturn,
            [
                new DbArrayElementCondition(
                    DbAttributeConditionType.ArrayElementNotExists,
                    SystemTableKeysAttributeName,
                    key.Name)],
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

        await using var mutex = await CreateMutexScopeAsync(SystemTableName, cancellationToken); //System table lock

        var deleteResult = await DeleteItemCoreAsync(
            SystemTableName,
            new DbKey(SystemTableKeyName, tableName),
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

        var conditions = new List<DbAttributeCondition>();
        foreach (var attr in item)
        {
            if (attr.Key == key.Name) continue;
            conditions.Add(
                BuildArrayElementNotExistsCondition(
                    SystemTableKeysAttributeName,
                    new PrimitiveType(attr.Key)));
        }
        if (conditions.Count == 0)
            return OperationResult<bool>.Success(true);

        await using var mutex = await CreateMutexScopeAsync(SystemTableName, cancellationToken); //System table lock

        var ifKeyAttrUsedSanityCheckResult = await ItemExistsCoreAsync(
            SystemTableName,
            new DbKey(SystemTableKeyName, tableName),
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

    private async Task<MemoryScopeMutex> CreateMutexScopeAsync(
        string tableName,
        CancellationToken cancellationToken)
    {
        return await MemoryScopeMutex.CreateScopeAsync(
            MemoryService,
            MemoryScope,
            $"{_mutexScopePrefix}{tableName}",
            TimeSpan.FromMinutes(1),
            cancellationToken);
    }
    private static readonly MemoryScopeLambda MemoryScope = new("CrossCloudKit.Interfaces.Classes.DatabaseServiceBase:MutexScope");
    private readonly string _mutexScopePrefix = databaseNameIfApplicable != null ? $"{databaseNameIfApplicable}:" : "";
}
