// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Utilities.Common;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CrossCloudKit.Database.Basic;

public sealed class DatabaseServiceBasic : DatabaseServiceBase, IDatabaseService, IDisposable
{
    /// <summary>
    /// Holds initialization success
    /// </summary>
    private readonly bool _initializationSucceed;

    /// <summary>
    /// Gets a value indicating whether the database service has been successfully initialized.
    /// </summary>
    // ReSharper disable once ConvertToAutoProperty
    public bool IsInitialized => _initializationSucceed;

    private readonly string _databaseName;
    private readonly string _databasePath;

    private readonly IMemoryService _memoryService;

    private const string RootFolderName = "CrossCloudKit.Database.Basic";

    /// <summary>
    /// DatabaseServiceBasic: Constructor for the file-based database
    /// </summary>
    /// <param name="databaseName">Database name (will be used as a directory name)</param>
    /// <param name="memoryService">Memory service (will be used for mutex operations)</param>
    /// <param name="basePath">Base path for database storage (optional, defaults to current directory)</param>
    /// <param name="errorMessageAction">Error messages will be pushed to this action</param>
    public DatabaseServiceBasic(
        string databaseName,
        IMemoryService memoryService,
        string? basePath = null,
        Action<string>? errorMessageAction = null)
    {
        _databaseName = databaseName;
        _memoryService = memoryService;
        _databasePath = "";

        ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);

        try
        {
            basePath ??= Environment.CurrentDirectory;
            _databasePath = Path.Combine(basePath, RootFolderName, databaseName.MakeValidFileName());

            // Ensure database directory exists
            Directory.CreateDirectory(_databasePath);

            _initializationSucceed = true;
        }
        catch (Exception e)
        {
            errorMessageAction?.Invoke($"DatabaseServiceBasic->Constructor: {e.Message} \n Trace: {e.StackTrace}");
            _initializationSucceed = false;
        }
    }

    private string GetTablePath(string tableName)
    {
        var sanitizedTableName = tableName.MakeValidFileName();
        return Path.Combine(_databasePath, sanitizedTableName);
    }

    private string GetItemFilePath(string tableName, string keyName, PrimitiveType keyValue)
    {
        var tablePath = GetTablePath(tableName);
        var keyString = keyValue.Kind switch
        {
            PrimitiveTypeKind.String => keyValue.AsString,
            PrimitiveTypeKind.Integer => keyValue.AsInteger.ToString(CultureInfo.InvariantCulture),
            PrimitiveTypeKind.Double => keyValue.AsDouble.ToString(CultureInfo.InvariantCulture),
            PrimitiveTypeKind.ByteArray => Convert.ToBase64String(keyValue.AsByteArray),
            _ => keyValue.ToString()
        };

        var sanitizedKey = $"{keyName}_{keyString}".MakeValidFileName();
        return Path.Combine(tablePath, $"{sanitizedKey}.json");
    }

    private bool EnsureTableExists(string tableName)
    {
        try
        {
            var tablePath = GetTablePath(tableName);
            if (!Directory.Exists(tablePath))
            {
                Directory.CreateDirectory(tablePath);
            }

            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }

    private static async Task<JObject?> ReadItemFromFileAsync(string filePath, CancellationToken cancellationToken = default)
    {
        try
        {
            if (!File.Exists(filePath))
            {
                return null;
            }

            var json = await File.ReadAllTextAsync(filePath, Encoding.UTF8, cancellationToken);

            return string.IsNullOrWhiteSpace(json) ? null : JObject.Parse(json);
        }
        catch (Exception)
        {
            return null;
        }
    }

    private static async Task<bool> WriteItemToFileAsync(string filePath, JObject item, CancellationToken cancellationToken = default)
    {
        try
        {
            var directory = Path.GetDirectoryName(filePath);
            if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            var json = item.ToString(Formatting.Indented);

            await FileSystemUtilities.WriteToFileEnsureWrittenToDiskAsync(json, filePath, cancellationToken);

            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }

    private bool EvaluateCondition(JObject item, DatabaseAttributeCondition condition)
    {
        return condition.ConditionType switch
        {
            DatabaseAttributeConditionType.AttributeExists => item.ContainsKey(condition.AttributeName),
            DatabaseAttributeConditionType.AttributeNotExists => !item.ContainsKey(condition.AttributeName),
            _ when condition is ValueCondition valueCondition => EvaluateValueCondition(item, valueCondition),
            _ when condition is ArrayElementCondition arrayCondition => EvaluateArrayElementCondition(item, arrayCondition),
            _ => false
        };
    }

    private static bool EvaluateValueCondition(JObject item, ValueCondition condition)
    {
        if (!item.TryGetValue(condition.AttributeName, out var token))
        {
            return false;
        }

        var itemValue = TokenToPrimitiveType(token);
        var conditionValue = condition.Value;

        return condition.ConditionType switch
        {
            DatabaseAttributeConditionType.AttributeEquals => ComparePrimitiveTypes(itemValue, conditionValue) == 0,
            DatabaseAttributeConditionType.AttributeNotEquals => ComparePrimitiveTypes(itemValue, conditionValue) != 0,
            DatabaseAttributeConditionType.AttributeGreater => ComparePrimitiveTypes(itemValue, conditionValue) > 0,
            DatabaseAttributeConditionType.AttributeGreaterOrEqual => ComparePrimitiveTypes(itemValue, conditionValue) >= 0,
            DatabaseAttributeConditionType.AttributeLess => ComparePrimitiveTypes(itemValue, conditionValue) < 0,
            DatabaseAttributeConditionType.AttributeLessOrEqual => ComparePrimitiveTypes(itemValue, conditionValue) <= 0,
            _ => false
        };
    }

    private bool EvaluateArrayElementCondition(JObject item, ArrayElementCondition condition)
    {
        if (!item.TryGetValue(condition.AttributeName, out var token) || token is not JArray array)
        {
            return condition.ConditionType == DatabaseAttributeConditionType.ArrayElementNotExists;
        }

        var elementExists = array.Any(element =>
        {
            var arrayElementValue = TokenToPrimitiveType(element);
            return ComparePrimitiveTypes(arrayElementValue, condition.ElementValue) == 0;
        });

        return condition.ConditionType == DatabaseAttributeConditionType.ArrayElementExists ? elementExists : !elementExists;
    }

    private static PrimitiveType TokenToPrimitiveType(JToken token)
    {
        return token.Type switch
        {
            JTokenType.String => new PrimitiveType(token.Value<string>().NotNull()),
            JTokenType.Integer => new PrimitiveType(token.Value<long>()),
            JTokenType.Float => new PrimitiveType(token.Value<double>()),
            JTokenType.Boolean => new PrimitiveType(token.Value<bool>().ToString()),
            _ => new PrimitiveType(token.ToString())
        };
    }

    private static int ComparePrimitiveTypes(PrimitiveType a, PrimitiveType b)
    {
        if (a.Kind != b.Kind)
        {
            return string.Compare(a.ToString(), b.ToString(), StringComparison.Ordinal);
        }

        return a.Kind switch
        {
            PrimitiveTypeKind.String => string.Compare(a.AsString, b.AsString, StringComparison.Ordinal),
            PrimitiveTypeKind.Integer => a.AsInteger.CompareTo(b.AsInteger),
            PrimitiveTypeKind.Double => a.AsDouble.CompareTo(b.AsDouble),
            PrimitiveTypeKind.ByteArray => CompareByteArrays(a.AsByteArray, b.AsByteArray),
            _ => string.Compare(a.ToString(), b.ToString(), StringComparison.Ordinal)
        };
    }

    private static int CompareByteArrays(byte[] a, byte[] b)
    {
        var minLength = Math.Min(a.Length, b.Length);
        for (var i = 0; i < minLength; i++)
        {
            var result = a[i].CompareTo(b[i]);
            if (result != 0) return result;
        }
        return a.Length.CompareTo(b.Length);
    }

    #region Modern Async API

    /// <inheritdoc />
    public async Task<OperationResult<bool>> ItemExistsAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        DatabaseAttributeCondition? condition = null,
        CancellationToken cancellationToken = default)
    {
        await using var mutex = await CreateFileMutexScopeAsync(cancellationToken);
        try
        {
            var filePath = GetItemFilePath(tableName, keyName, keyValue);
            var item = await ReadItemFromFileAsync(filePath, cancellationToken);

            return OperationResult<bool>.Success(
                item != null && (condition == null || EvaluateCondition(item, condition)));
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure($"DatabaseServiceBasic->ItemExistsAsync: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<JObject?>> GetItemAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        string[]? attributesToRetrieve = null,
        CancellationToken cancellationToken = default)
    {
        await using var mutex = await CreateFileMutexScopeAsync(cancellationToken);
        try
        {
            var filePath = GetItemFilePath(tableName, keyName, keyValue);
            var item = await ReadItemFromFileAsync(filePath, cancellationToken);

            if (item == null)
            {
                return OperationResult<JObject?>.Success(null);
            }

            // Add the key to the result
            AddKeyToJson(item, keyName, keyValue);
            ApplyOptions(item);

            return OperationResult<JObject?>.Success(item);
        }
        catch (Exception ex)
        {
            return OperationResult<JObject?>.Failure($"DatabaseServiceBasic->GetItemAsync: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<IReadOnlyList<JObject>>> GetItemsAsync(
        string tableName,
        string keyName,
        PrimitiveType[] keyValues,
        string[]? attributesToRetrieve = null,
        CancellationToken cancellationToken = default)
    {
        await using var mutex = await CreateFileMutexScopeAsync(cancellationToken);
        try
        {
            if (keyValues.Length == 0)
            {
                return OperationResult<IReadOnlyList<JObject>>.Success([]);
            }

            var results = new List<JObject>();

            foreach (var keyValue in keyValues)
            {
                var filePath = GetItemFilePath(tableName, keyName, keyValue);
                var item = await ReadItemFromFileAsync(filePath, cancellationToken);

                if (item == null) continue;

                AddKeyToJson(item, keyName, keyValue);
                ApplyOptions(item);
                results.Add(item);
            }

            return OperationResult<IReadOnlyList<JObject>>.Success(results.AsReadOnly());
        }
        catch (Exception e)
        {
            return OperationResult<IReadOnlyList<JObject>>.Failure($"DatabaseServiceBasic->GetItemsAsync: {e.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<JObject?>> PutItemAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        JObject item,
        ReturnItemBehavior returnBehavior = ReturnItemBehavior.DoNotReturn,
        bool overwriteIfExists = false,
        CancellationToken cancellationToken = default)
    {
        return await PutOrUpdateItemAsync(PutOrUpdateItemType.PutItem, tableName, keyName, keyValue, item,
            returnBehavior, null, overwriteIfExists, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<OperationResult<JObject?>> UpdateItemAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        JObject updateData,
        ReturnItemBehavior returnBehavior = ReturnItemBehavior.DoNotReturn,
        DatabaseAttributeCondition? condition = null,
        CancellationToken cancellationToken = default)
    {
        return await PutOrUpdateItemAsync(PutOrUpdateItemType.UpdateItem, tableName, keyName, keyValue, updateData,
            returnBehavior, condition, false, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<OperationResult<JObject?>> DeleteItemAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        ReturnItemBehavior returnBehavior = ReturnItemBehavior.DoNotReturn,
        DatabaseAttributeCondition? condition = null,
        CancellationToken cancellationToken = default)
    {
        await using var mutex = await CreateFileMutexScopeAsync(cancellationToken);
        try
        {
            var filePath = GetItemFilePath(tableName, keyName, keyValue);
            var existingItem = await ReadItemFromFileAsync(filePath, cancellationToken);

            if (existingItem == null)
            {
                return OperationResult<JObject?>.Success(null);
            }

            if (condition != null && !EvaluateCondition(existingItem, condition))
            {
                return OperationResult<JObject?>.Failure("Condition not satisfied");
            }

            JObject? returnItem = null;
            if (returnBehavior == ReturnItemBehavior.ReturnOldValues)
            {
                returnItem = new JObject(existingItem);
                AddKeyToJson(returnItem, keyName, keyValue);
                ApplyOptions(returnItem);
            }

            return FileSystemUtilities.DeleteFileAndCleanupParentFolders(filePath, RootFolderName)
                ? OperationResult<JObject?>.Success(returnItem)
                : OperationResult<JObject?>.Failure("Failed to delete item");
        }
        catch (Exception ex)
        {
            return OperationResult<JObject?>.Failure($"DatabaseServiceBasic->DeleteItemAsync: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<JObject?>> AddElementsToArrayAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        string arrayAttributeName,
        PrimitiveType[] elementsToAdd,
        ReturnItemBehavior returnBehavior = ReturnItemBehavior.DoNotReturn,
        DatabaseAttributeCondition? condition = null,
        CancellationToken cancellationToken = default)
    {
        await using var mutex = await CreateFileMutexScopeAsync(cancellationToken);
        try
        {
            if (elementsToAdd.Length == 0)
            {
                return OperationResult<JObject?>.Failure("ElementsToAdd must contain values.");
            }

            var expectedKind = elementsToAdd[0].Kind;
            if (elementsToAdd.Any(element => element.Kind != expectedKind))
            {
                return OperationResult<JObject?>.Failure("All elements must have the same type.");
            }

            var filePath = GetItemFilePath(tableName, keyName, keyValue);
            var existingItem = await ReadItemFromFileAsync(filePath, cancellationToken);

            if (existingItem != null && condition != null && !EvaluateCondition(existingItem, condition))
            {
                return OperationResult<JObject?>.Failure("Condition not satisfied");
            }

            JObject? returnItem = null;
            if (returnBehavior == ReturnItemBehavior.ReturnOldValues && existingItem != null)
            {
                returnItem = new JObject(existingItem);
                AddKeyToJson(returnItem, keyName, keyValue);
                ApplyOptions(returnItem);
            }

            existingItem ??= new JObject();

            if (!existingItem.TryGetValue(arrayAttributeName, out var arrayToken))
            {
                arrayToken = new JArray();
                existingItem[arrayAttributeName] = arrayToken;
            }

            if (arrayToken is JArray array)
            {
                foreach (var element in elementsToAdd)
                {
                    var jToken = PrimitiveTypeToJToken(element);
                    array.Add(jToken);
                }
            }

            if (!await WriteItemToFileAsync(filePath, existingItem, cancellationToken))
            {
                return OperationResult<JObject?>.Failure("Failed to write item");
            }

            if (returnBehavior == ReturnItemBehavior.ReturnNewValues)
            {
                returnItem = new JObject(existingItem);
                AddKeyToJson(returnItem, keyName, keyValue);
                ApplyOptions(returnItem);
            }

            return OperationResult<JObject?>.Success(returnItem);
        }
        catch (Exception ex)
        {
            return OperationResult<JObject?>.Failure($"DatabaseServiceBasic->AddElementsToArrayAsync: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<JObject?>> RemoveElementsFromArrayAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        string arrayAttributeName,
        PrimitiveType[] elementsToRemove,
        ReturnItemBehavior returnBehavior = ReturnItemBehavior.DoNotReturn,
        DatabaseAttributeCondition? condition = null,
        CancellationToken cancellationToken = default)
    {
        await using var mutex = await CreateFileMutexScopeAsync(cancellationToken);
        try
        {
            if (elementsToRemove.Length == 0)
            {
                return OperationResult<JObject?>.Failure("ElementsToRemove must contain values.");
            }

            var expectedKind = elementsToRemove[0].Kind;
            if (elementsToRemove.Any(element => element.Kind != expectedKind))
            {
                return OperationResult<JObject?>.Failure("All elements must have the same type.");
            }

            var filePath = GetItemFilePath(tableName, keyName, keyValue);
            var existingItem = await ReadItemFromFileAsync(filePath, cancellationToken);

            if (existingItem == null)
            {
                return OperationResult<JObject?>.Success(null);
            }

            if (condition != null && !EvaluateCondition(existingItem, condition))
            {
                return OperationResult<JObject?>.Failure("Condition not satisfied");
            }

            JObject? returnItem = null;
            if (returnBehavior == ReturnItemBehavior.ReturnOldValues)
            {
                returnItem = new JObject(existingItem);
                AddKeyToJson(returnItem, keyName, keyValue);
                ApplyOptions(returnItem);
            }

            if (existingItem.TryGetValue(arrayAttributeName, out var arrayToken) && arrayToken is JArray array)
            {
                foreach (var element in elementsToRemove)
                {
                    var jToken = PrimitiveTypeToJToken(element);
                    var toRemove = array.Where(item => JToken.DeepEquals(item, jToken)).ToList();
                    foreach (var item in toRemove)
                    {
                        array.Remove(item);
                    }
                }
            }

            await WriteItemToFileAsync(filePath, existingItem, cancellationToken);

            if (returnBehavior == ReturnItemBehavior.ReturnNewValues)
            {
                returnItem = new JObject(existingItem);
                AddKeyToJson(returnItem, keyName, keyValue);
                ApplyOptions(returnItem);
            }

            return OperationResult<JObject?>.Success(returnItem);
        }
        catch (Exception ex)
        {
            return OperationResult<JObject?>.Failure($"DatabaseServiceBasic->RemoveElementsFromArrayAsync: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<double>> IncrementAttributeAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        string numericAttributeName,
        double incrementValue,
        DatabaseAttributeCondition? condition = null,
        CancellationToken cancellationToken = default)
    {
        await using var mutex = await CreateFileMutexScopeAsync(cancellationToken);
        try
        {
            var filePath = GetItemFilePath(tableName, keyName, keyValue);
            var existingItem = await ReadItemFromFileAsync(filePath, cancellationToken);

            if (existingItem != null && condition != null && !EvaluateCondition(existingItem, condition))
            {
                return OperationResult<double>.Failure("Condition not satisfied");
            }

            existingItem ??= new JObject();

            var currentValue = 0.0;
            if (existingItem.TryGetValue(numericAttributeName, out var token))
            {
                currentValue = token.Type switch
                {
                    JTokenType.Integer => token.Value<long>(),
                    JTokenType.Float => token.Value<double>(),
                    _ => currentValue
                };
            }

            var newValue = currentValue + incrementValue;
            existingItem[numericAttributeName] = newValue;

            await WriteItemToFileAsync(filePath, existingItem, cancellationToken);

            return OperationResult<double>.Success(newValue);
        }
        catch (Exception ex)
        {
            return OperationResult<double>.Failure($"DatabaseServiceBasic->IncrementAttributeAsync: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<IReadOnlyList<JObject>>> ScanTableAsync(
        string tableName,
        string[] keyNames,
        CancellationToken cancellationToken = default)
    {
        await using var mutex = await CreateFileMutexScopeAsync(cancellationToken);
        return await InternalScanTableUnsafeAsync(tableName, keyNames, cancellationToken);
    }
    private async Task<OperationResult<IReadOnlyList<JObject>>> InternalScanTableUnsafeAsync(
        string tableName,
        string[] keyNames,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var tablePath = GetTablePath(tableName);
            if (!Directory.Exists(tablePath))
            {
                return OperationResult<IReadOnlyList<JObject>>.Success([]);
            }

            var files = Directory.GetFiles(tablePath, "*.json");
            var results = new ConcurrentBag<JObject>();

            await Task.Run(() =>
            {
                Parallel.ForEach(files, filePath =>
                {
                    try
                    {
                        var item = ReadItemFromFileAsync(filePath, cancellationToken).Result;
                        if (item == null) return;

                        // Try to extract the key from the filename
                        var fileName = Path.GetFileNameWithoutExtension(filePath);
                        foreach (var keyName in keyNames)
                        {
                            if (!fileName.StartsWith($"{keyName}_")) continue;

                            var keyValueString = fileName.Substring($"{keyName}_".Length);
                            if (!TryParseKeyValue(keyValueString, out var keyValue)) continue;

                            AddKeyToJson(item, keyName, keyValue);
                            break;
                        }

                        ApplyOptions(item);
                        results.Add(item);
                    }
                    catch (Exception)
                    {
                        // Skip corrupted files
                    }
                });
            }, cancellationToken);

            return OperationResult<IReadOnlyList<JObject>>.Success(results.ToList().AsReadOnly());
        }
        catch (Exception e)
        {
            return OperationResult<IReadOnlyList<JObject>>.Failure($"DatabaseServiceBasic->InternalScanTableUnsafeAsync: {e.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<(IReadOnlyList<JObject> Items, string? NextPageToken, long? TotalCount)>> ScanTablePaginatedAsync(
        string tableName,
        string[] keyNames,
        int pageSize,
        string? pageToken = null,
        CancellationToken cancellationToken = default)
    {
        await using var mutex = await CreateFileMutexScopeAsync(cancellationToken);
        try
        {
            var allItemsResult = await InternalScanTableUnsafeAsync(tableName, keyNames, cancellationToken);
            if (!allItemsResult.IsSuccessful)
            {
                return OperationResult<(IReadOnlyList<JObject>, string?, long?)>.Failure(allItemsResult.ErrorMessage.NotNull());
            }

            var allItems = allItemsResult.Data.NotNull();
            var totalCount = allItems.Count;

            var skip = 0;
            if (int.TryParse(pageToken, out var pageTokenInt))
            {
                skip = pageTokenInt;
            }

            var pagedItems = allItems.Skip(skip).Take(pageSize).ToList();
            var nextPageToken = skip + pageSize < totalCount ? (skip + pageSize).ToString() : null;

            return OperationResult<(IReadOnlyList<JObject>, string?, long?)>.Success(
                (pagedItems.AsReadOnly(), nextPageToken, totalCount));
        }
        catch (Exception e)
        {
            return OperationResult<(IReadOnlyList<JObject>, string?, long?)>.Failure(
                $"DatabaseServiceBasic->ScanTablePaginatedAsync: {e.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<IReadOnlyList<JObject>>> ScanTableWithFilterAsync(
        string tableName,
        string[] keyNames,
        DatabaseAttributeCondition filterCondition,
        CancellationToken cancellationToken = default)
    {
        await using var mutex = await CreateFileMutexScopeAsync(cancellationToken);
        try
        {
            var allItemsResult = await InternalScanTableUnsafeAsync(tableName, keyNames, cancellationToken);
            if (!allItemsResult.IsSuccessful)
            {
                return OperationResult<IReadOnlyList<JObject>>.Failure(allItemsResult.ErrorMessage.NotNull());
            }

            var filteredItems = allItemsResult.Data.NotNull().Where(item => EvaluateCondition(item, filterCondition)).ToList();
            return OperationResult<IReadOnlyList<JObject>>.Success(filteredItems.AsReadOnly());
        }
        catch (Exception e)
        {
            return OperationResult<IReadOnlyList<JObject>>.Failure(
                $"DatabaseServiceBasic->ScanTableWithFilterAsync: {e.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<(IReadOnlyList<JObject> Items, string? NextPageToken, long? TotalCount)>> ScanTableWithFilterPaginatedAsync(
        string tableName,
        string[] keyNames,
        DatabaseAttributeCondition filterCondition,
        int pageSize,
        string? pageToken = null,
        CancellationToken cancellationToken = default)
    {
        await using var mutex = await CreateFileMutexScopeAsync(cancellationToken);
        try
        {
            var filteredItemsResult = await ScanTableWithFilterAsync(tableName, keyNames, filterCondition, cancellationToken);
            if (!filteredItemsResult.IsSuccessful)
            {
                return OperationResult<(IReadOnlyList<JObject>, string?, long?)>.Failure(filteredItemsResult.ErrorMessage.NotNull());
            }

            var filteredItems = filteredItemsResult.Data.NotNull();
            var totalCount = filteredItems.Count;

            var skip = 0;
            if (int.TryParse(pageToken, out var pageTokenInt))
            {
                skip = pageTokenInt;
            }

            var pagedItems = filteredItems.Skip(skip).Take(pageSize).ToList();
            var nextPageToken = skip + pageSize < totalCount ? (skip + pageSize).ToString() : null;

            return OperationResult<(IReadOnlyList<JObject>, string?, long?)>.Success(
                (pagedItems.AsReadOnly(), nextPageToken, totalCount));
        }
        catch (Exception e)
        {
            return OperationResult<(IReadOnlyList<JObject>, string?, long?)>.Failure(
                $"DatabaseServiceBasic->ScanTableWithFilterPaginatedAsync: {e.Message}");
        }
    }

    #endregion

    #region Private Helper Methods

    private enum PutOrUpdateItemType
    {
        PutItem,
        UpdateItem
    }

    private async Task<OperationResult<JObject?>> PutOrUpdateItemAsync(
        PutOrUpdateItemType putOrUpdateItemType,
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        JObject newItem,
        ReturnItemBehavior returnBehavior = ReturnItemBehavior.DoNotReturn,
        DatabaseAttributeCondition? conditionExpression = null,
        bool shouldOverrideIfExists = false,
        CancellationToken cancellationToken = default)
    {
        await using var mutex = await CreateFileMutexScopeAsync(cancellationToken);
        try
        {
            if (!EnsureTableExists(tableName))
                return OperationResult<JObject?>.Failure($"Could not create table '{tableName}'");

            var filePath = GetItemFilePath(tableName, keyName, keyValue);
            var existingItem = await ReadItemFromFileAsync(filePath, cancellationToken);

            if (putOrUpdateItemType == PutOrUpdateItemType.PutItem)
            {
                if (!shouldOverrideIfExists && existingItem != null)
                {
                    return OperationResult<JObject?>.Failure("Item already exists");
                }
            }
            else // UpdateItem
            {
                if (conditionExpression != null && existingItem != null && !EvaluateCondition(existingItem, conditionExpression))
                {
                    return OperationResult<JObject?>.Failure("Condition not satisfied");
                }
            }

            JObject? returnItem = null;
            if (returnBehavior == ReturnItemBehavior.ReturnOldValues && existingItem != null)
            {
                returnItem = new JObject(existingItem);
                AddKeyToJson(returnItem, keyName, keyValue);
                ApplyOptions(returnItem);
            }

            var itemToSave = putOrUpdateItemType == PutOrUpdateItemType.UpdateItem && existingItem != null
                ? MergeJObjects(existingItem, newItem)
                : new JObject(newItem);

            // Don't store the key in the file itself
            itemToSave.Remove(keyName);

            await WriteItemToFileAsync(filePath, itemToSave, cancellationToken);

            if (returnBehavior == ReturnItemBehavior.ReturnNewValues)
            {
                returnItem = new JObject(itemToSave);
                AddKeyToJson(returnItem, keyName, keyValue);
                ApplyOptions(returnItem);
            }

            return OperationResult<JObject?>.Success(returnItem);
        }
        catch (Exception ex)
        {
            return OperationResult<JObject?>.Failure($"DatabaseServiceBasic->PutOrUpdateItemAsync: {ex.Message}");
        }
    }

    private static JObject MergeJObjects(JObject target, JObject source)
    {
        var result = new JObject(target);
        foreach (var property in source.Properties())
        {
            result[property.Name] = property.Value;
        }
        return result;
    }

    private static JToken PrimitiveTypeToJToken(PrimitiveType primitive)
    {
        return primitive.Kind switch
        {
            PrimitiveTypeKind.String => primitive.AsString,
            PrimitiveTypeKind.Integer => primitive.AsInteger,
            PrimitiveTypeKind.Double => primitive.AsDouble,
            PrimitiveTypeKind.ByteArray => Convert.ToBase64String(primitive.AsByteArray),
            _ => primitive.ToString()
        };
    }

    private static bool TryParseKeyValue(string keyValueString, out PrimitiveType keyValue)
    {
        keyValue = new PrimitiveType("");

        // Try parsing as long first
        if (long.TryParse(keyValueString, NumberStyles.Integer, CultureInfo.InvariantCulture, out var longValue))
        {
            keyValue = new PrimitiveType(longValue);
            return true;
        }

        // Try parsing as double
        if (double.TryParse(keyValueString, NumberStyles.Float, CultureInfo.InvariantCulture, out var doubleValue))
        {
            keyValue = new PrimitiveType(doubleValue);
            return true;
        }

        // Try parsing as base64 byte array
        try
        {
            var bytes = Convert.FromBase64String(keyValueString);
            keyValue = new PrimitiveType(bytes);
            return true;
        }
        catch (FormatException)
        {
            // Not base64, continue
        }

        // Default to string
        keyValue = new PrimitiveType(keyValueString);
        return true;
    }

    private void ApplyOptions(JObject jsonObject)
    {
        if (Options.AutoSortArrays == AutoSortArrays.Yes)
        {
            JsonUtilities.SortJObject(jsonObject, Options.AutoConvertRoundableFloatToInt == AutoConvertRoundableFloatToInt.Yes);
        }
        else if (Options.AutoConvertRoundableFloatToInt == AutoConvertRoundableFloatToInt.Yes)
        {
            JsonUtilities.ConvertRoundFloatToIntAllInJObject(jsonObject);
        }
    }

    private async Task<MemoryServiceScopeMutex> CreateFileMutexScopeAsync(
        CancellationToken cancellationToken)
    {
        return await MemoryServiceScopeMutex.CreateScopeAsync(
            _memoryService,
            FileMutexScope,
            _databaseName,
            TimeSpan.FromMinutes(1),
            cancellationToken);
    }
    private static readonly IMemoryServiceScope FileMutexScope = new LambdaMemoryServiceScope("CrossCloudKit.Database.Basic.DatabaseServiceBasic");

    #endregion

    #region Condition Builders

    public DatabaseAttributeCondition BuildAttributeExistsCondition(string attributeName) =>
        new ExistenceCondition(DatabaseAttributeConditionType.AttributeExists, attributeName);
    public DatabaseAttributeCondition BuildAttributeNotExistsCondition(string attributeName) =>
        new ExistenceCondition(DatabaseAttributeConditionType.AttributeNotExists, attributeName);
    public DatabaseAttributeCondition BuildAttributeEqualsCondition(string attributeName, PrimitiveType value) =>
        new ValueCondition(DatabaseAttributeConditionType.AttributeEquals, attributeName, value);
    public DatabaseAttributeCondition BuildAttributeNotEqualsCondition(string attributeName, PrimitiveType value) =>
        new ValueCondition(DatabaseAttributeConditionType.AttributeNotEquals, attributeName, value);
    public DatabaseAttributeCondition BuildAttributeGreaterCondition(string attributeName, PrimitiveType value) =>
        new ValueCondition(DatabaseAttributeConditionType.AttributeGreater, attributeName, value);
    public DatabaseAttributeCondition BuildAttributeGreaterOrEqualCondition(string attributeName, PrimitiveType value) =>
        new ValueCondition(DatabaseAttributeConditionType.AttributeGreaterOrEqual, attributeName, value);
    public DatabaseAttributeCondition BuildAttributeLessCondition(string attributeName, PrimitiveType value) =>
        new ValueCondition(DatabaseAttributeConditionType.AttributeLess, attributeName, value);
    public DatabaseAttributeCondition BuildAttributeLessOrEqualCondition(string attributeName, PrimitiveType value) =>
        new ValueCondition(DatabaseAttributeConditionType.AttributeLessOrEqual, attributeName, value);
    public DatabaseAttributeCondition BuildArrayElementExistsCondition(string attributeName, PrimitiveType elementValue) =>
        new ArrayElementCondition(DatabaseAttributeConditionType.ArrayElementExists, attributeName, elementValue);
    public DatabaseAttributeCondition BuildArrayElementNotExistsCondition(string attributeName, PrimitiveType elementValue) =>
        new ArrayElementCondition(DatabaseAttributeConditionType.ArrayElementNotExists, attributeName, elementValue);

    #endregion

    public void Dispose()
    {
        // No explicit cleanup needed for file operations
        // ReSharper disable once GCSuppressFinalizeForTypeWithoutDestructor
        GC.SuppressFinalize(this);
    }
}
