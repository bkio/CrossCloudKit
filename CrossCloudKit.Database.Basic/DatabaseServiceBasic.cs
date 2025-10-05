// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

//
// Note: It's essential that methods defined in IDatabaseService are not called directly from ...CoreAsync methods.
// Instead, call CoreAsync methods when needed.
//

using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Text;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Interfaces.Enums;
using CrossCloudKit.Utilities.Common;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CrossCloudKit.Database.Basic;

public sealed class DatabaseServiceBasic : DatabaseServiceBase, IDisposable
{
    /// <summary>
    /// Holds initialization success
    /// </summary>
    private readonly bool _initializationSucceed;

    /// <summary>
    /// Gets a value indicating whether the database service has been successfully initialized.
    /// </summary>
    // ReSharper disable once ConvertToAutoProperty
    public override bool IsInitialized => _initializationSucceed;

    private readonly string _databasePath;

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
        Action<string>? errorMessageAction = null) : base(memoryService)
    {
        _databasePath = "";

        ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);

        try
        {
            basePath ??= Path.GetTempPath();
            _databasePath = Path.Combine(basePath, RootFolderName, databaseName.MakeValidFileName());

            // Ensure database directory exists
            if (!Directory.Exists(_databasePath))
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

    private string GetItemFilePath(string tableName, string keyName, Primitive keyValue)
    {
        var tablePath = GetTablePath(tableName);
        var keyString = keyValue.Kind switch
        {
            PrimitiveKind.String => keyValue.AsString,
            PrimitiveKind.Boolean => keyValue.AsBoolean.ToString(CultureInfo.InvariantCulture),
            PrimitiveKind.Integer => keyValue.AsInteger.ToString(CultureInfo.InvariantCulture),
            PrimitiveKind.Double => keyValue.AsDouble.ToString(CultureInfo.InvariantCulture),
            PrimitiveKind.ByteArray => Convert.ToBase64String(keyValue.AsByteArray),
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

    private static async Task<OperationResult<bool>> WriteItemToFileAsync(string filePath, JObject item, CancellationToken cancellationToken = default)
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

            return OperationResult<bool>.Success(true);
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure($"WriteItemToFileAsync has failed with: {e.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <summary>
    /// Parses an attribute path into segments for nested object navigation.
    /// Supports dot notation like "User.Name" or "Account.Settings.Theme"
    /// </summary>
    private static string[] ParseAttributePath(string attributePath)
    {
        if (string.IsNullOrEmpty(attributePath))
            return [];

        // Validate that path doesn't contain array indexing syntax
        if (attributePath.Contains('[') || attributePath.Contains(']'))
        {
            throw new ArgumentException(
                $"Array indexing syntax (e.g., 'array[0]') is not supported. " +
                $"Use ArrayElementExists() or ArrayElementNotExists() conditions instead. Path: '{attributePath}'");
        }

        // Split on dots and validate each segment
        var segments = attributePath.Split('.', StringSplitOptions.RemoveEmptyEntries);

        foreach (var segment in segments)
        {
            if (string.IsNullOrWhiteSpace(segment))
                throw new ArgumentException($"Empty segment in attribute path: '{attributePath}'");

            // Validate segment contains only valid characters
            if (segment.Any(c => char.IsControl(c) || c == '"' || c == '\'' || c == '`'))
                throw new ArgumentException($"Invalid characters in attribute segment '{segment}' in path: '{attributePath}'");
        }

        return segments;
    }

    /// <summary>
    /// Navigates to a nested property in a JSON object using dot notation.
    /// Examples: "User.Name", "Account.Settings.Theme"
    /// </summary>
    private static JToken? NavigateToNestedProperty(JObject jsonObject, string attributePath)
    {
        if (string.IsNullOrEmpty(attributePath))
            return null;

        try
        {
            var pathSegments = ParseAttributePath(attributePath);
            JToken current = jsonObject;

            foreach (var segment in pathSegments)
            {
                if (current is JObject currentObj)
                {
                    if (!currentObj.TryGetValue(segment, out var next))
                        return null;
                    current = next;
                }
                else
                {
                    return null;
                }
            }

            return current;
        }
        catch (ArgumentException)
        {
            return null;
        }
    }

    /// <summary>
    /// Checks if a nested property exists in a JSON object
    /// </summary>
    private static bool NestedPropertyExists(JObject jsonObject, string attributePath)
    {
        if (string.IsNullOrEmpty(attributePath))
            return false;

        return NavigateToNestedProperty(jsonObject, attributePath) != null;
    }

    /// <summary>
    /// Validates an attribute name for use in conditions and operations
    /// </summary>
    private static bool IsValidAttributePath(string attributePath)
    {
        if (string.IsNullOrEmpty(attributePath))
            return false;

        // Handle size() function syntax
        if (attributePath.StartsWith("size(") && attributePath.EndsWith(")"))
        {
            var innerAttribute = attributePath[5..^1];
            return !string.IsNullOrWhiteSpace(innerAttribute) && IsValidAttributePath(innerAttribute);
        }

        // Basic format checks
        if (attributePath.StartsWith('.') || attributePath.EndsWith('.') || attributePath.Contains(".."))
            return false;

        // Reject array indexing syntax
        if (attributePath.Contains('[') || attributePath.Contains(']'))
            return false;

        // Check for invalid characters
        if (attributePath.Any(c => char.IsControl(c) || c == '\n' || c == '\r' || c == '\t'))
            return false;

        try
        {
            var segments = ParseAttributePath(attributePath);
            if (segments.Length == 0)
                return false;

            foreach (var segment in segments)
            {
                if (string.IsNullOrWhiteSpace(segment) || segment.Length > 255)
                    return false;
            }

            return true;
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    /// <summary>
    /// Validates an attribute name and throws if invalid
    /// </summary>
    private static void ValidateAttributeName(string attributeName)
    {
        if (!IsValidAttributePath(attributeName))
        {
            throw new ArgumentException($"Invalid attribute name: '{attributeName}'. " +
                "Attribute names must be well-formed paths for nested objects. " +
                "Examples: 'Name', 'User.Email', 'User.Address.Street', 'size(Tags)'. " +
                "For arrays, use ArrayElementExists or ArrayElementNotExists conditions.",
                nameof(attributeName));
        }
    }

    private static bool EvaluateCondition(JObject item, ConditionCoupling condition)
    {
        return condition.CouplingType switch
        {
            ConditionCouplingType.Empty => true,
            ConditionCouplingType.Single => condition.SingleCondition != null && EvaluateSingleCondition(item, condition.SingleCondition),
            ConditionCouplingType.And => condition is { First: not null, Second: not null } &&
                                           EvaluateCondition(item, condition.First) && EvaluateCondition(item, condition.Second),
            ConditionCouplingType.Or => condition is { First: not null, Second: not null } &&
                                          (EvaluateCondition(item, condition.First) || EvaluateCondition(item, condition.Second)),
            _ => false
        };
    }

    private static bool EvaluateSingleCondition(JObject item, Condition condition)
    {
        // Handle size() function specially
        if (condition.AttributeName.StartsWith("size(") && condition.AttributeName.EndsWith(')'))
        {
            var innerAttribute = condition.AttributeName[5..^1];
            var token = NavigateToNestedProperty(item, innerAttribute);
            if (token is not JArray array) return false;

            var arraySize = array.Count;
            if (condition is not ValueCondition valueCondition) return false;

            var expectedSize = valueCondition.Value.AsInteger;
            return condition.ConditionType switch
            {
                ConditionType.AttributeEquals => arraySize == expectedSize,
                ConditionType.AttributeNotEquals => arraySize != expectedSize,
                ConditionType.AttributeGreater => arraySize > expectedSize,
                ConditionType.AttributeGreaterOrEqual => arraySize >= expectedSize,
                ConditionType.AttributeLess => arraySize < expectedSize,
                ConditionType.AttributeLessOrEqual => arraySize <= expectedSize,
                _ => false
            };
        }

        return condition.ConditionType switch
        {
            ConditionType.AttributeExists => NestedPropertyExists(item, condition.AttributeName),
            ConditionType.AttributeNotExists => !NestedPropertyExists(item, condition.AttributeName),
            _ when condition is ValueCondition valueCondition => EvaluateValueCondition(item, valueCondition),
            _ when condition is ArrayCondition arrayCondition => EvaluateArrayElementCondition(item, arrayCondition),
            _ => false
        };
    }

    private static bool EvaluateValueCondition(JObject item, ValueCondition condition)
    {
        var token = NavigateToNestedProperty(item, condition.AttributeName);
        if (token == null)
        {
            return false;
        }

        var itemValue = TokenToPrimitive(token);
        var conditionValue = condition.Value;

        return condition.ConditionType switch
        {
            ConditionType.AttributeEquals => ComparePrimitives(itemValue, conditionValue) == 0,
            ConditionType.AttributeNotEquals => ComparePrimitives(itemValue, conditionValue) != 0,
            ConditionType.AttributeGreater => ComparePrimitives(itemValue, conditionValue) > 0,
            ConditionType.AttributeGreaterOrEqual => ComparePrimitives(itemValue, conditionValue) >= 0,
            ConditionType.AttributeLess => ComparePrimitives(itemValue, conditionValue) < 0,
            ConditionType.AttributeLessOrEqual => ComparePrimitives(itemValue, conditionValue) <= 0,
            _ => false
        };
    }

    private static bool EvaluateArrayElementCondition(JObject item, ArrayCondition condition)
    {
        var token = NavigateToNestedProperty(item, condition.AttributeName);
        if (token is not JArray array)
        {
            return condition.ConditionType == ConditionType.ArrayElementNotExists;
        }

        var elementExists = array.Any(element =>
        {
            var arrayElementValue = TokenToPrimitive(element);
            return ComparePrimitives(arrayElementValue, condition.ElementValue) == 0;
        });

        return condition.ConditionType == ConditionType.ArrayElementExists ? elementExists : !elementExists;
    }

    private static Primitive TokenToPrimitive(JToken token)
    {
        return token.Type switch
        {
            JTokenType.String => new Primitive(token.Value<string>().NotNull()),
            JTokenType.Integer => new Primitive(token.Value<long>()),
            JTokenType.Float => new Primitive(token.Value<double>()),
            JTokenType.Boolean => new Primitive(token.Value<bool>()),
            _ => new Primitive(token.ToString())
        };
    }

    private static int ComparePrimitives(Primitive a, Primitive b)
    {
        if (a.Kind != b.Kind)
        {
            return string.Compare(a.ToString(), b.ToString(), StringComparison.Ordinal);
        }

        return a.Kind switch
        {
            PrimitiveKind.String => string.Compare(a.AsString, b.AsString, StringComparison.Ordinal),
            PrimitiveKind.Boolean => a.AsBoolean.CompareTo(b.AsBoolean),
            PrimitiveKind.Integer => a.AsInteger.CompareTo(b.AsInteger),
            PrimitiveKind.Double => a.AsDouble.CompareTo(b.AsDouble),
            PrimitiveKind.ByteArray => CompareByteArrays(a.AsByteArray, b.AsByteArray),
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
    protected override async Task<OperationResult<bool>> ItemExistsCoreAsync(
        string tableName,
        DbKey key,
        ConditionCoupling? conditions = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var filePath = GetItemFilePath(tableName, key.Name, key.Value);
            var item = await ReadItemFromFileAsync(filePath, cancellationToken);

            if (item == null)
                return OperationResult<bool>.Failure("Item not found", HttpStatusCode.NotFound);

            if (conditions != null && !EvaluateCondition(item, conditions))
                return OperationResult<bool>.Failure("Conditions are not satisfied.", HttpStatusCode.PreconditionFailed);

            return OperationResult<bool>.Success(true);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure($"DatabaseServiceBasic->ItemExistsAsync: {ex.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<JObject?>> GetItemCoreAsync(
        string tableName,
        DbKey key,
        string[]? attributesToRetrieve = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var filePath = GetItemFilePath(tableName, key.Name, key.Value);
            var item = await ReadItemFromFileAsync(filePath, cancellationToken);

            if (item == null)
            {
                return OperationResult<JObject?>.Success(null);
            }

            // Add the key to the result
            AddKeyToJson(item, key.Name, key.Value);
            ApplyOptions(item);

            return OperationResult<JObject?>.Success(item);
        }
        catch (Exception ex)
        {
            return OperationResult<JObject?>.Failure($"DatabaseServiceBasic->GetItemAsync: {ex.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<IReadOnlyList<JObject>>> GetItemsCoreAsync(
        string tableName,
        DbKey[] keys,
        string[]? attributesToRetrieve = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (keys.Length == 0)
            {
                return OperationResult<IReadOnlyList<JObject>>.Success([]);
            }

            var results = new List<JObject>();

            foreach (var key in keys)
            {
                var filePath = GetItemFilePath(tableName, key.Name, key.Value);
                var item = await ReadItemFromFileAsync(filePath, cancellationToken);

                if (item == null) continue;

                AddKeyToJson(item, key.Name, key.Value);
                ApplyOptions(item);
                results.Add(item);
            }

            return OperationResult<IReadOnlyList<JObject>>.Success(results.AsReadOnly());
        }
        catch (Exception e)
        {
            return OperationResult<IReadOnlyList<JObject>>.Failure($"DatabaseServiceBasic->GetItemsAsync: {e.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<JObject?>> PutItemCoreAsync(
        string tableName,
        DbKey key,
        JObject item,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        bool overwriteIfExists = false,
        CancellationToken cancellationToken = default)
    {
        return await PutOrUpdateItemAsync(
            PutOrUpdateItemType.PutItem,
            tableName,
            key,
            item,
            returnBehavior,
            null,
            overwriteIfExists,
            cancellationToken);
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<JObject?>> UpdateItemCoreAsync(
        string tableName,
        DbKey key,
        JObject updateData,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        ConditionCoupling? conditions = null,
        CancellationToken cancellationToken = default)
    {
        return await PutOrUpdateItemAsync(
            PutOrUpdateItemType.UpdateItem,
            tableName,
            key,
            updateData,
            returnBehavior,
            conditions,
            false,
            cancellationToken);
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<JObject?>> DeleteItemCoreAsync(
        string tableName,
        DbKey key,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        ConditionCoupling? conditions = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var filePath = GetItemFilePath(tableName, key.Name, key.Value);
            var existingItem = await ReadItemFromFileAsync(filePath, cancellationToken);

            if (existingItem == null)
            {
                return OperationResult<JObject?>.Success(null);
            }

            if (conditions != null && !EvaluateCondition(existingItem, conditions))
            {
                return OperationResult<JObject?>.Failure("Condition not satisfied", HttpStatusCode.PreconditionFailed);
            }

            JObject? returnItem = null;
            if (returnBehavior == DbReturnItemBehavior.ReturnOldValues)
            {
                returnItem = new JObject(existingItem);
                AddKeyToJson(returnItem, key.Name, key.Value);
                ApplyOptions(returnItem);
            }

            return FileSystemUtilities.DeleteFileAndCleanupParentFolders(filePath, RootFolderName)
                ? OperationResult<JObject?>.Success(returnItem)
                : OperationResult<JObject?>.Failure("Failed to delete item", HttpStatusCode.InternalServerError);
        }
        catch (Exception ex)
        {
            return OperationResult<JObject?>.Failure($"DatabaseServiceBasic->DeleteItemAsync: {ex.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<JObject?>> AddElementsToArrayCoreAsync(
        string tableName,
        DbKey key,
        string arrayAttributeName,
        Primitive[] elementsToAdd,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        ConditionCoupling? conditions = null,
        bool isCalledFromPostInsert = false,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (elementsToAdd.Length == 0)
            {
                return OperationResult<JObject?>.Failure("ElementsToAdd must contain values.", HttpStatusCode.BadRequest);
            }

            var expectedKind = elementsToAdd[0].Kind;
            if (elementsToAdd.Any(element => element.Kind != expectedKind))
            {
                return OperationResult<JObject?>.Failure("All elements must have the same type.", HttpStatusCode.BadRequest);
            }

            if (!isCalledFromPostInsert)
            {
                var sanityCheckResult = await AttributeNamesSanityCheck(key, tableName, new JObject { [arrayAttributeName] = new JArray() }, cancellationToken);
                if (!sanityCheckResult.IsSuccessful)
                {
                    return OperationResult<JObject?>.Failure(sanityCheckResult.ErrorMessage, sanityCheckResult.StatusCode);
                }
            }

            var filePath = GetItemFilePath(tableName, key.Name, key.Value);
            var existingItem = await ReadItemFromFileAsync(filePath, cancellationToken);

            if (existingItem != null && conditions != null && !EvaluateCondition(existingItem, conditions))
            {
                return OperationResult<JObject?>.Failure("Condition not satisfied", HttpStatusCode.PreconditionFailed);
            }

            JObject? returnItem = null;
            if (returnBehavior == DbReturnItemBehavior.ReturnOldValues && existingItem != null)
            {
                returnItem = new JObject(existingItem);
                AddKeyToJson(returnItem, key.Name, key.Value);
                ApplyOptions(returnItem);
            }

            var noExistingItem = existingItem == null;
            existingItem ??= new JObject();

            // Handle nested paths
            var pathSegments = ParseAttributePath(arrayAttributeName);
            JToken current = existingItem;

            // Navigate/create intermediate objects
            for (var i = 0; i < pathSegments.Length - 1; i++)
            {
                var segment = pathSegments[i];
                if (current is not JObject currentObj)
                    return OperationResult<JObject?>.Failure($"Path segment '{segment}' is not an object", HttpStatusCode.BadRequest);

                if (!currentObj.TryGetValue(segment, out var next) || next.Type == JTokenType.Null)
                {
                    next = new JObject();
                    currentObj[segment] = next;
                }
                current = next;
            }

            // Get or create the array at the final segment
            var finalSegment = pathSegments[^1];
            if (current is not JObject finalObj)
                return OperationResult<JObject?>.Failure($"Cannot access property '{finalSegment}'", HttpStatusCode.BadRequest);

            if (!finalObj.TryGetValue(finalSegment, out var arrayToken) || arrayToken.Type == JTokenType.Null)
            {
                arrayToken = new JArray();
                finalObj[finalSegment] = arrayToken;
            }

            if (arrayToken is JArray array)
            {
                foreach (var element in elementsToAdd)
                {
                    var jToken = PrimitiveToJToken(element);
                    array.Add(jToken);
                }
            }
            else
            {
                return OperationResult<JObject?>.Failure($"Attribute '{arrayAttributeName}' exists but is not an array", HttpStatusCode.BadRequest);
            }

            var writeItemTask = WriteItemToFileAsync(filePath, existingItem, cancellationToken);
            if (noExistingItem && !isCalledFromPostInsert)
            {
                var postInsertTask = PostInsertItemAsync(tableName, key, cancellationToken);

                await Task.WhenAll(writeItemTask, postInsertTask);

                var postInsertResult = await postInsertTask;
                if (!postInsertResult.IsSuccessful)
                {
                    return OperationResult<JObject?>.Failure($"PostInsertItemAsync failed with: {postInsertResult.ErrorMessage}", postInsertResult.StatusCode);
                }
            }
            var writeItemResult = await writeItemTask;
            if (!writeItemResult.IsSuccessful)
            {
                return OperationResult<JObject?>.Failure($"Failed to write item: {writeItemResult.ErrorMessage}", writeItemResult.StatusCode);
            }

            if (returnBehavior == DbReturnItemBehavior.ReturnNewValues)
            {
                returnItem = new JObject(existingItem);
                AddKeyToJson(returnItem, key.Name, key.Value);
                ApplyOptions(returnItem);
            }

            return OperationResult<JObject?>.Success(returnItem);
        }
        catch (Exception ex)
        {
            return OperationResult<JObject?>.Failure($"DatabaseServiceBasic->AddElementsToArrayAsync: {ex.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<JObject?>> RemoveElementsFromArrayCoreAsync(
        string tableName,
        DbKey key,
        string arrayAttributeName,
        Primitive[] elementsToRemove,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        ConditionCoupling? conditions = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (elementsToRemove.Length == 0)
            {
                return OperationResult<JObject?>.Failure("ElementsToRemove must contain values.", HttpStatusCode.BadRequest);
            }

            var expectedKind = elementsToRemove[0].Kind;
            if (elementsToRemove.Any(element => element.Kind != expectedKind))
            {
                return OperationResult<JObject?>.Failure("All elements must have the same type.", HttpStatusCode.BadRequest);
            }

            var filePath = GetItemFilePath(tableName, key.Name, key.Value);
            var existingItem = await ReadItemFromFileAsync(filePath, cancellationToken);

            if (existingItem == null)
            {
                return OperationResult<JObject?>.Success(null);
            }

            if (conditions != null && !EvaluateCondition(existingItem, conditions))
            {
                return OperationResult<JObject?>.Failure("Condition not satisfied", HttpStatusCode.PreconditionFailed);
            }

            JObject? returnItem = null;
            if (returnBehavior == DbReturnItemBehavior.ReturnOldValues)
            {
                returnItem = new JObject(existingItem);
                AddKeyToJson(returnItem, key.Name, key.Value);
                ApplyOptions(returnItem);
            }

            // Handle nested paths
            var arrayToken = NavigateToNestedProperty(existingItem, arrayAttributeName);
            if (arrayToken is JArray array)
            {
                foreach (var element in elementsToRemove)
                {
                    var jToken = PrimitiveToJToken(element);
                    var toRemove = array.Where(item => JToken.DeepEquals(item, jToken)).ToList();
                    foreach (var item in toRemove)
                    {
                        array.Remove(item);
                    }
                }
            }

            var writeItemResult = await WriteItemToFileAsync(filePath, existingItem, cancellationToken);
            if (!writeItemResult.IsSuccessful)
            {
                return OperationResult<JObject?>.Failure($"WriteItemToFileAsync failed: {writeItemResult.ErrorMessage}", writeItemResult.StatusCode);
            }

            if (returnBehavior == DbReturnItemBehavior.ReturnNewValues)
            {
                returnItem = new JObject(existingItem);
                AddKeyToJson(returnItem, key.Name, key.Value);
                ApplyOptions(returnItem);
            }

            return OperationResult<JObject?>.Success(returnItem);
        }
        catch (Exception ex)
        {
            return OperationResult<JObject?>.Failure($"DatabaseServiceBasic->RemoveElementsFromArrayAsync: {ex.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<double>> IncrementAttributeCoreAsync(
        string tableName,
        DbKey key,
        string numericAttributeName,
        double incrementValue,
        ConditionCoupling? conditions = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var sanityCheckResult = await AttributeNamesSanityCheck(key, tableName, new JObject { [numericAttributeName] = new JArray() }, cancellationToken);
            if (!sanityCheckResult.IsSuccessful)
            {
                return OperationResult<double>.Failure(sanityCheckResult.ErrorMessage, sanityCheckResult.StatusCode);
            }

            var filePath = GetItemFilePath(tableName, key.Name, key.Value);
            var existingItem = await ReadItemFromFileAsync(filePath, cancellationToken);

            if (existingItem != null && conditions != null && !EvaluateCondition(existingItem, conditions))
            {
                return OperationResult<double>.Failure("Condition not satisfied", HttpStatusCode.PreconditionFailed);
            }

            var noExistingItem = existingItem == null;
            existingItem ??= new JObject();

            // Handle nested paths
            var pathSegments = ParseAttributePath(numericAttributeName);
            JToken current = existingItem;

            // Navigate/create intermediate objects
            for (var i = 0; i < pathSegments.Length - 1; i++)
            {
                var segment = pathSegments[i];
                if (current is not JObject currentObj)
                    return OperationResult<double>.Failure($"Path segment '{segment}' is not an object", HttpStatusCode.BadRequest);

                if (!currentObj.TryGetValue(segment, out var next))
                {
                    next = new JObject();
                    currentObj[segment] = next;
                }
                current = next;
            }

            // Get current value and increment
            var finalSegment = pathSegments[^1];
            if (current is not JObject finalObj)
                return OperationResult<double>.Failure($"Cannot access property '{finalSegment}'", HttpStatusCode.BadRequest);

            var currentValue = 0.0;
            if (finalObj.TryGetValue(finalSegment, out var token))
            {
                currentValue = token.Type switch
                {
                    JTokenType.Integer => token.Value<long>(),
                    JTokenType.Float => token.Value<double>(),
                    _ => currentValue
                };
            }

            var newValue = currentValue + incrementValue;
            finalObj[finalSegment] = newValue;

            var writeItemTask = WriteItemToFileAsync(filePath, existingItem, cancellationToken);
            if (noExistingItem)
            {
                var postInsertTask = PostInsertItemAsync(tableName, key, cancellationToken);

                await Task.WhenAll(writeItemTask, postInsertTask);

                var postInsertResult = await postInsertTask;
                if (!postInsertResult.IsSuccessful)
                {
                    return OperationResult<double>.Failure($"PostInsertItemAsync failed with {postInsertResult.ErrorMessage}", postInsertResult.StatusCode);
                }
            }

            var writeItemResult = await writeItemTask;
            return !writeItemResult.IsSuccessful
                ? OperationResult<double>.Failure($"WriteItemToFileAsync failed: {writeItemResult.ErrorMessage}", writeItemResult.StatusCode)
                : OperationResult<double>.Success(newValue);
        }
        catch (Exception ex)
        {
            return OperationResult<double>.Failure($"DatabaseServiceBasic->IncrementAttributeAsync: {ex.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<(IReadOnlyList<string> Keys, IReadOnlyList<JObject> Items)>> ScanTableCoreAsync(
        string tableName,
        CancellationToken cancellationToken = default)
    {
        return await InternalScanTableUnsafeAsync(tableName, cancellationToken);
    }
    private async Task<OperationResult<(IReadOnlyList<string> Keys, IReadOnlyList<JObject> Items)>> InternalScanTableUnsafeAsync(
        string tableName,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var tablePath = GetTablePath(tableName);
            if (!Directory.Exists(tablePath))
            {
                return OperationResult<(IReadOnlyList<string>, IReadOnlyList<JObject>)>.Success(([], []));
            }

            var files = Directory.GetFiles(tablePath, "*.json");
            var results = new ConcurrentBag<JObject>();

            foreach (var filePath in files)
            {
                try
                {
                    var item = ReadItemFromFileAsync(filePath, cancellationToken).Result;
                    if (item == null) continue;

                    // Try to extract the key from the filename
                    var fileName = Path.GetFileNameWithoutExtension(filePath);

                    var underscoreIndex = fileName.IndexOf('_');
                    if (underscoreIndex > 0)
                    {
                        var keyName = fileName[..underscoreIndex];
                        var keyValueString = fileName[(underscoreIndex + 1)..];

                        if (TryParseKeyValue(keyValueString, out var keyValue))
                        {
                            AddKeyToJson(item, keyName, keyValue);
                        }
                    }

                    ApplyOptions(item);
                    results.Add(item);
                }
                catch (Exception)
                {
                    // Skip corrupted files
                }
            }

            var getKeysResult = await GetTableKeysCoreAsync(tableName, cancellationToken);
            return !getKeysResult.IsSuccessful
                ? OperationResult<(IReadOnlyList<string>, IReadOnlyList<JObject>)>.Failure(getKeysResult.ErrorMessage, getKeysResult.StatusCode)
                : OperationResult<(IReadOnlyList<string>, IReadOnlyList<JObject>)>.Success((getKeysResult.Data, results.ToList().AsReadOnly()));
        }
        catch (Exception e)
        {
            return OperationResult<(IReadOnlyList<string>, IReadOnlyList<JObject>)>.Failure($"DatabaseServiceBasic->InternalScanTableUnsafeAsync: {e.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    protected override async
        Task<OperationResult<(
            IReadOnlyList<string>? Keys,
            IReadOnlyList<JObject> Items,
            string? NextPageToken,
            long? TotalCount)>> ScanTablePaginatedCoreAsync(
        string tableName,
        int pageSize,
        string? pageToken = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var allItemsResult = await InternalScanTableUnsafeAsync(tableName, cancellationToken);
            if (!allItemsResult.IsSuccessful)
            {
                return OperationResult<(IReadOnlyList<string>?, IReadOnlyList<JObject>, string?, long?)>.Failure(allItemsResult.ErrorMessage, allItemsResult.StatusCode);
            }

            var allItems = allItemsResult.Data;
            var totalCount = allItems.Items.Count;

            var skip = 0;
            if (int.TryParse(pageToken, out var pageTokenInt))
            {
                skip = pageTokenInt;
            }

            var pagedItems = allItems.Items.Skip(skip).Take(pageSize).ToList();
            var nextPageToken = skip + pageSize < totalCount ? (skip + pageSize).ToString() : null;

            return OperationResult<(IReadOnlyList<string>?, IReadOnlyList<JObject>, string?, long?)>.Success(
                (allItems.Keys, pagedItems.AsReadOnly(), nextPageToken, totalCount));
        }
        catch (Exception e)
        {
            return OperationResult<(IReadOnlyList<string>?, IReadOnlyList<JObject>, string?, long?)>.Failure(
                $"DatabaseServiceBasic->ScanTablePaginatedAsync: {e.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<(IReadOnlyList<string> Keys, IReadOnlyList<JObject> Items)>> ScanTableWithFilterCoreAsync(
        string tableName,
        ConditionCoupling filterConditions,
        CancellationToken cancellationToken = default)
    {
        return await InternalScanTableWithFilterCoreAsync(tableName, filterConditions, cancellationToken);
    }

    private async Task<OperationResult<(IReadOnlyList<string> Keys, IReadOnlyList<JObject> Items)>>
        InternalScanTableWithFilterCoreAsync(
            string tableName,
            ConditionCoupling filterConditions,
            CancellationToken cancellationToken = default)
    {
        try
        {
            var allItemsResult = await InternalScanTableUnsafeAsync(tableName, cancellationToken);
            if (!allItemsResult.IsSuccessful)
            {
                return OperationResult<(IReadOnlyList<string>, IReadOnlyList<JObject>)>.Failure(allItemsResult.ErrorMessage, allItemsResult.StatusCode);
            }

            var filteredItems = allItemsResult.Data.Items.Where(item => EvaluateCondition(item, filterConditions)).ToList();
            return OperationResult<(IReadOnlyList<string>, IReadOnlyList<JObject>)>.Success((allItemsResult.Data.Keys, filteredItems.AsReadOnly()));
        }
        catch (Exception e)
        {
            return OperationResult<(IReadOnlyList<string>, IReadOnlyList<JObject>)>.Failure(
                $"DatabaseServiceBasic->ScanTableWithFilterAsync: {e.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    protected override async
        Task<OperationResult<(
            IReadOnlyList<string>? Keys,
            IReadOnlyList<JObject> Items,
            string? NextPageToken,
            long? TotalCount)>> ScanTableWithFilterPaginatedCoreAsync(
        string tableName,
        ConditionCoupling filterConditions,
        int pageSize,
        string? pageToken = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var filteredItemsResult = await InternalScanTableWithFilterCoreAsync(tableName, filterConditions, cancellationToken);
            if (!filteredItemsResult.IsSuccessful)
            {
                return OperationResult<(IReadOnlyList<string>?, IReadOnlyList<JObject>, string?, long?)>.Failure(filteredItemsResult.ErrorMessage, filteredItemsResult.StatusCode);
            }

            var filteredItems = filteredItemsResult.Data;
            var totalCount = filteredItems.Items.Count;

            var skip = 0;
            if (int.TryParse(pageToken, out var pageTokenInt))
            {
                skip = pageTokenInt;
            }

            var pagedItems = filteredItems.Items.Skip(skip).Take(pageSize).ToList();
            var nextPageToken = skip + pageSize < totalCount ? (skip + pageSize).ToString() : null;

            return OperationResult<(IReadOnlyList<string>?, IReadOnlyList<JObject>, string?, long?)>.Success(
                (filteredItems.Keys, pagedItems.AsReadOnly(), nextPageToken, totalCount));
        }
        catch (Exception e)
        {
            return OperationResult<(IReadOnlyList<string>?, IReadOnlyList<JObject>, string?, long?)>.Failure(
                $"DatabaseServiceBasic->ScanTableWithFilterPaginatedAsync: {e.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    protected override Task<OperationResult<IReadOnlyList<string>>> GetTableNamesCoreAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            if (!Directory.Exists(_databasePath))
            {
                return Task.FromResult(OperationResult<IReadOnlyList<string>>.Success([]));
            }

            var directories = Directory.GetDirectories(_databasePath);
            var tableNames = directories
                .Select(Path.GetFileName)
                .Where(name => !string.IsNullOrEmpty(name))
                .Cast<string>()
                .Where(t => !t.StartsWith(SystemTableNamePrefix))
                .ToList();

            return Task.FromResult(OperationResult<IReadOnlyList<string>>.Success(tableNames.AsReadOnly()));
        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<IReadOnlyList<string>>.Failure($"DatabaseServiceBasic->GetTableNamesAsync: {ex.Message}", HttpStatusCode.InternalServerError));
        }
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<bool>> DropTableCoreAsync(string tableName, CancellationToken cancellationToken = default)
    {
        try
        {
            var tablePath = GetTablePath(tableName);

            // Check if table directory exists
            if (!Directory.Exists(tablePath))
            {
                // Table doesn't exist, consider this a successful deletion
                return OperationResult<bool>.Success(true);
            }

            // Delete the table directory
            var deleteTableTask = Task.Run(() =>
            {
                try
                {
                    Directory.Delete(tablePath, recursive: true);
                }
                catch (UnauthorizedAccessException ex)
                {
                    return OperationResult<bool>.Failure($"Access denied when deleting table directory: {ex.Message}",
                        HttpStatusCode.Forbidden);
                }
                catch (DirectoryNotFoundException)
                {
                    // Directory already doesn't exist, consider success
                    return OperationResult<bool>.Success(true);
                }
                catch (IOException ex)
                {
                    return OperationResult<bool>.Failure($"I/O error when deleting table directory: {ex.Message}",
                        HttpStatusCode.Conflict);
                }
                return OperationResult<bool>.Success(true);
            }, cancellationToken);

            var postDropTableTask = PostDropTableAsync(tableName, cancellationToken);

            await Task.WhenAll(deleteTableTask, postDropTableTask);

            var deleteTableResult = await deleteTableTask;
            if (!deleteTableResult.IsSuccessful)
            {
                return deleteTableResult;
            }

            var postDropTableResult = await postDropTableTask;
            if (!postDropTableResult.IsSuccessful)
            {
                return OperationResult<bool>.Failure(
                    $"PostDropTableAsync has failed with {postDropTableResult.ErrorMessage}",
                    postDropTableResult.StatusCode);
            }

            return OperationResult<bool>.Success(true);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure($"DatabaseServiceBasic->DropTableAsync: {ex.Message}", HttpStatusCode.InternalServerError);
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
        DbKey key,
        JObject newItem,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        ConditionCoupling? conditions = null,
        bool shouldOverrideIfExists = false,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var sanityCheckResult = await AttributeNamesSanityCheck(key, tableName, newItem, cancellationToken);
            if (!sanityCheckResult.IsSuccessful)
            {
                return OperationResult<JObject?>.Failure(sanityCheckResult.ErrorMessage, sanityCheckResult.StatusCode);
            }

            if (!EnsureTableExists(tableName))
                return OperationResult<JObject?>.Failure($"Could not create table '{tableName}'", HttpStatusCode.InternalServerError);

            var filePath = GetItemFilePath(tableName, key.Name, key.Value);
            var existingItem = await ReadItemFromFileAsync(filePath, cancellationToken);

            if (putOrUpdateItemType == PutOrUpdateItemType.PutItem)
            {
                if (!shouldOverrideIfExists && existingItem != null)
                {
                    return OperationResult<JObject?>.Failure("Item already exists", HttpStatusCode.Conflict);
                }
            }
            else // UpdateItem
            {
                if (conditions != null && existingItem != null && !EvaluateCondition(existingItem, conditions))
                {
                    return OperationResult<JObject?>.Failure("Condition not satisfied", HttpStatusCode.PreconditionFailed);
                }
            }

            JObject? returnItem = null;
            if (returnBehavior == DbReturnItemBehavior.ReturnOldValues && existingItem != null)
            {
                returnItem = new JObject(existingItem);
                AddKeyToJson(returnItem, key.Name, key.Value);
                ApplyOptions(returnItem);
            }

            var itemToSave = putOrUpdateItemType == PutOrUpdateItemType.UpdateItem && existingItem != null
                ? MergeJObjects(existingItem, newItem)
                : new JObject(newItem);

            // Don't store the key in the file itself
            itemToSave.Remove(key.Name);

            var writeItemTask = WriteItemToFileAsync(filePath, itemToSave, cancellationToken);

            if (putOrUpdateItemType == PutOrUpdateItemType.PutItem)
            {
                var postInsertTask = PostInsertItemAsync(tableName, key, cancellationToken);

                await Task.WhenAll(writeItemTask, postInsertTask);

                var postInsertResult = await postInsertTask;
                if (!postInsertResult.IsSuccessful)
                {
                    return OperationResult<JObject?>.Failure($"PostInsertItemAsync failed with: {postInsertResult.ErrorMessage}", postInsertResult.StatusCode);
                }
            }

            var writeItemResult = await writeItemTask;
            if (!writeItemResult.IsSuccessful)
            {
                return OperationResult<JObject?>.Failure($"WriteItemToFileAsync failed: {writeItemResult.ErrorMessage}", writeItemResult.StatusCode);
            }

            if (returnBehavior == DbReturnItemBehavior.ReturnNewValues)
            {
                returnItem = new JObject(itemToSave);
                AddKeyToJson(returnItem, key.Name, key.Value);
                ApplyOptions(returnItem);
            }

            return OperationResult<JObject?>.Success(returnItem);
        }
        catch (Exception ex)
        {
            return OperationResult<JObject?>.Failure($"DatabaseServiceBasic->PutOrUpdateItemAsync: {ex.Message}", HttpStatusCode.InternalServerError);
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

    private static JToken PrimitiveToJToken(Primitive primitive)
    {
        return primitive.Kind switch
        {
            PrimitiveKind.String => primitive.AsString,
            PrimitiveKind.Boolean => primitive.AsBoolean,
            PrimitiveKind.Integer => primitive.AsInteger,
            PrimitiveKind.Double => primitive.AsDouble,
            PrimitiveKind.ByteArray => Convert.ToBase64String(primitive.AsByteArray),
            _ => primitive.ToString()
        };
    }

    private static bool TryParseKeyValue(string keyValueString, out Primitive keyValue)
    {
        keyValue = new Primitive("");

        // Try parsing as long first
        if (long.TryParse(keyValueString, NumberStyles.Integer, CultureInfo.InvariantCulture, out var longValue))
        {
            keyValue = new Primitive(longValue);
            return true;
        }

        // Try parsing as double
        if (double.TryParse(keyValueString, NumberStyles.Float, CultureInfo.InvariantCulture, out var doubleValue))
        {
            keyValue = new Primitive(doubleValue);
            return true;
        }

        // Try parsing as base64 byte array
        try
        {
            var bytes = Convert.FromBase64String(keyValueString);
            keyValue = new Primitive(bytes);
            return true;
        }
        catch (FormatException)
        {
            // Not base64, continue
        }

        // Default to string
        keyValue = new Primitive(keyValueString);
        return true;
    }

    private void ApplyOptions(JObject jsonObject)
    {
        if (Options.AutoSortArrays == DbAutoSortArrays.Yes)
        {
            JsonUtilities.SortJObject(jsonObject, Options.AutoConvertRoundableFloatToInt == DbAutoConvertRoundableFloatToInt.Yes);
        }
        else if (Options.AutoConvertRoundableFloatToInt == DbAutoConvertRoundableFloatToInt.Yes)
        {
            JsonUtilities.ConvertRoundFloatToIntAllInJObject(jsonObject);
        }
    }

    #endregion

    #region Condition Builders

    /// <inheritdoc />
    public override Condition AttributeExists(string attributeName)
    {
        ValidateAttributeName(attributeName);
        return new ExistenceCondition(ConditionType.AttributeExists, attributeName);
    }

    /// <inheritdoc />
    public override Condition AttributeNotExists(string attributeName)
    {
        ValidateAttributeName(attributeName);
        return new ExistenceCondition(ConditionType.AttributeNotExists, attributeName);
    }

    /// <inheritdoc />
    public override Condition AttributeEquals(string attributeName, Primitive value)
    {
        ValidateAttributeName(attributeName);
        return new ValueCondition(ConditionType.AttributeEquals, attributeName, value);
    }

    /// <inheritdoc />
    public override Condition AttributeNotEquals(string attributeName, Primitive value)
    {
        ValidateAttributeName(attributeName);
        return new ValueCondition(ConditionType.AttributeNotEquals, attributeName, value);
    }

    /// <inheritdoc />
    public override Condition AttributeIsGreaterThan(string attributeName, Primitive value)
    {
        ValidateAttributeName(attributeName);
        return new ValueCondition(ConditionType.AttributeGreater, attributeName, value);
    }

    /// <inheritdoc />
    public override Condition AttributeIsGreaterOrEqual(string attributeName, Primitive value)
    {
        ValidateAttributeName(attributeName);
        return new ValueCondition(ConditionType.AttributeGreaterOrEqual, attributeName, value);
    }

    /// <inheritdoc />
    public override Condition AttributeIsLessThan(string attributeName, Primitive value)
    {
        ValidateAttributeName(attributeName);
        return new ValueCondition(ConditionType.AttributeLess, attributeName, value);
    }

    /// <inheritdoc />
    public override Condition AttributeIsLessOrEqual(string attributeName, Primitive value)
    {
        ValidateAttributeName(attributeName);
        return new ValueCondition(ConditionType.AttributeLessOrEqual, attributeName, value);
    }

    /// <inheritdoc />
    public override Condition ArrayElementExists(string attributeName, Primitive elementValue)
    {
        ValidateAttributeName(attributeName);
        return new ArrayCondition(ConditionType.ArrayElementExists, attributeName, elementValue);
    }

    /// <inheritdoc />
    public override Condition ArrayElementNotExists(string attributeName, Primitive elementValue)
    {
        ValidateAttributeName(attributeName);
        return new ArrayCondition(ConditionType.ArrayElementNotExists, attributeName, elementValue);
    }

    #endregion

    public void Dispose()
    {
        // No explicit cleanup needed for file operations
        // ReSharper disable once GCSuppressFinalizeForTypeWithoutDestructor
        GC.SuppressFinalize(this);
    }
}
