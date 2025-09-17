// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Globalization;
using CrossCloudKit.Interfaces;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Datastore.V1;
using Newtonsoft.Json.Linq;
using CrossCloudKit.Utilities.Common;

namespace CrossCloudKit.Database.GC;

public sealed class DatabaseServiceGC : DatabaseServiceBase, IDatabaseService, IAsyncDisposable
{
    /// <summary>
    /// Holds initialization success
    /// </summary>
    private readonly bool _bInitializationSucceed;

    private readonly DatastoreDb? _dsdb;
    private readonly ServiceAccountCredential? _credential;

    /// <summary>
    /// Gets a value indicating whether the database service has been successfully initialized.
    /// </summary>
    // ReSharper disable once ConvertToAutoProperty
    public bool IsInitialized => _bInitializationSucceed;

    /// <summary>
    /// DatabaseServiceGC: Constructor using service account JSON file path
    /// </summary>
    /// <param name="projectId">GC Project ID</param>
    /// <param name="serviceAccountKeyFilePath">Path to the service account JSON key file</param>
    /// <param name="errorMessageAction">Error messages will be pushed to this action</param>
    public DatabaseServiceGC(
        string projectId,
        string serviceAccountKeyFilePath,
        Action<string>? errorMessageAction = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(projectId);
        ArgumentException.ThrowIfNullOrWhiteSpace(serviceAccountKeyFilePath);

        try
        {
            using var stream = new FileStream(serviceAccountKeyFilePath, FileMode.Open, FileAccess.Read);
            _credential = GoogleCredential.FromStream(stream)
                        .CreateScoped(DatastoreClient.DefaultScopes)
                        .UnderlyingCredential as ServiceAccountCredential;

            DatastoreClient? dsClient;
            if (_credential != null)
            {
                dsClient = new DatastoreClientBuilder
                {
                    Credential = GoogleCredential.FromServiceAccountCredential(_credential)
                }.Build();
            }
            else
            {
                dsClient = DatastoreClient.Create();
            }

            _dsdb = DatastoreDb.Create(projectId, "", dsClient);
            _bInitializationSucceed = _dsdb != null;
        }
        catch (Exception e)
        {
            errorMessageAction?.Invoke($"DatabaseServiceGC->Constructor: {e.Message}, Trace: {e.StackTrace}");
            _bInitializationSucceed = false;
        }
    }

    /// <summary>
    /// DatabaseServiceGC: Constructor using service account JSON content
    /// </summary>
    /// <param name="projectId">GC Project ID</param>
    /// <param name="serviceAccountJsonContent">JSON content of the service account key</param>
    /// <param name="isBase64Encoded">Whether the JSON content is base64 encoded</param>
    /// <param name="errorMessageAction">Error messages will be pushed to this action</param>
    public DatabaseServiceGC(
        string projectId,
        string serviceAccountJsonContent,
        bool isBase64Encoded,
        Action<string>? errorMessageAction = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(projectId);
        ArgumentException.ThrowIfNullOrWhiteSpace(serviceAccountJsonContent);

        try
        {
            string? jsonContent = serviceAccountJsonContent;

            if (isBase64Encoded)
            {
                if (!TryBase64Decode(serviceAccountJsonContent, out jsonContent, errorMessageAction))
                {
                    throw new Exception("Base64 decode operation for service account JSON has failed.");
                }
            }

            if (jsonContent != null)
            {
                _credential = GoogleCredential.FromJson(jsonContent)
                            .CreateScoped(DatastoreClient.DefaultScopes)
                            .UnderlyingCredential as ServiceAccountCredential;
            }

            DatastoreClient? dsClient;
            if (_credential != null)
            {
                dsClient = new DatastoreClientBuilder
                {
                    Credential = GoogleCredential.FromServiceAccountCredential(_credential)
                }.Build();
            }
            else
            {
                dsClient = DatastoreClient.Create();
            }

            _dsdb = DatastoreDb.Create(projectId, "", dsClient);
            _bInitializationSucceed = _dsdb != null;
        }
        catch (Exception e)
        {
            errorMessageAction?.Invoke($"DatabaseServiceGC->Constructor: {e.Message}, Trace: {e.StackTrace}");
            _bInitializationSucceed = false;
        }
    }

    /// <summary>
    /// DatabaseServiceGC: Constructor using hex-encoded service account JSON content
    /// </summary>
    /// <param name="projectId">GC Project ID</param>
    /// <param name="serviceAccountJsonHexContent">Hex-encoded JSON content of the service account key</param>
    /// <param name="errorMessageAction">Error messages will be pushed to this action</param>
    public DatabaseServiceGC(
        string projectId,
        ReadOnlySpan<char> serviceAccountJsonHexContent,
        Action<string>? errorMessageAction = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(projectId);

        try
        {
            var hexString = serviceAccountJsonHexContent.ToString();
            if (!TryHexDecode(hexString, out var jsonContent, errorMessageAction))
            {
                throw new Exception("Hex decode operation for service account JSON has failed.");
            }

            if (jsonContent != null)
            {
                _credential = GoogleCredential.FromJson(jsonContent)
                            .CreateScoped(DatastoreClient.DefaultScopes)
                            .UnderlyingCredential as ServiceAccountCredential;
            }

            DatastoreClient? dsClient;
            if (_credential != null)
            {
                dsClient = new DatastoreClientBuilder
                {
                    Credential = GoogleCredential.FromServiceAccountCredential(_credential)
                }.Build();
            }
            else
            {
                dsClient = DatastoreClient.Create();
            }

            _dsdb = DatastoreDb.Create(projectId, "", dsClient);
            _bInitializationSucceed = _dsdb != null;
        }
        catch (Exception e)
        {
            errorMessageAction?.Invoke($"DatabaseServiceGC->Constructor: {e.Message}, Trace: {e.StackTrace}");
            _bInitializationSucceed = false;
        }
    }

    /// <summary>
    /// DatabaseServiceGC: Constructor for default credentials (uses Application Default Credentials)
    /// </summary>
    /// <param name="projectId">GC Project ID</param>
    /// <param name="useDefaultCredentials">Must be true to use this constructor</param>
    /// <param name="errorMessageAction">Error messages will be pushed to this action</param>
    public DatabaseServiceGC(
        string projectId,
        bool useDefaultCredentials,
        Action<string>? errorMessageAction = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(projectId);

        if (!useDefaultCredentials)
        {
            throw new ArgumentException("This constructor is for default credentials only. Set useDefaultCredentials to true or use a different constructor.", nameof(useDefaultCredentials));
        }

        try
        {
            // Use Application Default Credentials (ADC)
            var dsClient = DatastoreClient.Create();
            _dsdb = DatastoreDb.Create(projectId, "", dsClient);
            _bInitializationSucceed = _dsdb != null;
        }
        catch (Exception e)
        {
            errorMessageAction?.Invoke($"DatabaseServiceGC->Constructor: {e.Message}, Trace: {e.StackTrace}");
            _bInitializationSucceed = false;
        }
    }

    /// <summary>
    /// Map that holds loaded kind definition instances
    /// </summary>
    private readonly Dictionary<string, KeyFactory> _loadedKindKeyFactories = [];
    private readonly Lock _loadedKindKeyFactoriesDictionaryLock = new();

    /// <summary>
    /// Searches kind key factories in LoadedKindKeyFactories, if not loaded, loads, stores and returns
    /// </summary>
    private bool LoadStoreAndGetKindKeyFactory(
        string kind,
        out KeyFactory? resultKeyFactory,
        Action<string>? errorMessageAction = null)
    {
        if (_dsdb == null)
        {
            resultKeyFactory = null;
            errorMessageAction?.Invoke("DatabaseServiceGC->LoadStoreAndGetKindKeyFactory: DSDB is null.");
            return false;
        }
        lock (_loadedKindKeyFactoriesDictionaryLock)
        {
            if (!_loadedKindKeyFactories.TryGetValue(kind, out resultKeyFactory))
            {
                try
                {
                    resultKeyFactory = _dsdb.CreateKeyFactory(kind);
                    if (resultKeyFactory != null)
                    {
                        _loadedKindKeyFactories[kind] = resultKeyFactory;
                        return true;
                    }
                    else
                    {
                        errorMessageAction?.Invoke("DatabaseServiceGC->LoadStoreAndGetKindKeyFactory: CreateKeyFactory returned null.");
                        return false;
                    }
                }
                catch (Exception e)
                {
                    resultKeyFactory = null;
                    errorMessageAction?.Invoke($"DatabaseServiceGC->LoadStoreAndGetKindKeyFactory: Exception: {e.Message}");
                    return false;
                }
            }
        }
        return true;
    }

    private static void ChangeExcludeFromIndexes(Value value)
    {
        if (value.ValueTypeCase != Value.ValueTypeOneofCase.ArrayValue)
        {
            value.ExcludeFromIndexes = true;
        }
    }

    private Entity? FromJsonToEntity(KeyFactory factory, string keyName, PrimitiveType keyValue, JObject? jsonObject)
    {
        if (jsonObject == null) return null;

        var result = FromJsonToEntity(jsonObject);
        result.Key = factory.CreateKey(GetFinalKeyFromNameValue(keyName, keyValue));
        return result;
    }

    private Entity FromJsonToEntity(JObject jsonObject)
    {
        var result = new Entity();
        foreach (var current in jsonObject)
        {
            var name = current.Key;
            var value = current.Value;
            if (value != null)
            {
                result.Properties[name] = GetDsValueFromJToken(value);
                ChangeExcludeFromIndexes(result.Properties[name]);
            }
        }
        return result;
    }

    private Value GetDsValueFromJToken(JToken value)
    {
        return value.Type switch
        {
            JTokenType.Object => new Value { EntityValue = FromJsonToEntity((JObject)value) },
            JTokenType.Array => GetArrayValue((JArray)value),
            JTokenType.Integer => new Value { IntegerValue = (long)value },
            JTokenType.Float => new Value { DoubleValue = (double)value },
            JTokenType.Boolean => new Value { BooleanValue = (bool)value },
            JTokenType.String => new Value { StringValue = value.Value<string>() },
            _ => new Value { StringValue = value.ToString() }
        };
    }

    private Value GetArrayValue(JArray asArray)
    {
        var asArrayValue = new ArrayValue();
        foreach (var current in asArray)
        {
            var curVal = GetDsValueFromJToken(current);
            ChangeExcludeFromIndexes(curVal);
            asArrayValue.Values.Add(curVal);
        }
        return new Value { ArrayValue = asArrayValue };
    }

    private static JObject? FromEntityToJson(Entity? entity)
    {
        if (entity?.Properties == null) return null;

        var result = new JObject();
        foreach (var current in entity.Properties)
        {
            result[current.Key] = FromValueToJsonToken(current.Value);
        }
        return result;
    }

    private static JToken? FromValueToJsonToken(Value value)
    {
        return value.ValueTypeCase switch
        {
            Value.ValueTypeOneofCase.EntityValue => FromEntityToJson(value.EntityValue),
            Value.ValueTypeOneofCase.ArrayValue => GetArrayToken(value.ArrayValue),
            Value.ValueTypeOneofCase.BooleanValue => value.BooleanValue,
            Value.ValueTypeOneofCase.IntegerValue => value.IntegerValue,
            Value.ValueTypeOneofCase.DoubleValue => value.DoubleValue,
            Value.ValueTypeOneofCase.StringValue => value.StringValue,
            _ => value.ToString()
        };
    }

    private static JArray GetArrayToken(ArrayValue arrayValue)
    {
        var asJArray = new JArray();
        foreach (var arrayVal in arrayValue.Values)
        {
            var token = FromValueToJsonToken(arrayVal);
            if (token != null)
            {
                asJArray.Add(token);
            }
        }
        return asJArray;
    }

    private static bool CompareJTokenWithPrimitive(JToken token, PrimitiveType primitive)
    {
        return primitive.Kind switch
        {
            PrimitiveTypeKind.Double => Math.Abs(primitive.AsDouble - token.Value<double>()) < 0.0000001,
            PrimitiveTypeKind.Integer => primitive.AsInteger == token.Value<long>(),
            PrimitiveTypeKind.ByteArray
                => Convert.ToBase64String(primitive.AsByteArray) == token.Value<string>(),
            _ => primitive.AsString == token.Value<string>()
        };
    }

    private static string GetFinalKeyFromNameValue(string keyName, PrimitiveType keyValue)
        => $"{keyName}:{keyValue}";

    /// <inheritdoc />
    public async Task<OperationResult<bool>> ItemExistsAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        DatabaseAttributeCondition? condition = null,
        CancellationToken cancellationToken = default)
    {
        if (_dsdb == null)
        {
            return OperationResult<bool>.Failure("DatabaseServiceGC->ItemExistsAsync: DSDB is null.");
        }
        try
        {
            if (!LoadStoreAndGetKindKeyFactory(tableName, out var factory))
            {
                return OperationResult<bool>.Failure("Failed to load table key factory");
            }

            var key = factory.NotNull().CreateKey(GetFinalKeyFromNameValue(keyName, keyValue));
            var entity = await _dsdb.LookupAsync(key);

            if (entity == null)
            {
                return OperationResult<bool>.Success(false);
            }

            if (condition != null)
            {
                var entityJson = FromEntityToJson(entity);
                if (entityJson != null)
                {
                    AddKeyToJson(entityJson, keyName, keyValue);
                    ApplyOptions(entityJson);
                    bool conditionSatisfied = ConditionCheck(entityJson, condition);
                    return OperationResult<bool>.Success(conditionSatisfied);
                }
            }

            return OperationResult<bool>.Success(true);
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure($"DatabaseServiceGC->ItemExistsAsync: {e.Message}");
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
        if (_dsdb == null)
        {
            return OperationResult<JObject?>.Failure("DatabaseServiceGC->GetItemAsync: DSDB is null.");
        }
        try
        {
            if (!LoadStoreAndGetKindKeyFactory(tableName, out var factory))
            {
                return OperationResult<JObject?>.Failure("Failed to load table key factory");
            }

            var key = factory.NotNull().CreateKey(GetFinalKeyFromNameValue(keyName, keyValue));
            var entity = await _dsdb.LookupAsync(key);

            if (entity == null)
            {
                return OperationResult<JObject?>.Success(null);
            }

            var result = FromEntityToJson(entity);
            if (result != null)
            {
                AddKeyToJson(result, keyName, keyValue);
                ApplyOptions(result);
            }

            return OperationResult<JObject?>.Success(result);
        }
        catch (Exception e)
        {
            return OperationResult<JObject?>.Failure($"DatabaseServiceGC->GetItemAsync: {e.Message}");
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
        if (_dsdb == null)
        {
            return OperationResult<IReadOnlyList<JObject>>.Failure("DatabaseServiceGC->GetItemsAsync: DSDB is null.");
        }
        try
        {
            if (keyValues.Length == 0)
            {
                return OperationResult<IReadOnlyList<JObject>>.Success([]);
            }

            if (!LoadStoreAndGetKindKeyFactory(tableName, out var factory))
            {
                return OperationResult<IReadOnlyList<JObject>>.Failure("Failed to load table key factory");
            }

            var datastoreKeys = keyValues.Select(value =>
                factory.NotNull().CreateKey(GetFinalKeyFromNameValue(keyName, value))).ToArray();

            var queryResult = await _dsdb.LookupAsync(datastoreKeys);
            var results = new List<JObject>();

            for (int i = 0; i < queryResult.Count; i++)
            {
                var current = queryResult[i];
                if (current != null)
                {
                    var asJson = FromEntityToJson(current);
                    if (asJson != null)
                    {
                        AddKeyToJson(asJson, keyName, keyValues[i]);
                        ApplyOptions(asJson);
                        results.Add(asJson);
                    }
                }
            }

            return OperationResult<IReadOnlyList<JObject>>.Success(results.AsReadOnly());
        }
        catch (Exception e)
        {
            return OperationResult<IReadOnlyList<JObject>>.Failure($"DatabaseServiceGC->GetItemsAsync: {e.Message}");
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
        const int maxRetryNumber = 5;
        int retryCount = 0;

        if (_dsdb == null)
        {
            return OperationResult<JObject?>.Failure("DatabaseServiceGC->DeleteItemAsync: DSDB is null.");
        }

        while (++retryCount <= maxRetryNumber)
        {
            try
            {
                if (!LoadStoreAndGetKindKeyFactory(tableName, out var factory))
                {
                    return OperationResult<JObject?>.Failure("Failed to load table key factory");
                }

                using var transaction = await _dsdb.BeginTransactionAsync();
                var key = factory.NotNull().CreateKey(GetFinalKeyFromNameValue(keyName, keyValue));

                JObject? returnItem = null;

                if (returnBehavior == ReturnItemBehavior.ReturnOldValues || condition != null)
                {
                    var entity = await transaction.LookupAsync(key);
                    if (entity != null)
                    {
                        var entityJson = FromEntityToJson(entity);
                        if (entityJson != null)
                        {
                            AddKeyToJson(entityJson, keyName, keyValue);
                            ApplyOptions(entityJson);

                            if (condition != null && !ConditionCheck(entityJson, condition))
                            {
                                return OperationResult<JObject?>.Failure("Condition not satisfied");
                            }

                            if (returnBehavior == ReturnItemBehavior.ReturnOldValues)
                            {
                                returnItem = entityJson;
                            }
                        }
                    }
                }

                transaction.Delete(key);
                await transaction.CommitAsync();

                return OperationResult<JObject?>.Success(returnItem);
            }
            catch (Exception e) when (CheckForRetriability(e))
            {
                await Task.Delay(5000, cancellationToken);
            }
            catch (Exception e)
            {
                return OperationResult<JObject?>.Failure($"DatabaseServiceGC->DeleteItemAsync: {e.Message}");
            }
        }

        return OperationResult<JObject?>.Failure($"DatabaseServiceGC->DeleteItemAsync: Too much contention on datastore entities; tried {maxRetryNumber} times");
    }

    /// <inheritdoc />
    public async Task<OperationResult<IReadOnlyList<JObject>>> ScanTableAsync(
        string tableName,
        string[] keyNames,
        CancellationToken cancellationToken = default)
    {
        if (_dsdb == null)
        {
            return OperationResult<IReadOnlyList<JObject>>.Failure("DatabaseServiceGC->ScanTableAsync: DSDB is null.");
        }
        try
        {
            var query = new Query(tableName);
            var queryResults = await _dsdb.RunQueryAsync(query);

            var results = new List<JObject>();
            foreach (var entity in queryResults.Entities)
            {
                var asJson = FromEntityToJson(entity);
                if (asJson != null)
                {
                    // Try to find the appropriate key from keyNames
                    foreach (var keyName in keyNames)
                    {
                        if (entity.Key.Path.Count > 0)
                        {
                            var keyParts = entity.Key.Path.Last().Name?.Split(':');
                            if (keyParts?.Length == 2 && keyParts[0] == keyName)
                            {
                                AddKeyToJson(asJson, keyName, new PrimitiveType(keyParts[1]));
                                break;
                            }
                        }
                    }

                    ApplyOptions(asJson);
                    results.Add(asJson);
                }
            }

            return OperationResult<IReadOnlyList<JObject>>.Success(results.AsReadOnly());
        }
        catch (Exception e)
        {
            return OperationResult<IReadOnlyList<JObject>>.Failure($"DatabaseServiceGC->ScanTableAsync: {e.Message}");
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
        if (_dsdb == null)
        {
            return OperationResult<(IReadOnlyList<JObject>, string?, long?)>.Failure("DatabaseServiceGC->ScanTablePaginatedAsync: DSDB is null.");
        }
        try
        {
            var query = new Query(tableName)
            {
                Limit = pageSize
            };

            if (!string.IsNullOrEmpty(pageToken))
            {
                try
                {
                    var cursorBytes = Convert.FromBase64String(pageToken);
                    query.StartCursor = Google.Protobuf.ByteString.CopyFrom(cursorBytes);
                }
                catch
                {
                    // Invalid page token, ignore and start from beginning
                }
            }

            var queryResults = await _dsdb.RunQueryAsync(query);
            var results = new List<JObject>();

            foreach (var entity in queryResults.Entities)
            {
                var asJson = FromEntityToJson(entity);
                if (asJson != null)
                {
                    // Try to find the appropriate key from keyNames
                    foreach (var keyName in keyNames)
                    {
                        if (entity.Key.Path.Count > 0)
                        {
                            var keyParts = entity.Key.Path.Last().Name?.Split(':');
                            if (keyParts?.Length == 2 && keyParts[0] == keyName)
                            {
                                AddKeyToJson(asJson, keyName, new PrimitiveType(keyParts[1]));
                                break;
                            }
                        }
                    }

                    ApplyOptions(asJson);
                    results.Add(asJson);
                }
            }

            string? nextPageToken = null;
            if (queryResults.EndCursor != null && !queryResults.EndCursor.IsEmpty)
            {
                nextPageToken = Convert.ToBase64String(queryResults.EndCursor.ToByteArray());
            }

            // Note: Google Cloud Datastore doesn't easily provide total count for queries
            return OperationResult<(IReadOnlyList<JObject>, string?, long?)>.Success(
                (results.AsReadOnly(), nextPageToken, null));
        }
        catch (Exception e)
        {
            return OperationResult<(IReadOnlyList<JObject>, string?, long?)>.Failure(
                $"DatabaseServiceGC->ScanTablePaginatedAsync: {e.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<IReadOnlyList<JObject>>> ScanTableWithFilterAsync(
        string tableName,
        string[] keyNames,
        DatabaseAttributeCondition filterCondition,
        CancellationToken cancellationToken = default)
    {
        if (_dsdb == null)
        {
            return OperationResult<IReadOnlyList<JObject>>.Failure("DatabaseServiceGC->ScanTableWithFilterAsync: DSDB is null.");
        }
        try
        {
            var query = new Query(tableName);

            // Build filter from condition - For Google Cloud Datastore, complex filtering may require indexes
            // This is a simplified implementation
            var queryResults = await _dsdb.RunQueryAsync(query);
            var results = new List<JObject>();

            foreach (var entity in queryResults.Entities)
            {
                var asJson = FromEntityToJson(entity);
                if (asJson != null)
                {
                    // Try to find the appropriate key from keyNames
                    foreach (var keyName in keyNames)
                    {
                        if (entity.Key.Path.Count > 0)
                        {
                            var keyParts = entity.Key.Path.Last().Name?.Split(':');
                            if (keyParts?.Length == 2 && keyParts[0] == keyName)
                            {
                                AddKeyToJson(asJson, keyName, new PrimitiveType(keyParts[1]));
                                break;
                            }
                        }
                    }

                    // Apply client-side filtering
                    if (ConditionCheck(asJson, filterCondition))
                    {
                        ApplyOptions(asJson);
                        results.Add(asJson);
                    }
                }
            }

            return OperationResult<IReadOnlyList<JObject>>.Success(results.AsReadOnly());
        }
        catch (Exception e)
        {
            return OperationResult<IReadOnlyList<JObject>>.Failure(
                $"DatabaseServiceGC->ScanTableWithFilterAsync: {e.Message}");
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
        if (_dsdb == null)
        {
            return OperationResult<(IReadOnlyList<JObject>, string?, long?)>.Failure("DatabaseServiceGC->ScanTableWithFilterPaginatedAsync: DSDB is null.");
        }
        try
        {
            var query = new Query(tableName)
            {
                Limit = pageSize * 2 // Get more to account for filtering
            };

            if (!string.IsNullOrEmpty(pageToken))
            {
                try
                {
                    var cursorBytes = Convert.FromBase64String(pageToken);
                    query.StartCursor = Google.Protobuf.ByteString.CopyFrom(cursorBytes);
                }
                catch
                {
                    // Invalid page token, ignore and start from beginning
                }
            }

            var queryResults = await _dsdb.RunQueryAsync(query);
            var results = new List<JObject>();

            foreach (var entity in queryResults.Entities)
            {
                if (results.Count >= pageSize) break;

                var asJson = FromEntityToJson(entity);
                if (asJson != null)
                {
                    // Try to find the appropriate key from keyNames
                    foreach (var keyName in keyNames)
                    {
                        if (entity.Key.Path.Count > 0)
                        {
                            var keyParts = entity.Key.Path.Last().Name?.Split(':');
                            if (keyParts?.Length == 2 && keyParts[0] == keyName)
                            {
                                AddKeyToJson(asJson, keyName, new PrimitiveType(keyParts[1]));
                                break;
                            }
                        }
                    }

                    // Apply client-side filtering
                    if (ConditionCheck(asJson, filterCondition))
                    {
                        ApplyOptions(asJson);
                        results.Add(asJson);
                    }
                }
            }

            string? nextPageToken = null;
            if (queryResults.EndCursor != null && !queryResults.EndCursor.IsEmpty && results.Count == pageSize)
            {
                nextPageToken = Convert.ToBase64String(queryResults.EndCursor.ToByteArray());
            }

            return OperationResult<(IReadOnlyList<JObject>, string?, long?)>.Success(
                (results.AsReadOnly(), nextPageToken, null));
        }
        catch (Exception e)
        {
            return OperationResult<(IReadOnlyList<JObject>, string?, long?)>.Failure(
                $"DatabaseServiceGC->ScanTableWithFilterPaginatedAsync: {e.Message}");
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
        if (_dsdb == null)
        {
            return OperationResult<JObject?>.Failure("DatabaseServiceGC->AddElementsToArrayAsync: DSDB is null.");
        }

        const int maxRetryNumber = 5;
        int retryCount = 0;

        while (++retryCount <= maxRetryNumber)
        {
            try
            {
                if (elementsToAdd.Length == 0)
                {
                    return OperationResult<JObject?>.Failure("ElementsToAdd must contain values.");
                }

                if (!LoadStoreAndGetKindKeyFactory(tableName, out var factory))
                {
                    return OperationResult<JObject?>.Failure("Failed to load table key factory");
                }

                using var transaction = await _dsdb.BeginTransactionAsync();
                var key = factory.NotNull().CreateKey(GetFinalKeyFromNameValue(keyName, keyValue));

                JObject? returnItem = null;
                var entity = await transaction.LookupAsync(key);

                if (entity != null)
                {
                    var entityJson = FromEntityToJson(entity);
                    if (entityJson != null)
                    {
                        AddKeyToJson(entityJson, keyName, keyValue);
                        ApplyOptions(entityJson);

                        if (condition != null && !ConditionCheck(entityJson, condition))
                        {
                            return OperationResult<JObject?>.Failure("Condition not satisfied");
                        }

                        if (returnBehavior == ReturnItemBehavior.ReturnOldValues)
                        {
                            returnItem = (JObject)entityJson.DeepClone();
                        }

                        // Get existing array or create new one
                        var existingArray = entityJson[arrayAttributeName] as JArray ?? [];

                        // Add new elements
                        foreach (var element in elementsToAdd)
                        {
                            var jToken = element.Kind switch
                            {
                                PrimitiveTypeKind.String => new JValue(element.AsString),
                                PrimitiveTypeKind.Integer => new JValue(element.AsInteger),
                                PrimitiveTypeKind.Double => new JValue(element.AsDouble),
                                PrimitiveTypeKind.ByteArray => new JValue(Convert.ToBase64String(element.AsByteArray)),
                                _ => new JValue(element.ToString())
                            };
                            existingArray.Add(jToken);
                        }

                        entityJson[arrayAttributeName] = existingArray;

                        // Convert back to entity and update
                        var updatedEntity = FromJsonToEntity(factory.NotNull(), keyName, keyValue, entityJson);
                        if (updatedEntity != null)
                        {
                            transaction.Upsert(updatedEntity);
                            await transaction.CommitAsync();

                            if (returnBehavior == ReturnItemBehavior.ReturnNewValues)
                            {
                                var getResult = await GetItemAsync(tableName, keyName, keyValue, null, cancellationToken);
                                return getResult;
                            }

                            return OperationResult<JObject?>.Success(returnItem);
                        }
                    }
                }
                else
                {
                    // Create new entity with the array
                    var newJson = new JObject();
                    var newArray = new JArray();

                    foreach (var element in elementsToAdd)
                    {
                        var jToken = element.Kind switch
                        {
                            PrimitiveTypeKind.String => new JValue(element.AsString),
                            PrimitiveTypeKind.Integer => new JValue(element.AsInteger),
                            PrimitiveTypeKind.Double => new JValue(element.AsDouble),
                            PrimitiveTypeKind.ByteArray => new JValue(Convert.ToBase64String(element.AsByteArray)),
                            _ => new JValue(element.ToString())
                        };
                        newArray.Add(jToken);
                    }

                    newJson[arrayAttributeName] = newArray;

                    var newEntity = FromJsonToEntity(factory.NotNull(), keyName, keyValue, newJson);
                    if (newEntity != null)
                    {
                        transaction.Upsert(newEntity);
                        await transaction.CommitAsync();

                        if (returnBehavior == ReturnItemBehavior.ReturnNewValues)
                        {
                            var getResult = await GetItemAsync(tableName, keyName, keyValue, null, cancellationToken);
                            return getResult;
                        }

                        return OperationResult<JObject?>.Success(returnItem);
                    }
                }

                return OperationResult<JObject?>.Failure("Failed to process array update");
            }
            catch (Exception e) when (CheckForRetriability(e))
            {
                await Task.Delay(5000, cancellationToken);
            }
            catch (Exception e)
            {
                return OperationResult<JObject?>.Failure($"DatabaseServiceGC->AddElementsToArrayAsync: {e.Message}");
            }
        }

        return OperationResult<JObject?>.Failure($"DatabaseServiceGC->AddElementsToArrayAsync: Too much contention on datastore entities; tried {maxRetryNumber} times");
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
        if (_dsdb == null)
        {
            return OperationResult<JObject?>.Failure("DatabaseServiceGC->RemoveElementsFromArrayAsync: DSDB is null.");
        }

        const int maxRetryNumber = 5;
        int retryCount = 0;

        while (++retryCount <= maxRetryNumber)
        {
            try
            {
                if (elementsToRemove.Length == 0)
                {
                    return OperationResult<JObject?>.Failure("ElementsToRemove must contain values.");
                }

                if (!LoadStoreAndGetKindKeyFactory(tableName, out var factory))
                {
                    return OperationResult<JObject?>.Failure("Failed to load table key factory");
                }

                using var transaction = await _dsdb.BeginTransactionAsync();
                var key = factory.NotNull().CreateKey(GetFinalKeyFromNameValue(keyName, keyValue));

                JObject? returnItem = null;
                var entity = await transaction.LookupAsync(key);

                if (entity != null)
                {
                    var entityJson = FromEntityToJson(entity);
                    if (entityJson != null)
                    {
                        AddKeyToJson(entityJson, keyName, keyValue);
                        ApplyOptions(entityJson);

                        if (condition != null && !ConditionCheck(entityJson, condition))
                        {
                            return OperationResult<JObject?>.Failure("Condition not satisfied");
                        }

                        if (returnBehavior == ReturnItemBehavior.ReturnOldValues)
                        {
                            returnItem = (JObject)entityJson.DeepClone();
                        }

                        // Get existing array
                        if (entityJson[arrayAttributeName] is JArray existingArray)
                        {
                            // Remove elements
                            var elementsToRemoveStrings = elementsToRemove.Select(element => element.Kind switch
                            {
                                PrimitiveTypeKind.String => element.AsString,
                                PrimitiveTypeKind.Integer => element.AsInteger.ToString(),
                                PrimitiveTypeKind.Double => element.AsDouble.ToString(CultureInfo.InvariantCulture),
                                PrimitiveTypeKind.ByteArray => Convert.ToBase64String(element.AsByteArray),
                                _ => element.ToString()
                            }).ToHashSet();

                            var itemsToRemove = new List<JToken>();
                            foreach (var item in existingArray)
                            {
                                var itemString = item.ToString();
                                if (elementsToRemoveStrings.Contains(itemString))
                                {
                                    itemsToRemove.Add(item);
                                }
                            }

                            foreach (var item in itemsToRemove)
                            {
                                existingArray.Remove(item);
                            }

                            // Convert back to entity and update
                            var updatedEntity = FromJsonToEntity(factory.NotNull(), keyName, keyValue, entityJson);
                            if (updatedEntity != null)
                            {
                                transaction.Upsert(updatedEntity);
                                await transaction.CommitAsync();

                                if (returnBehavior == ReturnItemBehavior.ReturnNewValues)
                                {
                                    var getResult = await GetItemAsync(tableName, keyName, keyValue, null, cancellationToken);
                                    return getResult;
                                }

                                return OperationResult<JObject?>.Success(returnItem);
                            }
                        }
                    }
                }

                return OperationResult<JObject?>.Success(returnItem);
            }
            catch (Exception e) when (CheckForRetriability(e))
            {
                await Task.Delay(5000, cancellationToken);
            }
            catch (Exception e)
            {
                return OperationResult<JObject?>.Failure($"DatabaseServiceGC->RemoveElementsFromArrayAsync: {e.Message}");
            }
        }

        return OperationResult<JObject?>.Failure($"DatabaseServiceGC->RemoveElementsFromArrayAsync: Too much contention on datastore entities; tried {maxRetryNumber} times");
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
        if (_dsdb == null)
        {
            return OperationResult<double>.Failure("DatabaseServiceGC->IncrementAttributeAsync: DSDB is null.");
        }

        const int maxRetryNumber = 5;
        int retryCount = 0;

        while (++retryCount <= maxRetryNumber)
        {
            try
            {
                if (!LoadStoreAndGetKindKeyFactory(tableName, out var factory))
                {
                    return OperationResult<double>.Failure("Failed to load table key factory");
                }

                using var transaction = await _dsdb.BeginTransactionAsync();
                var key = factory.NotNull().CreateKey(GetFinalKeyFromNameValue(keyName, keyValue));

                var entity = await transaction.LookupAsync(key);
                double newValue = incrementValue; // Default if entity doesn't exist

                if (entity != null)
                {
                    var entityJson = FromEntityToJson(entity);
                    if (entityJson != null)
                    {
                        AddKeyToJson(entityJson, keyName, keyValue);
                        ApplyOptions(entityJson);

                        if (condition != null && !ConditionCheck(entityJson, condition))
                        {
                            return OperationResult<double>.Failure("Condition not satisfied");
                        }

                        // Get current value and increment
                        var currentValue = 0.0;
                        if (entityJson.TryGetValue(numericAttributeName, out var currentToken))
                        {
                            currentValue = currentToken.Type switch
                            {
                                JTokenType.Integer => (long)currentToken,
                                JTokenType.Float => (double)currentToken,
                                _ => 0.0
                            };
                        }

                        newValue = currentValue + incrementValue;
                        entityJson[numericAttributeName] = newValue;

                        // Convert back to entity and update
                        var updatedEntity = FromJsonToEntity(factory.NotNull(), keyName, keyValue, entityJson);
                        if (updatedEntity != null)
                        {
                            transaction.Upsert(updatedEntity);
                            await transaction.CommitAsync();

                            return OperationResult<double>.Success(newValue);
                        }
                    }
                }
                else
                {
                    // Create new entity with the incremented value
                    var newJson = new JObject
                    {
                        [numericAttributeName] = newValue
                    };

                    var newEntity = FromJsonToEntity(factory.NotNull(), keyName, keyValue, newJson);
                    if (newEntity != null)
                    {
                        transaction.Upsert(newEntity);
                        await transaction.CommitAsync();

                        return OperationResult<double>.Success(newValue);
                    }
                }

                return OperationResult<double>.Failure("Failed to increment attribute");
            }
            catch (Exception e) when (CheckForRetriability(e))
            {
                await Task.Delay(5000, cancellationToken);
            }
            catch (Exception e)
            {
                return OperationResult<double>.Failure($"DatabaseServiceGC->IncrementAttributeAsync: {e.Message}");
            }
        }

        return OperationResult<double>.Failure($"DatabaseServiceGC->IncrementAttributeAsync: Too much contention on datastore entities; tried {maxRetryNumber} times");
    }

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
        bool shouldOverrideIfExist = false,
        CancellationToken cancellationToken = default)
    {
        if (_dsdb == null)
        {
            return OperationResult<JObject?>.Failure("DatabaseServiceGC->PutOrUpdateItemAsync: DSDB is null.");
        }

        const int maxRetryNumber = 5;
        var newItemCopy = new JObject(newItem);

        newItemCopy.Remove(keyName);

        int retryCount = 0;
        while (++retryCount <= maxRetryNumber)
        {
            try
            {
                if (!LoadStoreAndGetKindKeyFactory(tableName, out var factory))
                {
                    return OperationResult<JObject?>.Failure("Failed to load table key factory");
                }

                using var transaction = await _dsdb.BeginTransactionAsync();
                var key = factory.NotNull().CreateKey(GetFinalKeyFromNameValue(keyName, keyValue));

                JObject? returnedPreOperationObject = null;
                var entity = await transaction.LookupAsync(key);

                if (entity != null)
                {
                    returnedPreOperationObject = FromEntityToJson(entity);
                    if (returnedPreOperationObject != null)
                    {
                        AddKeyToJson(returnedPreOperationObject, keyName, keyValue);
                        ApplyOptions(returnedPreOperationObject);
                    }
                }

                if (putOrUpdateItemType == PutOrUpdateItemType.UpdateItem)
                {
                    if (conditionExpression != null && returnedPreOperationObject != null)
                    {
                        if (!ConditionCheck(returnedPreOperationObject, conditionExpression))
                        {
                            return OperationResult<JObject?>.Failure("Condition not satisfied");
                        }
                    }

                    if (returnedPreOperationObject != null)
                    {
                        var copyObject = new JObject(returnedPreOperationObject);
                        copyObject.Merge(newItemCopy, new JsonMergeSettings
                        {
                            MergeArrayHandling = MergeArrayHandling.Replace
                        });
                        newItemCopy = copyObject;
                    }
                }
                else // PutItem
                {
                    if (!shouldOverrideIfExist && returnedPreOperationObject != null)
                    {
                        return OperationResult<JObject?>.Failure("Item already exists");
                    }
                }

                JObject? returnItem = null;
                if (returnBehavior == ReturnItemBehavior.ReturnOldValues)
                {
                    returnItem = returnedPreOperationObject ?? [];
                }

                var itemAsEntity = FromJsonToEntity(factory.NotNull(), keyName, keyValue, newItemCopy);
                if (itemAsEntity != null)
                {
                    transaction.Upsert(itemAsEntity);
                    await transaction.CommitAsync();

                    if (returnBehavior == ReturnItemBehavior.ReturnNewValues)
                    {
                        var getResult = await GetItemAsync(tableName, keyName, keyValue, null, cancellationToken);
                        return getResult;
                    }

                    return OperationResult<JObject?>.Success(returnItem);
                }

                return OperationResult<JObject?>.Failure("Failed to convert JSON to entity");
            }
            catch (Exception e) when (CheckForRetriability(e))
            {
                await Task.Delay(5000, cancellationToken);
            }
            catch (Exception e)
            {
                return OperationResult<JObject?>.Failure($"DatabaseServiceGC->PutOrUpdateItemAsync: {e.Message}");
            }
        }

        return OperationResult<JObject?>.Failure($"DatabaseServiceGC->PutOrUpdateItemAsync: Too much contention on datastore entities; tried {maxRetryNumber} times");
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

    private static bool TryHexDecode(string input, out string? output, Action<string>? errorAction)
    {
        output = null;
        try
        {
            var bytes = Convert.FromHexString(input);
            output = System.Text.Encoding.UTF8.GetString(bytes);
            return true;
        }
        catch (Exception e)
        {
            errorAction?.Invoke($"Hex decode failed: {e.Message}");
            return false;
        }
    }

    private static bool TryBase64Decode(string input, out string? output, Action<string>? errorAction)
    {
        output = null;
        try
        {
            var bytes = Convert.FromBase64String(input);
            output = System.Text.Encoding.UTF8.GetString(bytes);
            return true;
        }
        catch (Exception e)
        {
            errorAction?.Invoke($"Base64 decode failed: {e.Message}");
            return false;
        }
    }

    private static bool CheckForRetriability(Exception e)
    {
        if (e is Grpc.Core.RpcException rpcException)
        {
            return rpcException.StatusCode == Grpc.Core.StatusCode.Aborted ||
                   rpcException.StatusCode == Grpc.Core.StatusCode.Unavailable;
        }
        return false;
    }

    private static bool ConditionCheck(JObject? jsonObjectToCheck, DatabaseAttributeCondition? conditionExpression)
    {
        if (jsonObjectToCheck == null || conditionExpression == null)
            return true;

        return conditionExpression.ConditionType switch
        {
            DatabaseAttributeConditionType.AttributeExists => jsonObjectToCheck.ContainsKey(conditionExpression.AttributeName),
            DatabaseAttributeConditionType.AttributeNotExists => !jsonObjectToCheck.ContainsKey(conditionExpression.AttributeName),
            _ when conditionExpression is ValueCondition valueCondition => CheckValueCondition(jsonObjectToCheck, valueCondition),
            _ when conditionExpression is ArrayElementCondition arrayCondition => CheckArrayElementCondition(jsonObjectToCheck, arrayCondition),
            _ => true
        };
    }

    private static bool CheckValueCondition(JObject jsonObject, ValueCondition condition)
    {
        if (!jsonObject.TryGetValue(condition.AttributeName, out var token))
            return false;

        return condition.ConditionType switch
        {
            DatabaseAttributeConditionType.AttributeEquals => CompareJTokenWithPrimitive(token, condition.Value),
            DatabaseAttributeConditionType.AttributeNotEquals => !CompareJTokenWithPrimitive(token, condition.Value),
            DatabaseAttributeConditionType.AttributeGreater => CompareJTokenValues(token, condition.Value) > 0,
            DatabaseAttributeConditionType.AttributeGreaterOrEqual => CompareJTokenValues(token, condition.Value) >= 0,
            DatabaseAttributeConditionType.AttributeLess => CompareJTokenValues(token, condition.Value) < 0,
            DatabaseAttributeConditionType.AttributeLessOrEqual => CompareJTokenValues(token, condition.Value) <= 0,
            _ => false
        };
    }

    private static bool CheckArrayElementCondition(JObject jsonObject, ArrayElementCondition condition)
    {
        if (!jsonObject.TryGetValue(condition.AttributeName, out var token) || token is not JArray array)
            return false;

        var elementExists = array.Any(item => CompareJTokenWithPrimitive(item, condition.ElementValue));

        return condition.ConditionType switch
        {
            DatabaseAttributeConditionType.ArrayElementExists => elementExists,
            DatabaseAttributeConditionType.ArrayElementNotExists => !elementExists,
            _ => false
        };
    }

    private static int CompareJTokenValues(JToken token, PrimitiveType primitive)
    {
        try
        {
            return primitive.Kind switch
            {
                PrimitiveTypeKind.Double when token.Type is JTokenType.Float or JTokenType.Integer =>
                    ((double)token).CompareTo(primitive.AsDouble),
                PrimitiveTypeKind.Integer when token.Type is JTokenType.Integer or JTokenType.Float =>
                    ((long)token).CompareTo(primitive.AsInteger),
                PrimitiveTypeKind.String when token.Type != JTokenType.Null =>
                    string.Compare(token.Value<string>(), primitive.AsString, StringComparison.InvariantCulture),
                _ => 0
            };
        }
        catch
        {
            return 0;
        }
    }

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

    public async ValueTask DisposeAsync()
    {
        // Google Cloud Datastore clients are designed to be long-lived
        // No explicit disposal is typically needed, but we suppress finalization
        // ReSharper disable once GCSuppressFinalizeForTypeWithoutDestructor
        System.GC.SuppressFinalize(this);
        await Task.CompletedTask;
    }
}
