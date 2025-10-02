// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

//
// Note: It's essential that methods defined in IDatabaseService are not called directly from ...CoreAsync methods.
// Instead, call CoreAsync methods when needed.
//

using System.Globalization;
using System.Net;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Interfaces.Enums;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Datastore.V1;
using Newtonsoft.Json.Linq;
using CrossCloudKit.Utilities.Common;

namespace CrossCloudKit.Database.GC;

public sealed class DatabaseServiceGC : DatabaseServiceBase, IAsyncDisposable
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
    public override bool IsInitialized => _bInitializationSucceed;

    /// <summary>
    /// DatabaseServiceGC: Constructor using service account JSON file path
    /// </summary>
    /// <param name="projectId">GC Project ID</param>
    /// <param name="serviceAccountKeyFilePath">Path to the service account JSON key file</param>
    /// <param name="memoryService">This memory service will be used to ensure global atomicity on functions</param>
    /// <param name="errorMessageAction">Error messages will be pushed to this action</param>
    public DatabaseServiceGC(
        string projectId,
        string serviceAccountKeyFilePath,
        IMemoryService memoryService,
        Action<string>? errorMessageAction = null) : base(memoryService)
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
    /// <param name="memoryService">This memory service will be used to ensure global atomicity on functions</param>
    /// <param name="errorMessageAction">Error messages will be pushed to this action</param>
    public DatabaseServiceGC(
        string projectId,
        string serviceAccountJsonContent,
        bool isBase64Encoded,
        IMemoryService memoryService,
        Action<string>? errorMessageAction = null) : base(memoryService)
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
    /// <param name="memoryService">This memory service will be used to ensure global atomicity on functions</param>
    /// <param name="errorMessageAction">Error messages will be pushed to this action</param>
    public DatabaseServiceGC(
        string projectId,
        ReadOnlySpan<char> serviceAccountJsonHexContent,
        IMemoryService memoryService,
        Action<string>? errorMessageAction = null) : base(memoryService)
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
    /// <param name="memoryService">This memory service will be used to ensure global atomicity on functions</param>
    /// <param name="errorMessageAction">Error messages will be pushed to this action</param>
    public DatabaseServiceGC(
        string projectId,
        bool useDefaultCredentials,
        IMemoryService memoryService,
        Action<string>? errorMessageAction = null) : base(memoryService)
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
            PrimitiveTypeKind.Boolean => primitive.AsBoolean == token.Value<bool>(),
            PrimitiveTypeKind.Integer => primitive.AsInteger == token.Value<long>(),
            PrimitiveTypeKind.ByteArray
                => Convert.ToBase64String(primitive.AsByteArray) == token.Value<string>(),
            _ => primitive.AsString == token.Value<string>()
        };
    }

    private static string GetFinalKeyFromNameValue(string keyName, PrimitiveType keyValue)
        => $"{keyName}:{keyValue}";

    /// <inheritdoc />
    protected override async Task<OperationResult<bool>> ItemExistsCoreAsync(
        string tableName,
        DbKey key,
        IEnumerable<DbAttributeCondition>? conditions = null,
        CancellationToken cancellationToken = default)
    {
        if (_dsdb == null)
        {
            return OperationResult<bool>.Failure("DatabaseServiceGC->ItemExistsAsync: DSDB is null.", HttpStatusCode.ServiceUnavailable);
        }
        try
        {
            if (!LoadStoreAndGetKindKeyFactory(tableName, out var factory))
            {
                return OperationResult<bool>.Failure("Failed to load table key factory", HttpStatusCode.InternalServerError);
            }

            var gKey = factory.NotNull().CreateKey(GetFinalKeyFromNameValue(key.Name, key.Value));
            var entity = await _dsdb.LookupAsync(gKey);

            if (entity == null)
            {
                return OperationResult<bool>.Failure("Item not found", HttpStatusCode.NotFound);
            }

            if (conditions != null)
            {
                var entityJson = FromEntityToJson(entity);
                if (entityJson != null)
                {
                    AddKeyToJson(entityJson, key.Name, key.Value);
                    ApplyOptions(entityJson);

                    if (conditions.Any(condition => !ConditionCheck(entityJson, condition)))
                        return OperationResult<bool>.Failure("Conditions are not satisfied.", HttpStatusCode.PreconditionFailed);
                }
            }

            return OperationResult<bool>.Success(true);
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure($"DatabaseServiceGC->ItemExistsAsync: {e.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<JObject?>> GetItemCoreAsync(
        string tableName,
        DbKey key,
        string[]? attributesToRetrieve = null,
        CancellationToken cancellationToken = default)
    {
        if (_dsdb == null)
        {
            return OperationResult<JObject?>.Failure("DatabaseServiceGC->GetItemAsync: DSDB is null.", HttpStatusCode.ServiceUnavailable);
        }
        try
        {
            if (!LoadStoreAndGetKindKeyFactory(tableName, out var factory))
            {
                return OperationResult<JObject?>.Failure("Failed to load table key factory", HttpStatusCode.InternalServerError);
            }

            var gKey = factory.NotNull().CreateKey(GetFinalKeyFromNameValue(key.Name, key.Value));
            var entity = await _dsdb.LookupAsync(gKey);

            if (entity == null)
            {
                return OperationResult<JObject?>.Success(null);
            }

            var result = FromEntityToJson(entity);
            if (result != null)
            {
                AddKeyToJson(result, key.Name, key.Value);
                ApplyOptions(result);
            }

            return OperationResult<JObject?>.Success(result);
        }
        catch (Exception e)
        {
            return OperationResult<JObject?>.Failure($"DatabaseServiceGC->GetItemAsync: {e.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<IReadOnlyList<JObject>>> GetItemsCoreAsync(
        string tableName,
        DbKey[] keys,
        string[]? attributesToRetrieve = null,
        CancellationToken cancellationToken = default)
    {
        if (_dsdb == null)
        {
            return OperationResult<IReadOnlyList<JObject>>.Failure("DatabaseServiceGC->GetItemsAsync: DSDB is null.", HttpStatusCode.ServiceUnavailable);
        }
        try
        {
            if (keys.Length == 0)
            {
                return OperationResult<IReadOnlyList<JObject>>.Success([]);
            }

            if (!LoadStoreAndGetKindKeyFactory(tableName, out var factory))
            {
                return OperationResult<IReadOnlyList<JObject>>.Failure("Failed to load table key factory", HttpStatusCode.InternalServerError);
            }

            var datastoreKeys = keys.Select(k =>
                factory.NotNull().CreateKey(GetFinalKeyFromNameValue(k.Name, k.Value))).ToArray();

            var queryResult = await _dsdb.LookupAsync(datastoreKeys);
            var results = new List<JObject>();

            for (var i = 0; i < queryResult.Count; i++)
            {
                var current = queryResult[i];
                if (current == null) continue;

                var asJson = FromEntityToJson(current);
                if (asJson == null) continue;

                AddKeyToJson(asJson, keys[i].Name, keys[i].Value);
                ApplyOptions(asJson);
                results.Add(asJson);
            }

            return OperationResult<IReadOnlyList<JObject>>.Success(results.AsReadOnly());
        }
        catch (Exception e)
        {
            return OperationResult<IReadOnlyList<JObject>>.Failure($"DatabaseServiceGC->GetItemsAsync: {e.Message}", HttpStatusCode.InternalServerError);
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
        return await PutOrUpdateItemAsync(PutOrUpdateItemType.PutItem, tableName, key, item,
            returnBehavior, null, overwriteIfExists, cancellationToken);
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<JObject?>> UpdateItemCoreAsync(
        string tableName,
        DbKey key,
        JObject updateData,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        IEnumerable<DbAttributeCondition>? conditions = null,
        CancellationToken cancellationToken = default)
    {
        return await PutOrUpdateItemAsync(PutOrUpdateItemType.UpdateItem, tableName, key, updateData,
            returnBehavior, conditions, false, cancellationToken);
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<JObject?>> DeleteItemCoreAsync(
        string tableName,
        DbKey key,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        IEnumerable<DbAttributeCondition>? conditions = null,
        CancellationToken cancellationToken = default)
    {
        const int maxRetryNumber = 5;
        int retryCount = 0;

        if (_dsdb == null)
        {
            return OperationResult<JObject?>.Failure("DatabaseServiceGC->DeleteItemAsync: DSDB is null.", HttpStatusCode.ServiceUnavailable);
        }

        while (++retryCount <= maxRetryNumber)
        {
            try
            {
                if (!LoadStoreAndGetKindKeyFactory(tableName, out var factory))
                {
                    return OperationResult<JObject?>.Failure("Failed to load table key factory", HttpStatusCode.InternalServerError);
                }

                using var transaction = await _dsdb.BeginTransactionAsync();
                var gKey = factory.NotNull().CreateKey(GetFinalKeyFromNameValue(key.Name, key.Value));

                JObject? returnItem = null;

                if (returnBehavior == DbReturnItemBehavior.ReturnOldValues || conditions != null)
                {
                    var entity = await transaction.LookupAsync(gKey);
                    if (entity != null)
                    {
                        var entityJson = FromEntityToJson(entity);
                        if (entityJson != null)
                        {
                            AddKeyToJson(entityJson, key.Name, key.Value);
                            ApplyOptions(entityJson);

                            // ReSharper disable once PossibleMultipleEnumeration
                            if (conditions != null && conditions.Any(condition => !ConditionCheck(entityJson, condition)))
                            {
                                return OperationResult<JObject?>.Failure("Condition not satisfied", HttpStatusCode.PreconditionFailed);
                            }

                            if (returnBehavior == DbReturnItemBehavior.ReturnOldValues)
                            {
                                returnItem = entityJson;
                            }
                        }
                    }
                }

                transaction.Delete(gKey);
                await transaction.CommitAsync();

                return OperationResult<JObject?>.Success(returnItem);
            }
            catch (Exception e) when (CheckForRetriability(e))
            {
                await Task.Delay(5000, cancellationToken);
            }
            catch (Exception e)
            {
                return OperationResult<JObject?>.Failure($"DatabaseServiceGC->DeleteItemAsync: {e.Message}", HttpStatusCode.InternalServerError);
            }
        }

        return OperationResult<JObject?>.Failure($"DatabaseServiceGC->DeleteItemAsync: Too much contention on datastore entities; tried {maxRetryNumber} times", HttpStatusCode.TooManyRequests);
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<(IReadOnlyList<string> Keys, IReadOnlyList<JObject> Items)>> ScanTableCoreAsync(
        string tableName,
        CancellationToken cancellationToken = default)
    {
        return await InternalScanTableAsync(tableName, null, cancellationToken);
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

        return await InternalScanTablePaginatedAsync(tableName, pageSize, pageToken, null, cancellationToken);
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<(IReadOnlyList<string> Keys, IReadOnlyList<JObject> Items)>> ScanTableWithFilterCoreAsync(
        string tableName,
        IEnumerable<DbAttributeCondition> filterConditions,
        CancellationToken cancellationToken = default)
    {
        return await InternalScanTableAsync(tableName, filterConditions, cancellationToken);
    }

    /// <inheritdoc />
    protected override async
        Task<OperationResult<(
            IReadOnlyList<string>? Keys,
            IReadOnlyList<JObject> Items,
            string? NextPageToken,
            long? TotalCount)>> ScanTableWithFilterPaginatedCoreAsync(
        string tableName,
        IEnumerable<DbAttributeCondition> filterConditions,
        int pageSize,
        string? pageToken = null,
        CancellationToken cancellationToken = default)
    {
        return await InternalScanTablePaginatedAsync(tableName, pageSize, pageToken, filterConditions, cancellationToken);
    }

    private async
        Task<OperationResult<(
            IReadOnlyList<string>? Keys,
            IReadOnlyList<JObject> Items,
            string? NextPageToken,
            long? TotalCount)>> InternalScanTablePaginatedAsync(
        string tableName,
        int pageSize,
        string? pageToken = null,
        IEnumerable<DbAttributeCondition>? filterConditions = null,
        CancellationToken cancellationToken = default)
    {
        if (_dsdb == null)
        {
            return OperationResult<(IReadOnlyList<string>?, IReadOnlyList<JObject>, string?, long?)>.Failure("DatabaseServiceGC->ScanTableWithFilterPaginatedAsync: DSDB is null.", HttpStatusCode.ServiceUnavailable);
        }
        try
        {
            IReadOnlyList<string>? keys = null;
            if (string.IsNullOrEmpty(pageToken))
            {
                var getKeysResult = await GetTableKeysCoreAsync(tableName, cancellationToken);
                if (!getKeysResult.IsSuccessful)
                    return OperationResult<(IReadOnlyList<string>? Keys, IReadOnlyList<JObject> Items, string? NextPageToken, long? TotalCount)>.Failure(getKeysResult.ErrorMessage, getKeysResult.StatusCode);
                keys = getKeysResult.Data;
            }
            var results = new List<JObject>();

            var nextPageToken = pageToken;
            do
            {
                var query = new Query(tableName)
                {
                    Limit = pageSize - results.Count
                };

                if (!string.IsNullOrEmpty(nextPageToken))
                {
                    try
                    {
                        query.StartCursor = Google.Protobuf.ByteString.CopyFrom(Convert.FromBase64String(nextPageToken));
                    }
                    catch
                    {
                        // Invalid page token, ignore and start from beginning
                    }
                }

                var queryResults = await _dsdb.RunQueryAsync(query);

                // ReSharper disable once PossibleMultipleEnumeration
                results.AddRange(ConvertEntitiesToJson(queryResults, filterConditions, pageSize));

                if (queryResults.EndCursor != null && !queryResults.EndCursor.IsEmpty)
                {
                    var newToken = Convert.ToBase64String(queryResults.EndCursor.ToByteArray());
                    if (newToken == nextPageToken && queryResults.Entities.Count == 0)
                    {
                        nextPageToken = null;
                    }
                    else
                    {
                        nextPageToken = newToken;
                    }
                }
                else
                {
                    nextPageToken = null;
                }
            } while (results.Count < pageSize && nextPageToken != null);

            return OperationResult<(IReadOnlyList<string>?, IReadOnlyList<JObject>, string?, long?)>.Success(
                (keys, results.AsReadOnly(), nextPageToken, null));
        }
        catch (Exception e)
        {
            return OperationResult<(IReadOnlyList<string>?, IReadOnlyList<JObject>, string?, long?)>.Failure(
                $"DatabaseServiceGC->ScanTableWithFilterPaginatedAsync: {e.Message}", HttpStatusCode.InternalServerError);
        }
    }

    private async Task<OperationResult<(IReadOnlyList<string> Keys, IReadOnlyList<JObject> Items)>> InternalScanTableAsync(
        string tableName,
        IEnumerable<DbAttributeCondition>? filterConditions = null,
        CancellationToken cancellationToken = default)
    {
        if (_dsdb == null)
        {
            return OperationResult<(IReadOnlyList<string>, IReadOnlyList<JObject>)>.Failure("DatabaseServiceGC->ScanTableWithFilterAsync: DSDB is null.", HttpStatusCode.ServiceUnavailable);
        }
        try
        {
            var query = new Query(tableName);

            var getKeysTask = GetTableKeysCoreAsync(tableName, cancellationToken);
            var queryResultsTask = _dsdb.RunQueryAsync(query);

            await Task.WhenAll(getKeysTask, queryResultsTask);

            var getKeysResult = await getKeysTask;
            if (!getKeysResult.IsSuccessful)
                return OperationResult<(IReadOnlyList<string>, IReadOnlyList<JObject>)>.Failure(getKeysResult.ErrorMessage, getKeysResult.StatusCode);

            var results = ConvertEntitiesToJson(await queryResultsTask, filterConditions);

            return OperationResult<(IReadOnlyList<string>, IReadOnlyList<JObject>)>.Success((getKeysResult.Data, results.AsReadOnly()));
        }
        catch (Exception e)
        {
            return OperationResult<(IReadOnlyList<string>, IReadOnlyList<JObject>)>.Failure(
                $"DatabaseServiceGC->InternalScanTableAsync: {e.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<JObject?>> AddElementsToArrayCoreAsync(
        string tableName,
        DbKey key,
        string arrayAttributeName,
        PrimitiveType[] elementsToAdd,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        IEnumerable<DbAttributeCondition>? conditions = null,
        bool isCalledFromPostInsert = false,
        CancellationToken cancellationToken = default)
    {
        if (_dsdb == null)
        {
            return OperationResult<JObject?>.Failure("DatabaseServiceGC->AddElementsToArrayAsync: DSDB is null.", HttpStatusCode.ServiceUnavailable);
        }

        const int maxRetryNumber = 5;
        int retryCount = 0;

        while (++retryCount <= maxRetryNumber)
        {
            try
            {
                if (elementsToAdd.Length == 0)
                {
                    return OperationResult<JObject?>.Failure("ElementsToAdd must contain values.", HttpStatusCode.BadRequest);
                }

                if (!isCalledFromPostInsert)
                {
                    var sanityCheckResult = await AttributeNamesSanityCheck(key, tableName, new JObject { [arrayAttributeName] = new JArray() }, cancellationToken);
                    if (!sanityCheckResult.IsSuccessful)
                    {
                        return OperationResult<JObject?>.Failure(sanityCheckResult.ErrorMessage, sanityCheckResult.StatusCode);
                    }
                }

                if (!LoadStoreAndGetKindKeyFactory(tableName, out var factory))
                {
                    return OperationResult<JObject?>.Failure("Failed to load table key factory", HttpStatusCode.InternalServerError);
                }

                using var transaction = await _dsdb.BeginTransactionAsync();
                var gKey = factory.NotNull().CreateKey(GetFinalKeyFromNameValue(key.Name, key.Value));

                JObject? returnItem = null;
                var entity = await transaction.LookupAsync(gKey);

                if (entity != null)
                {
                    var entityJson = FromEntityToJson(entity);
                    if (entityJson != null)
                    {
                        AddKeyToJson(entityJson, key.Name, key.Value);
                        ApplyOptions(entityJson);

                        // ReSharper disable once PossibleMultipleEnumeration
                        if (conditions != null && conditions.Any(c => !ConditionCheck(entityJson, c)))
                        {
                            return OperationResult<JObject?>.Failure("Condition not satisfied", HttpStatusCode.PreconditionFailed);
                        }

                        if (returnBehavior == DbReturnItemBehavior.ReturnOldValues)
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
                                PrimitiveTypeKind.Boolean => new JValue(element.AsBoolean),
                                PrimitiveTypeKind.Double => new JValue(element.AsDouble),
                                PrimitiveTypeKind.ByteArray => new JValue(Convert.ToBase64String(element.AsByteArray)),
                                _ => new JValue(element.ToString())
                            };
                            existingArray.Add(jToken);
                        }

                        entityJson[arrayAttributeName] = existingArray;

                        // Convert back to entity and update
                        var updatedEntity = FromJsonToEntity(factory.NotNull(), key.Name, key.Value, entityJson);
                        if (updatedEntity != null)
                        {
                            transaction.Upsert(updatedEntity);
                            await transaction.CommitAsync();

                            if (returnBehavior == DbReturnItemBehavior.ReturnNewValues)
                            {
                                var getResult = await GetItemCoreAsync(tableName, key, null, cancellationToken);
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
                            PrimitiveTypeKind.Boolean => new JValue(element.AsBoolean),
                            PrimitiveTypeKind.Double => new JValue(element.AsDouble),
                            PrimitiveTypeKind.ByteArray => new JValue(Convert.ToBase64String(element.AsByteArray)),
                            _ => new JValue(element.ToString())
                        };
                        newArray.Add(jToken);
                    }

                    newJson[arrayAttributeName] = newArray;

                    var newEntity = FromJsonToEntity(factory.NotNull(), key.Name, key.Value, newJson);
                    if (newEntity != null)
                    {
                        transaction.Upsert(newEntity);

                        var commitTask = transaction.CommitAsync();
                        if (!isCalledFromPostInsert)
                        {
                            var postInsertTask = PostInsertItemAsync(tableName, key, cancellationToken);

                            await Task.WhenAll(commitTask, postInsertTask);

                            var postInsertResult = await postInsertTask;
                            if (!postInsertResult.IsSuccessful)
                            {
                                return OperationResult<JObject?>.Failure(
                                    $"PostInsertItemAsync failed with: {postInsertResult.ErrorMessage}",
                                    postInsertResult.StatusCode);
                            }
                        }
                        else await commitTask;

                        if (returnBehavior == DbReturnItemBehavior.ReturnNewValues)
                        {
                            var getResult = await GetItemCoreAsync(tableName, key, null, cancellationToken);
                            return getResult;
                        }

                        return OperationResult<JObject?>.Success(returnItem);
                    }
                }

                return OperationResult<JObject?>.Failure("Failed to process array update", HttpStatusCode.InternalServerError);
            }
            catch (Exception e) when (CheckForRetriability(e))
            {
                await Task.Delay(5000, cancellationToken);
            }
            catch (Exception e)
            {
                return OperationResult<JObject?>.Failure($"DatabaseServiceGC->AddElementsToArrayAsync: {e.Message}", HttpStatusCode.InternalServerError);
            }
        }

        return OperationResult<JObject?>.Failure($"DatabaseServiceGC->AddElementsToArrayAsync: Too much contention on datastore entities; tried {maxRetryNumber} times", HttpStatusCode.TooManyRequests);
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<JObject?>> RemoveElementsFromArrayCoreAsync(
        string tableName,
        DbKey key,
        string arrayAttributeName,
        PrimitiveType[] elementsToRemove,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        IEnumerable<DbAttributeCondition>? conditions = null,
        CancellationToken cancellationToken = default)
    {
        if (_dsdb == null)
        {
            return OperationResult<JObject?>.Failure("DatabaseServiceGC->RemoveElementsFromArrayAsync: DSDB is null.", HttpStatusCode.ServiceUnavailable);
        }

        const int maxRetryNumber = 5;
        int retryCount = 0;

        while (++retryCount <= maxRetryNumber)
        {
            try
            {
                if (elementsToRemove.Length == 0)
                {
                    return OperationResult<JObject?>.Failure("ElementsToRemove must contain values.", HttpStatusCode.BadRequest);
                }

                if (!LoadStoreAndGetKindKeyFactory(tableName, out var factory))
                {
                    return OperationResult<JObject?>.Failure("Failed to load table key factory", HttpStatusCode.InternalServerError);
                }

                using var transaction = await _dsdb.BeginTransactionAsync();
                var gKey = factory.NotNull().CreateKey(GetFinalKeyFromNameValue(key.Name, key.Value));

                JObject? returnItem = null;
                var entity = await transaction.LookupAsync(gKey);

                if (entity != null)
                {
                    var entityJson = FromEntityToJson(entity);
                    if (entityJson != null)
                    {
                        AddKeyToJson(entityJson, key.Name, key.Value);
                        ApplyOptions(entityJson);

                        // ReSharper disable once PossibleMultipleEnumeration
                        if (conditions != null && conditions.Any(c => !ConditionCheck(entityJson, c)))
                        {
                            return OperationResult<JObject?>.Failure("Condition not satisfied", HttpStatusCode.PreconditionFailed);
                        }

                        if (returnBehavior == DbReturnItemBehavior.ReturnOldValues)
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
                                PrimitiveTypeKind.Integer => element.AsInteger.ToString(CultureInfo.InvariantCulture),
                                PrimitiveTypeKind.Boolean => element.AsBoolean.ToString(CultureInfo.InvariantCulture),
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
                            var updatedEntity = FromJsonToEntity(factory.NotNull(), key.Name, key.Value, entityJson);
                            if (updatedEntity != null)
                            {
                                transaction.Upsert(updatedEntity);
                                await transaction.CommitAsync();

                                if (returnBehavior == DbReturnItemBehavior.ReturnNewValues)
                                {
                                    var getResult = await GetItemCoreAsync(tableName, key, null, cancellationToken);
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
                return OperationResult<JObject?>.Failure($"DatabaseServiceGC->RemoveElementsFromArrayAsync: {e.Message}", HttpStatusCode.InternalServerError);
            }
        }

        return OperationResult<JObject?>.Failure($"DatabaseServiceGC->RemoveElementsFromArrayAsync: Too much contention on datastore entities; tried {maxRetryNumber} times", HttpStatusCode.TooManyRequests);
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<double>> IncrementAttributeCoreAsync(
        string tableName,
        DbKey key,
        string numericAttributeName,
        double incrementValue,
        IEnumerable<DbAttributeCondition>? conditions = null,
        CancellationToken cancellationToken = default)
    {
        if (_dsdb == null)
        {
            return OperationResult<double>.Failure("DatabaseServiceGC->IncrementAttributeAsync: DSDB is null.", HttpStatusCode.ServiceUnavailable);
        }

        const int maxRetryNumber = 5;
        var retryCount = 0;

        while (++retryCount <= maxRetryNumber)
        {
            try
            {
                var sanityCheckResult = await AttributeNamesSanityCheck(key, tableName, new JObject { [numericAttributeName] = new JArray() }, cancellationToken);
                if (!sanityCheckResult.IsSuccessful)
                {
                    return OperationResult<double>.Failure(sanityCheckResult.ErrorMessage, sanityCheckResult.StatusCode);
                }

                if (!LoadStoreAndGetKindKeyFactory(tableName, out var factory))
                {
                    return OperationResult<double>.Failure("Failed to load table key factory", HttpStatusCode.InternalServerError);
                }

                using var transaction = await _dsdb.BeginTransactionAsync();
                var gKey = factory.NotNull().CreateKey(GetFinalKeyFromNameValue(key.Name, key.Value));

                var entity = await transaction.LookupAsync(gKey);
                double newValue = incrementValue; // Default if entity doesn't exist

                if (entity != null)
                {
                    var entityJson = FromEntityToJson(entity);
                    if (entityJson != null)
                    {
                        AddKeyToJson(entityJson, key.Name, key.Value);
                        ApplyOptions(entityJson);

                        // ReSharper disable once PossibleMultipleEnumeration
                        if (conditions != null && conditions.Any(c => !ConditionCheck(entityJson, c)))
                        {
                            return OperationResult<double>.Failure("Condition not satisfied", HttpStatusCode.PreconditionFailed);
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
                        var updatedEntity = FromJsonToEntity(factory.NotNull(), key.Name, key.Value, entityJson);
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

                    var newEntity = FromJsonToEntity(factory.NotNull(), key.Name, key.Value, newJson);
                    if (newEntity != null)
                    {
                        transaction.Upsert(newEntity);

                        var commitTask = transaction.CommitAsync();
                        var postInsertTask = PostInsertItemAsync(tableName, key, cancellationToken);

                        await Task.WhenAll(commitTask, postInsertTask);

                        var postInsertResult = await postInsertTask;
                        return !postInsertResult.IsSuccessful
                            ? OperationResult<double>.Failure($"PostInsertItemAsync failed with: {postInsertResult.ErrorMessage}", postInsertResult.StatusCode)
                            : OperationResult<double>.Success(newValue);
                    }
                }

                return OperationResult<double>.Failure("Failed to increment attribute", HttpStatusCode.InternalServerError);
            }
            catch (Exception e) when (CheckForRetriability(e))
            {
                await Task.Delay(5000, cancellationToken);
            }
            catch (Exception e)
            {
                return OperationResult<double>.Failure($"DatabaseServiceGC->IncrementAttributeAsync: {e.Message}", HttpStatusCode.InternalServerError);
            }
        }

        return OperationResult<double>.Failure($"DatabaseServiceGC->IncrementAttributeAsync: Too much contention on datastore entities; tried {maxRetryNumber} times", HttpStatusCode.TooManyRequests);
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<IReadOnlyList<string>>> GetTableNamesCoreAsync(CancellationToken cancellationToken = default)
    {
        if (_dsdb == null)
        {
            return OperationResult<IReadOnlyList<string>>.Failure("DatabaseServiceGC->GetTableNamesAsync: DSDB is null.", HttpStatusCode.ServiceUnavailable);
        }

        try
        {
            // In Google Cloud Datastore, there's no direct API to list all kinds
            // We need to use the __kind__ special entity to get all kinds
            var query = new Query("__kind__")
            {
                Projection = { "__key__" }
            };

            var queryResults = await _dsdb.RunQueryAsync(query);
            var kindNames = new List<string>();

            foreach (var entity in queryResults.Entities)
            {
                if (entity.Key.Path.Count <= 0) continue;

                var kindName = entity.Key.Path.Last().Name;
                if (!string.IsNullOrEmpty(kindName) && !kindName.StartsWith("__") && !kindName.StartsWith(SystemTableNamePrefix))
                {
                    kindNames.Add(kindName);
                }
            }

            return OperationResult<IReadOnlyList<string>>.Success(kindNames.Distinct().ToList().AsReadOnly());
        }
        catch (Exception ex)
        {
            return OperationResult<IReadOnlyList<string>>.Failure($"DatabaseServiceGC->GetTableNamesAsync: {ex.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<bool>> DropTableCoreAsync(string tableName, CancellationToken cancellationToken = default)
    {
        try
        {
            const int batchSize = 500; // Google Cloud Datastore batch size limit
            string? pageToken = null;

            var deleteTableTask = Task.Run(async () =>
            {
                do
                {
                    // Get the next batch of entity keys directly from Datastore
                    var keysResult = await GetEntityKeysForDeletion(tableName, batchSize, pageToken);
                    if (!keysResult.IsSuccessful)
                    {
                        return OperationResult<bool>.Failure($"Failed to get entity keys for deletion: {keysResult.ErrorMessage}", keysResult.StatusCode);
                    }

                    var (keys, nextPageToken) = keysResult.Data;

                    // If no keys found, we're done
                    if (keys.Count == 0)
                    {
                        break;
                    }

                    // Delete this batch of keys
                    var deleteResult = await DeleteKeysWithRetry(keys, cancellationToken);
                    if (!deleteResult.IsSuccessful)
                    {
                        return deleteResult;
                    }

                    // Move to next page
                    pageToken = nextPageToken;
                }
                while (pageToken != null);

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

            // Remove from loaded kind key factories cache
            lock (_loadedKindKeyFactoriesDictionaryLock)
            {
                _loadedKindKeyFactories.Remove(tableName);
            }

            return OperationResult<bool>.Success(true);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure($"DatabaseServiceGC->DropTableAsync: {ex.Message}", HttpStatusCode.InternalServerError);
        }
    }

    private async Task<OperationResult<(IReadOnlyList<Key> Keys, string? NextPageToken)>> GetEntityKeysForDeletion(
        string tableName,
        int batchSize,
        string? pageToken)
    {
        if (_dsdb == null)
        {
            return OperationResult<(IReadOnlyList<Key> Keys, string? NextPageToken)>.Failure("DatabaseServiceGC->GetEntityKeysForDeletion: DSDB is null.", HttpStatusCode.ServiceUnavailable);
        }

        try
        {
            var query = new Query(tableName)
            {
                Projection = { "__key__" }, // Only get keys for efficiency
                Limit = batchSize
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
            var keys = queryResults.Entities.Select(e => e.Key).ToList();

            string? nextPageToken = null;
            if (queryResults.EndCursor != null && !queryResults.EndCursor.IsEmpty)
            {
                nextPageToken = Convert.ToBase64String(queryResults.EndCursor.ToByteArray());
            }

            return OperationResult<(IReadOnlyList<Key>, string?)>.Success((keys.AsReadOnly(), nextPageToken));
        }
        catch (Exception ex)
        {
            return OperationResult<(IReadOnlyList<Key>, string?)>.Failure($"Failed to get entity keys: {ex.Message}", HttpStatusCode.InternalServerError);
        }
    }

    private async Task<OperationResult<bool>> DeleteKeysWithRetry(IReadOnlyList<Key> keys, CancellationToken cancellationToken)
    {
        if (_dsdb == null)
        {
            return OperationResult<bool>.Failure("DatabaseServiceGC->DeleteKeysWithRetry: DSDB is null.", HttpStatusCode.ServiceUnavailable);
        }

        const int maxRetryNumber = 5;
        var retryCount = 0;

        while (++retryCount <= maxRetryNumber)
        {
            try
            {
                using var transaction = await _dsdb.BeginTransactionAsync();

                foreach (var key in keys)
                {
                    transaction.Delete(key);
                }

                await transaction.CommitAsync();
                return OperationResult<bool>.Success(true);
            }
            catch (Exception ex) when (CheckForRetriability(ex))
            {
                await Task.Delay(1000 * retryCount, cancellationToken); // Exponential backoff
            }
            catch (Exception ex)
            {
                return OperationResult<bool>.Failure($"Failed to delete batch: {ex.Message}", HttpStatusCode.InternalServerError);
            }
        }

        return OperationResult<bool>.Failure($"Failed to delete batch after {maxRetryNumber} retries", HttpStatusCode.TooManyRequests);
    }

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
        IEnumerable<DbAttributeCondition>? conditions = null,
        bool shouldOverrideIfExist = false,
        CancellationToken cancellationToken = default)
    {
        if (_dsdb == null)
        {
            return OperationResult<JObject?>.Failure("DatabaseServiceGC->PutOrUpdateItemAsync: DSDB is null.", HttpStatusCode.ServiceUnavailable);
        }

        var sanityCheckResult = await AttributeNamesSanityCheck(key, tableName, newItem, cancellationToken);
        if (!sanityCheckResult.IsSuccessful)
        {
            return OperationResult<JObject?>.Failure(sanityCheckResult.ErrorMessage, sanityCheckResult.StatusCode);
        }

        const int maxRetryNumber = 5;
        var newItemCopy = new JObject(newItem);

        newItemCopy.Remove(key.Name);

        var retryCount = 0;
        while (++retryCount <= maxRetryNumber)
        {
            try
            {
                if (!LoadStoreAndGetKindKeyFactory(tableName, out var factory))
                {
                    return OperationResult<JObject?>.Failure("Failed to load table key factory", HttpStatusCode.InternalServerError);
                }

                using var transaction = await _dsdb.BeginTransactionAsync();
                var gKey = factory.NotNull().CreateKey(GetFinalKeyFromNameValue(key.Name, key.Value));

                JObject? returnedPreOperationObject = null;
                var entity = await transaction.LookupAsync(gKey);

                if (entity != null)
                {
                    returnedPreOperationObject = FromEntityToJson(entity);
                    if (returnedPreOperationObject != null)
                    {
                        AddKeyToJson(returnedPreOperationObject, key.Name, key.Value);
                        ApplyOptions(returnedPreOperationObject);
                    }
                }

                if (putOrUpdateItemType == PutOrUpdateItemType.UpdateItem)
                {
                    if (conditions != null && returnedPreOperationObject != null)
                    {
                        // ReSharper disable once PossibleMultipleEnumeration
                        if (conditions.Any(c => !ConditionCheck(returnedPreOperationObject, c)))
                        {
                            return OperationResult<JObject?>.Failure("Condition not satisfied", HttpStatusCode.PreconditionFailed);
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
                        return OperationResult<JObject?>.Failure("Item already exists", HttpStatusCode.Conflict);
                    }
                }

                JObject? returnItem = null;
                if (returnBehavior == DbReturnItemBehavior.ReturnOldValues)
                {
                    returnItem = returnedPreOperationObject ?? [];
                }

                var itemAsEntity = FromJsonToEntity(factory.NotNull(), key.Name, key.Value, newItemCopy);
                if (itemAsEntity != null)
                {
                    transaction.Upsert(itemAsEntity);
                    var commitTask = transaction.CommitAsync();

                    if (putOrUpdateItemType == PutOrUpdateItemType.PutItem)
                    {
                        var postInsertTask = PostInsertItemAsync(tableName, key, cancellationToken);

                        await Task.WhenAll(commitTask, postInsertTask);

                        var postInsertResult = await postInsertTask;
                        if (!postInsertResult.IsSuccessful)
                        {
                            return OperationResult<JObject?>.Failure($"PostInsertItemAsync failed with: {postInsertResult.ErrorMessage}", postInsertResult.StatusCode);
                        }
                    }
                    else
                    {
                        await commitTask;
                    }

                    if (returnBehavior == DbReturnItemBehavior.ReturnNewValues)
                    {
                        var getResult = await GetItemCoreAsync(tableName, key, null, cancellationToken);
                        return getResult;
                    }

                    return OperationResult<JObject?>.Success(returnItem);
                }

                return OperationResult<JObject?>.Failure("Failed to convert JSON to entity", HttpStatusCode.InternalServerError);
            }
            catch (Exception e) when (CheckForRetriability(e))
            {
                await Task.Delay(5000, cancellationToken);
            }
            catch (Exception e)
            {
                return OperationResult<JObject?>.Failure($"DatabaseServiceGC->PutOrUpdateItemAsync: {e.Message}", HttpStatusCode.InternalServerError);
            }
        }

        return OperationResult<JObject?>.Failure($"DatabaseServiceGC->PutOrUpdateItemAsync: Too much contention on datastore entities; tried {maxRetryNumber} times", HttpStatusCode.TooManyRequests);
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

    private static bool ConditionCheck(JObject? jsonObjectToCheck, DbAttributeCondition? conditionExpression)
    {
        if (jsonObjectToCheck == null || conditionExpression == null)
            return true;

        return conditionExpression.ConditionType switch
        {
            DbAttributeConditionType.AttributeExists => jsonObjectToCheck.ContainsKey(conditionExpression.AttributeName),
            DbAttributeConditionType.AttributeNotExists => !jsonObjectToCheck.ContainsKey(conditionExpression.AttributeName),
            _ when conditionExpression is DbValueCondition valueCondition => CheckValueCondition(jsonObjectToCheck, valueCondition),
            _ when conditionExpression is DbArrayElementCondition arrayCondition => CheckArrayElementCondition(jsonObjectToCheck, arrayCondition),
            _ => true
        };
    }

    private static bool CheckValueCondition(JObject jsonObject, DbValueCondition condition)
    {
        if (!jsonObject.TryGetValue(condition.AttributeName, out var token))
            return false;

        return condition.ConditionType switch
        {
            DbAttributeConditionType.AttributeEquals => CompareJTokenWithPrimitive(token, condition.Value),
            DbAttributeConditionType.AttributeNotEquals => !CompareJTokenWithPrimitive(token, condition.Value),
            DbAttributeConditionType.AttributeGreater => CompareJTokenValues(token, condition.Value) > 0,
            DbAttributeConditionType.AttributeGreaterOrEqual => CompareJTokenValues(token, condition.Value) >= 0,
            DbAttributeConditionType.AttributeLess => CompareJTokenValues(token, condition.Value) < 0,
            DbAttributeConditionType.AttributeLessOrEqual => CompareJTokenValues(token, condition.Value) <= 0,
            _ => false
        };
    }

    private static bool CheckArrayElementCondition(JObject jsonObject, DbArrayElementCondition condition)
    {
        if (!jsonObject.TryGetValue(condition.AttributeName, out var token) || token is not JArray array)
            return false;

        var elementExists = array.Any(item => CompareJTokenWithPrimitive(item, condition.ElementValue));

        return condition.ConditionType switch
        {
            DbAttributeConditionType.ArrayElementExists => elementExists,
            DbAttributeConditionType.ArrayElementNotExists => !elementExists,
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

    private List<JObject> ConvertEntitiesToJson(
        DatastoreQueryResults queryResults,
       IEnumerable<DbAttributeCondition>? filterConditions = null,
        int pageSize = -1)
    {
        var results = new List<JObject>();
        foreach (var entity in queryResults.Entities)
        {
            if (pageSize > 0 && results.Count >= pageSize) break;

            var asJson = FromEntityToJson(entity);
            if (asJson == null) continue;

            if (entity.Key.Path.Count > 0)
            {
                var keyParts = entity.Key.Path.Last().Name?.Split(':');
                if (keyParts?.Length == 2)
                {
                    AddKeyToJson(asJson, keyParts[0], new PrimitiveType(keyParts[1]));
                }
            }

            // Apply client-side filtering
            // ReSharper disable once PossibleMultipleEnumeration
            if (filterConditions != null && filterConditions.Any(c => !ConditionCheck(asJson, c)))
            {
                continue;
            }

            ApplyOptions(asJson);
            results.Add(asJson);
        }

        return results;
    }

    /// <inheritdoc />
    public override DbAttributeCondition BuildAttributeExistsCondition(string attributeName) =>
        new DbExistenceCondition(DbAttributeConditionType.AttributeExists, attributeName);

    /// <inheritdoc />
    public override DbAttributeCondition BuildAttributeNotExistsCondition(string attributeName) =>
        new DbExistenceCondition(DbAttributeConditionType.AttributeNotExists, attributeName);

    /// <inheritdoc />
    public override DbAttributeCondition BuildAttributeEqualsCondition(string attributeName, PrimitiveType value) =>
        new DbValueCondition(DbAttributeConditionType.AttributeEquals, attributeName, value);

    /// <inheritdoc />
    public override DbAttributeCondition BuildAttributeNotEqualsCondition(string attributeName, PrimitiveType value) =>
        new DbValueCondition(DbAttributeConditionType.AttributeNotEquals, attributeName, value);

    /// <inheritdoc />
    public override DbAttributeCondition BuildAttributeGreaterCondition(string attributeName, PrimitiveType value) =>
        new DbValueCondition(DbAttributeConditionType.AttributeGreater, attributeName, value);

    /// <inheritdoc />
    public override DbAttributeCondition BuildAttributeGreaterOrEqualCondition(string attributeName, PrimitiveType value) =>
        new DbValueCondition(DbAttributeConditionType.AttributeGreaterOrEqual, attributeName, value);

    /// <inheritdoc />
    public override DbAttributeCondition BuildAttributeLessCondition(string attributeName, PrimitiveType value) =>
        new DbValueCondition(DbAttributeConditionType.AttributeLess, attributeName, value);

    /// <inheritdoc />
    public override DbAttributeCondition BuildAttributeLessOrEqualCondition(string attributeName, PrimitiveType value) =>
        new DbValueCondition(DbAttributeConditionType.AttributeLessOrEqual, attributeName, value);

    /// <inheritdoc />
    public override DbAttributeCondition BuildArrayElementExistsCondition(string attributeName, PrimitiveType elementValue) =>
        new DbArrayElementCondition(DbAttributeConditionType.ArrayElementExists, attributeName, elementValue);

    /// <inheritdoc />
    public override DbAttributeCondition BuildArrayElementNotExistsCondition(string attributeName, PrimitiveType elementValue) =>
        new DbArrayElementCondition(DbAttributeConditionType.ArrayElementNotExists, attributeName, elementValue);

    public async ValueTask DisposeAsync()
    {
        // Google Cloud Datastore clients are designed to be long-lived
        // No explicit disposal is typically needed, but we suppress finalization
        // ReSharper disable once GCSuppressFinalizeForTypeWithoutDestructor
        System.GC.SuppressFinalize(this);
        await Task.CompletedTask;
    }
}
