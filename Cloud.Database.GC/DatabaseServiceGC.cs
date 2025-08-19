// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Cloud.Interfaces;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Datastore.V1;
using Utilities.Common;
using Newtonsoft.Json.Linq;

namespace Cloud.Database.GC;

public class DatabaseServiceGC : DatabaseServiceBase, IDatabaseService, IAsyncDisposable
{
    /// <summary>
    /// Holds initialization success
    /// </summary>
    private readonly bool bInitializationSucceed;

    private readonly DatastoreClient? DSClient;
    private readonly DatastoreDb? DSDB;
    private readonly ServiceAccountCredential? Credential;

    /// <summary>
    /// Gets a value indicating whether the database service has been successfully initialized.
    /// </summary>
    public bool IsInitialized => bInitializationSucceed;

    /// <summary>
    /// DatabaseServiceGC: Constructor for Managed Service by Google
    /// </summary>
    /// <param name="projectId">GC Project ID</param>
    /// <param name="errorMessageAction">Error messages will be pushed to this action</param>
    public DatabaseServiceGC(
        string projectId,
        Action<string>? errorMessageAction = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(projectId);
        
        try
        {
            string? applicationCredentials = Environment.GetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS");
            string? applicationCredentialsPlain = Environment.GetEnvironmentVariable("GOOGLE_PLAIN_CREDENTIALS");
            string? applicationCredentialsBase64 = Environment.GetEnvironmentVariable("GOOGLE_BASE64_CREDENTIALS");

            if (applicationCredentials == null && applicationCredentialsPlain == null && applicationCredentialsBase64 == null)
            {
                errorMessageAction?.Invoke("DatabaseServiceGC->Constructor: GOOGLE_APPLICATION_CREDENTIALS (or GOOGLE_PLAIN_CREDENTIALS or GOOGLE_BASE64_CREDENTIALS) environment variable is not defined.");
                bInitializationSucceed = false;
                return;
            }

            if (applicationCredentials == null)
            {
                if (applicationCredentialsPlain != null && !TryHexDecode(applicationCredentialsPlain, out applicationCredentialsPlain, errorMessageAction))
                {
                    throw new Exception("Hex decode operation for application credentials plain has failed.");
                }
                else if (applicationCredentialsBase64 != null && !TryBase64Decode(applicationCredentialsBase64, out applicationCredentialsPlain, errorMessageAction))
                {
                    throw new Exception("Base64 decode operation for application credentials plain has failed.");
                }

                if (applicationCredentialsPlain != null)
                {
                    Credential = GoogleCredential.FromJson(applicationCredentialsPlain)
                                 .CreateScoped(DatastoreClient.DefaultScopes)
                                 .UnderlyingCredential as ServiceAccountCredential;
                }
            }
            else
            {
                using var stream = new FileStream(applicationCredentials, FileMode.Open, FileAccess.Read);
                Credential = GoogleCredential.FromStream(stream)
                            .CreateScoped(DatastoreClient.DefaultScopes)
                            .UnderlyingCredential as ServiceAccountCredential;
            }

            if (Credential != null)
            {
                DSClient = new DatastoreClientBuilder
                {
                    Credential = GoogleCredential.FromServiceAccountCredential(Credential)
                }.Build();
            }
            else
            {
                DSClient = DatastoreClient.Create();
            }

            DSDB = DatastoreDb.Create(projectId, "", DSClient);
            bInitializationSucceed = DSDB != null;
        }
        catch (Exception e)
        {
            errorMessageAction?.Invoke($"DatabaseServiceGC->Constructor: {e.Message}, Trace: {e.StackTrace}");
            bInitializationSucceed = false;
        }
    }

    /// <summary>
    /// Map that holds loaded kind definition instances
    /// </summary>
    private readonly Dictionary<string, KeyFactory> LoadedKindKeyFactories = [];
    private readonly Lock LoadedKindKeyFactories_DictionaryLock = new();

    /// <summary>
    /// Searches kind key factories in LoadedKindKeyFactories, if not loaded, loads, stores and returns
    /// </summary>
    private bool LoadStoreAndGetKindKeyFactory(
        string kind,
        out KeyFactory? resultKeyFactory,
        Action<string>? errorMessageAction = null)
    {
        if (DSDB == null)
        {
            resultKeyFactory = null;
            errorMessageAction?.Invoke("DatabaseServiceGC->LoadStoreAndGetKindKeyFactory: DSDB is null.");
            return false;
        }
        lock (LoadedKindKeyFactories_DictionaryLock)
        {
            if (!LoadedKindKeyFactories.TryGetValue(kind, out resultKeyFactory))
            {
                try
                {
                    resultKeyFactory = DSDB.CreateKeyFactory(kind);
                    if (resultKeyFactory != null)
                    {
                        LoadedKindKeyFactories[kind] = resultKeyFactory;
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
                result.Properties[name] = GetDSValueFromJToken(value);
                ChangeExcludeFromIndexes(result.Properties[name]);
            }
        }
        return result;
    }

    private Value GetDSValueFromJToken(JToken value)
    {
        return value.Type switch
        {
            JTokenType.Object => new Value { EntityValue = FromJsonToEntity((JObject)value) },
            JTokenType.Array => GetArrayValue((JArray)value),
            JTokenType.Integer => new Value { IntegerValue = (long)value },
            JTokenType.Float => new Value { DoubleValue = (double)value },
            JTokenType.Boolean => new Value { BooleanValue = (bool)value },
            JTokenType.String => new Value { StringValue = (string)value! },
            _ => new Value { StringValue = value.ToString() }
        };
    }

    private Value GetArrayValue(JArray asArray)
    {
        var asArrayValue = new ArrayValue();
        foreach (var current in asArray)
        {
            var curVal = GetDSValueFromJToken(current);
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
            PrimitiveTypeKind.Double => primitive.AsDouble == token.Value<double>(),
            PrimitiveTypeKind.Integer => primitive.AsInteger == token.Value<long>(),
            PrimitiveTypeKind.ByteArray
                => Convert.ToBase64String(primitive.AsByteArray) == token.Value<string>(),
            _ => primitive.AsString == token.Value<string>()
        };
    }

    private static string GetFinalKeyFromNameValue(string keyName, PrimitiveType keyValue)
        => $"{keyName}:{keyValue}";

    #region Modern Async API

    /// <summary>
    /// Checks if an item exists and optionally satisfies a condition.
    /// </summary>
    public async Task<DatabaseResult<bool>> ItemExistsAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        DatabaseAttributeCondition? condition = null,
        CancellationToken cancellationToken = default)
    {
        if (DSDB == null)
        {
            return DatabaseResult<bool>.Failure("DatabaseServiceGC->ItemExistsAsync: DSDB is null.");
        }
        try
        {
            if (!LoadStoreAndGetKindKeyFactory(tableName, out var factory, null))
            {
                return DatabaseResult<bool>.Failure("Failed to load table key factory");
            }

            var key = factory!.CreateKey(GetFinalKeyFromNameValue(keyName, keyValue));
            var entity = await DSDB.LookupAsync(key);

            if (entity == null)
            {
                return DatabaseResult<bool>.Success(false);
            }

            if (condition != null)
            {
                var entityJson = FromEntityToJson(entity);
                if (entityJson != null)
                {
                    AddKeyToJson(entityJson, keyName, keyValue);
                    ApplyOptions(entityJson);
                    bool conditionSatisfied = ConditionCheck(entityJson, condition);
                    return DatabaseResult<bool>.Success(conditionSatisfied);
                }
            }

            return DatabaseResult<bool>.Success(true);
        }
        catch (Exception e)
        {
            return DatabaseResult<bool>.Failure($"DatabaseServiceGC->ItemExistsAsync: {e.Message}");
        }
    }

    /// <summary>
    /// Gets an item from the database.
    /// </summary>
    public async Task<DatabaseResult<JObject?>> GetItemAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        string[]? attributesToRetrieve = null,
        CancellationToken cancellationToken = default)
    {
        if (DSDB == null)
        {
            return DatabaseResult<JObject?>.Failure("DatabaseServiceGC->GetItemAsync: DSDB is null.");
        }
        try
        {
            if (!LoadStoreAndGetKindKeyFactory(tableName, out var factory, null))
            {
                return DatabaseResult<JObject?>.Failure("Failed to load table key factory");
            }

            var key = factory!.CreateKey(GetFinalKeyFromNameValue(keyName, keyValue));
            var entity = await DSDB.LookupAsync(key);

            if (entity == null)
            {
                return DatabaseResult<JObject?>.Success(null);
            }

            var result = FromEntityToJson(entity);
            if (result != null)
            {
                AddKeyToJson(result, keyName, keyValue);
                ApplyOptions(result);
            }

            return DatabaseResult<JObject?>.Success(result);
        }
        catch (Exception e)
        {
            return DatabaseResult<JObject?>.Failure($"DatabaseServiceGC->GetItemAsync: {e.Message}");
        }
    }

    /// <summary>
    /// Gets multiple items from the database.
    /// </summary>
    public async Task<DatabaseResult<IReadOnlyList<JObject>>> GetItemsAsync(
        string tableName,
        string keyName,
        PrimitiveType[] keyValues,
        string[]? attributesToRetrieve = null,
        CancellationToken cancellationToken = default)
    {
        if (DSDB == null)
        {
            return DatabaseResult<IReadOnlyList<JObject>>.Failure("DatabaseServiceGC->GetItemsAsync: DSDB is null.");
        }
        try
        {
            if (keyValues.Length == 0)
            {
                return DatabaseResult<IReadOnlyList<JObject>>.Success([]);
            }

            if (!LoadStoreAndGetKindKeyFactory(tableName, out var factory, null))
            {
                return DatabaseResult<IReadOnlyList<JObject>>.Failure("Failed to load table key factory");
            }

            var datastoreKeys = keyValues.Select(value => 
                factory!.CreateKey(GetFinalKeyFromNameValue(keyName, value))).ToArray();

            var queryResult = await DSDB.LookupAsync(datastoreKeys);
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

            return DatabaseResult<IReadOnlyList<JObject>>.Success(results.AsReadOnly());
        }
        catch (Exception e)
        {
            return DatabaseResult<IReadOnlyList<JObject>>.Failure($"DatabaseServiceGC->GetItemsAsync: {e.Message}");
        }
    }

    /// <summary>
    /// Puts an item into the database.
    /// </summary>
    public async Task<DatabaseResult<JObject?>> PutItemAsync(
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

    /// <summary>
    /// Updates an existing item in the database.
    /// </summary>
    public async Task<DatabaseResult<JObject?>> UpdateItemAsync(
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

    /// <summary>
    /// Deletes an item from the database.
    /// </summary>
    public async Task<DatabaseResult<JObject?>> DeleteItemAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        ReturnItemBehavior returnBehavior = ReturnItemBehavior.DoNotReturn,
        DatabaseAttributeCondition? condition = null,
        CancellationToken cancellationToken = default)
    {
        const int maxRetryNumber = 5;
        int retryCount = 0;

        if (DSDB == null)
        {
            return DatabaseResult<JObject?>.Failure("DatabaseServiceGC->DeleteItemAsync: DSDB is null.");
        }

        while (++retryCount <= maxRetryNumber)
        {
            try
            {
                if (!LoadStoreAndGetKindKeyFactory(tableName, out var factory, null))
                {
                    return DatabaseResult<JObject?>.Failure("Failed to load table key factory");
                }

                using var transaction = await DSDB.BeginTransactionAsync();
                var key = factory!.CreateKey(GetFinalKeyFromNameValue(keyName, keyValue));

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
                                return DatabaseResult<JObject?>.Failure("Condition not satisfied");
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

                return DatabaseResult<JObject?>.Success(returnItem);
            }
            catch (Exception e) when (CheckForRetriability(e))
            {
                await Task.Delay(5000, cancellationToken);
                continue;
            }
            catch (Exception e)
            {
                return DatabaseResult<JObject?>.Failure($"DatabaseServiceGC->DeleteItemAsync: {e.Message}");
            }
        }

        return DatabaseResult<JObject?>.Failure($"DatabaseServiceGC->DeleteItemAsync: Too much contention on datastore entities; tried {maxRetryNumber} times");
    }

    /// <summary>
    /// Scans a table and returns all items.
    /// </summary>
    public async Task<DatabaseResult<IReadOnlyList<JObject>>> ScanTableAsync(
        string tableName,
        string[] keyNames,
        CancellationToken cancellationToken = default)
    {
        if (DSDB == null)
        {
            return DatabaseResult<IReadOnlyList<JObject>>.Failure("DatabaseServiceGC->ScanTableAsync: DSDB is null.");
        }
        try
        {
            var query = new Query(tableName);
            var queryResults = await DSDB.RunQueryAsync(query);

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

            return DatabaseResult<IReadOnlyList<JObject>>.Success(results.AsReadOnly());
        }
        catch (Exception e)
        {
            return DatabaseResult<IReadOnlyList<JObject>>.Failure($"DatabaseServiceGC->ScanTableAsync: {e.Message}");
        }
    }

    /// <summary>
    /// Scans a table with pagination support.
    /// </summary>
    public async Task<DatabaseResult<(IReadOnlyList<JObject> Items, string? NextPageToken, long? TotalCount)>> ScanTablePaginatedAsync(
        string tableName,
        string[] keyNames,
        int pageSize,
        string? pageToken = null,
        CancellationToken cancellationToken = default)
    {
        if (DSDB == null)
        {
            return DatabaseResult<(IReadOnlyList<JObject>, string?, long?)>.Failure("DatabaseServiceGC->ScanTablePaginatedAsync: DSDB is null.");
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

            var queryResults = await DSDB.RunQueryAsync(query);
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
            return DatabaseResult<(IReadOnlyList<JObject>, string?, long?)>.Success(
                (results.AsReadOnly(), nextPageToken, null));
        }
        catch (Exception e)
        {
            return DatabaseResult<(IReadOnlyList<JObject>, string?, long?)>.Failure(
                $"DatabaseServiceGC->ScanTablePaginatedAsync: {e.Message}");
        }
    }

    /// <summary>
    /// Scans a table and returns items that match the specified filter condition.
    /// </summary>
    public async Task<DatabaseResult<IReadOnlyList<JObject>>> ScanTableWithFilterAsync(
        string tableName,
        string[] keyNames,
        DatabaseAttributeCondition filterCondition,
        CancellationToken cancellationToken = default)
    {
        if (DSDB == null)
        {
            return DatabaseResult<IReadOnlyList<JObject>>.Failure("DatabaseServiceGC->ScanTableWithFilterAsync: DSDB is null.");
        }
        try
        {
            var query = new Query(tableName);
            
            // Build filter from condition - For Google Cloud Datastore, complex filtering may require indexes
            // This is a simplified implementation
            var queryResults = await DSDB.RunQueryAsync(query);
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

            return DatabaseResult<IReadOnlyList<JObject>>.Success(results.AsReadOnly());
        }
        catch (Exception e)
        {
            return DatabaseResult<IReadOnlyList<JObject>>.Failure(
                $"DatabaseServiceGC->ScanTableWithFilterAsync: {e.Message}");
        }
    }

    /// <summary>
    /// Scans a table with filtering and pagination support.
    /// </summary>
    public async Task<DatabaseResult<(IReadOnlyList<JObject> Items, string? NextPageToken, long? TotalCount)>> ScanTableWithFilterPaginatedAsync(
        string tableName,
        string[] keyNames,
        DatabaseAttributeCondition filterCondition,
        int pageSize,
        string? pageToken = null,
        CancellationToken cancellationToken = default)
    {
        if (DSDB == null)
        {
            return DatabaseResult<(IReadOnlyList<JObject>, string?, long?)>.Failure("DatabaseServiceGC->ScanTableWithFilterPaginatedAsync: DSDB is null.");
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

            var queryResults = await DSDB.RunQueryAsync(query);
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

            return DatabaseResult<(IReadOnlyList<JObject>, string?, long?)>.Success(
                (results.AsReadOnly(), nextPageToken, null));
        }
        catch (Exception e)
        {
            return DatabaseResult<(IReadOnlyList<JObject>, string?, long?)>.Failure(
                $"DatabaseServiceGC->ScanTableWithFilterPaginatedAsync: {e.Message}");
        }
    }

    /// <summary>
    /// Adds elements to an array attribute of an item.
    /// </summary>
    public async Task<DatabaseResult<JObject?>> AddElementsToArrayAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        string arrayAttributeName,
        PrimitiveType[] elementsToAdd,
        ReturnItemBehavior returnBehavior = ReturnItemBehavior.DoNotReturn,
        DatabaseAttributeCondition? condition = null,
        CancellationToken cancellationToken = default)
    {
        if (DSDB == null)
        {
            return DatabaseResult<JObject?>.Failure("DatabaseServiceGC->AddElementsToArrayAsync: DSDB is null.");
        }

        const int maxRetryNumber = 5;
        int retryCount = 0;

        while (++retryCount <= maxRetryNumber)
        {
            try
            {
                if (elementsToAdd.Length == 0)
                {
                    return DatabaseResult<JObject?>.Failure("ElementsToAdd must contain values.");
                }

                if (!LoadStoreAndGetKindKeyFactory(tableName, out var factory, null))
                {
                    return DatabaseResult<JObject?>.Failure("Failed to load table key factory");
                }

                using var transaction = await DSDB.BeginTransactionAsync();
                var key = factory!.CreateKey(GetFinalKeyFromNameValue(keyName, keyValue));

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
                            return DatabaseResult<JObject?>.Failure("Condition not satisfied");
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
                        var updatedEntity = FromJsonToEntity(factory, keyName, keyValue, entityJson);
                        if (updatedEntity != null)
                        {
                            transaction.Upsert(updatedEntity);
                            await transaction.CommitAsync();

                            if (returnBehavior == ReturnItemBehavior.ReturnNewValues)
                            {
                                var getResult = await GetItemAsync(tableName, keyName, keyValue, null, cancellationToken);
                                return getResult;
                            }

                            return DatabaseResult<JObject?>.Success(returnItem);
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
                    
                    var newEntity = FromJsonToEntity(factory, keyName, keyValue, newJson);
                    if (newEntity != null)
                    {
                        transaction.Upsert(newEntity);
                        await transaction.CommitAsync();

                        if (returnBehavior == ReturnItemBehavior.ReturnNewValues)
                        {
                            var getResult = await GetItemAsync(tableName, keyName, keyValue, null, cancellationToken);
                            return getResult;
                        }

                        return DatabaseResult<JObject?>.Success(returnItem);
                    }
                }

                return DatabaseResult<JObject?>.Failure("Failed to process array update");
            }
            catch (Exception e) when (CheckForRetriability(e))
            {
                await Task.Delay(5000, cancellationToken);
                continue;
            }
            catch (Exception e)
            {
                return DatabaseResult<JObject?>.Failure($"DatabaseServiceGC->AddElementsToArrayAsync: {e.Message}");
            }
        }

        return DatabaseResult<JObject?>.Failure($"DatabaseServiceGC->AddElementsToArrayAsync: Too much contention on datastore entities; tried {maxRetryNumber} times");
    }

    /// <summary>
    /// Removes elements from an array attribute of an item.
    /// </summary>
    public async Task<DatabaseResult<JObject?>> RemoveElementsFromArrayAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        string arrayAttributeName,
        PrimitiveType[] elementsToRemove,
        ReturnItemBehavior returnBehavior = ReturnItemBehavior.DoNotReturn,
        DatabaseAttributeCondition? condition = null,
        CancellationToken cancellationToken = default)
    {
        if (DSDB == null)
        {
            return DatabaseResult<JObject?>.Failure("DatabaseServiceGC->RemoveElementsFromArrayAsync: DSDB is null.");
        }

        const int maxRetryNumber = 5;
        int retryCount = 0;

        while (++retryCount <= maxRetryNumber)
        {
            try
            {
                if (elementsToRemove.Length == 0)
                {
                    return DatabaseResult<JObject?>.Failure("ElementsToRemove must contain values.");
                }

                if (!LoadStoreAndGetKindKeyFactory(tableName, out var factory, null))
                {
                    return DatabaseResult<JObject?>.Failure("Failed to load table key factory");
                }

                using var transaction = await DSDB.BeginTransactionAsync();
                var key = factory!.CreateKey(GetFinalKeyFromNameValue(keyName, keyValue));

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
                            return DatabaseResult<JObject?>.Failure("Condition not satisfied");
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
                                PrimitiveTypeKind.Double => element.AsDouble.ToString(),
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
                            var updatedEntity = FromJsonToEntity(factory, keyName, keyValue, entityJson);
                            if (updatedEntity != null)
                            {
                                transaction.Upsert(updatedEntity);
                                await transaction.CommitAsync();

                                if (returnBehavior == ReturnItemBehavior.ReturnNewValues)
                                {
                                    var getResult = await GetItemAsync(tableName, keyName, keyValue, null, cancellationToken);
                                    return getResult;
                                }

                                return DatabaseResult<JObject?>.Success(returnItem);
                            }
                        }
                    }
                }

                return DatabaseResult<JObject?>.Success(returnItem);
            }
            catch (Exception e) when (CheckForRetriability(e))
            {
                await Task.Delay(5000, cancellationToken);
                continue;
            }
            catch (Exception e)
            {
                return DatabaseResult<JObject?>.Failure($"DatabaseServiceGC->RemoveElementsFromArrayAsync: {e.Message}");
            }
        }

        return DatabaseResult<JObject?>.Failure($"DatabaseServiceGC->RemoveElementsFromArrayAsync: Too much contention on datastore entities; tried {maxRetryNumber} times");
    }

    /// <summary>
    /// Atomically increments or decrements a numeric attribute of an item.
    /// </summary>
    public async Task<DatabaseResult<double>> IncrementAttributeAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        string numericAttributeName,
        double incrementValue,
        DatabaseAttributeCondition? condition = null,
        CancellationToken cancellationToken = default)
    {
        if (DSDB == null)
        {
            return DatabaseResult<double>.Failure("DatabaseServiceGC->IncrementAttributeAsync: DSDB is null.");
        }

        const int maxRetryNumber = 5;
        int retryCount = 0;

        while (++retryCount <= maxRetryNumber)
        {
            try
            {
                if (!LoadStoreAndGetKindKeyFactory(tableName, out var factory, null))
                {
                    return DatabaseResult<double>.Failure("Failed to load table key factory");
                }

                using var transaction = await DSDB.BeginTransactionAsync();
                var key = factory!.CreateKey(GetFinalKeyFromNameValue(keyName, keyValue));

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
                            return DatabaseResult<double>.Failure("Condition not satisfied");
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
                        var updatedEntity = FromJsonToEntity(factory, keyName, keyValue, entityJson);
                        if (updatedEntity != null)
                        {
                            transaction.Upsert(updatedEntity);
                            await transaction.CommitAsync();

                            return DatabaseResult<double>.Success(newValue);
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

                    var newEntity = FromJsonToEntity(factory, keyName, keyValue, newJson);
                    if (newEntity != null)
                    {
                        transaction.Upsert(newEntity);
                        await transaction.CommitAsync();

                        return DatabaseResult<double>.Success(newValue);
                    }
                }

                return DatabaseResult<double>.Failure("Failed to increment attribute");
            }
            catch (Exception e) when (CheckForRetriability(e))
            {
                await Task.Delay(5000, cancellationToken);
                continue;
            }
            catch (Exception e)
            {
                return DatabaseResult<double>.Failure($"DatabaseServiceGC->IncrementAttributeAsync: {e.Message}");
            }
        }

        return DatabaseResult<double>.Failure($"DatabaseServiceGC->IncrementAttributeAsync: Too much contention on datastore entities; tried {maxRetryNumber} times");
    }

    #endregion

    #region Private Helper Methods

    private enum PutOrUpdateItemType
    {
        PutItem,
        UpdateItem
    }

    private async Task<DatabaseResult<JObject?>> PutOrUpdateItemAsync(
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
        if (DSDB == null)
        {
            return DatabaseResult<JObject?>.Failure("DatabaseServiceGC->PutOrUpdateItemAsync: DSDB is null.");
        }

        const int maxRetryNumber = 5;
        var newItemCopy = new JObject(newItem);

        newItemCopy.Remove(keyName);

        int retryCount = 0;
        while (++retryCount <= maxRetryNumber)
        {
            try
            {
                if (!LoadStoreAndGetKindKeyFactory(tableName, out var factory, null))
                {
                    return DatabaseResult<JObject?>.Failure("Failed to load table key factory");
                }

                using var transaction = await DSDB.BeginTransactionAsync();
                var key = factory!.CreateKey(GetFinalKeyFromNameValue(keyName, keyValue));
                
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
                            return DatabaseResult<JObject?>.Failure("Condition not satisfied");
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
                        return DatabaseResult<JObject?>.Failure("Item already exists");
                    }
                }

                JObject? returnItem = null;
                if (returnBehavior == ReturnItemBehavior.ReturnOldValues)
                {
                    returnItem = returnedPreOperationObject ?? [];
                }

                var itemAsEntity = FromJsonToEntity(factory, keyName, keyValue, newItemCopy);
                if (itemAsEntity != null)
                {
                    transaction.Upsert(itemAsEntity);
                    await transaction.CommitAsync();

                    if (returnBehavior == ReturnItemBehavior.ReturnNewValues)
                    {
                        var getResult = await GetItemAsync(tableName, keyName, keyValue, null, cancellationToken);
                        return getResult;
                    }

                    return DatabaseResult<JObject?>.Success(returnItem);
                }

                return DatabaseResult<JObject?>.Failure("Failed to convert JSON to entity");
            }
            catch (Exception e) when (CheckForRetriability(e))
            {
                await Task.Delay(5000, cancellationToken);
                continue;
            }
            catch (Exception e)
            {
                return DatabaseResult<JObject?>.Failure($"DatabaseServiceGC->PutOrUpdateItemAsync: {e.Message}");
            }
        }

        return DatabaseResult<JObject?>.Failure($"DatabaseServiceGC->PutOrUpdateItemAsync: Too much contention on datastore entities; tried {maxRetryNumber} times");
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
                    ((string)token!).CompareTo(primitive.AsString),
                _ => 0
            };
        }
        catch
        {
            return 0;
        }
    }

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

    public async ValueTask DisposeAsync()
    {
        // Google Cloud Datastore clients are designed to be long-lived
        // No explicit disposal is typically needed, but we suppress finalization
        System.GC.SuppressFinalize(this);
        await Task.CompletedTask;
    }
}
