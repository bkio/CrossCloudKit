// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

//
// Note: It's essential that methods defined in IDatabaseService are not called directly from ...CoreAsync methods.
// Instead, call CoreAsync methods when needed.
//

using System.Collections.Concurrent;
using System.Net;
using System.Text;
using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Interfaces.Enums;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Driver;
using CrossCloudKit.Utilities.Common;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

namespace CrossCloudKit.Database.Mongo;

public sealed class DatabaseServiceMongo : DatabaseServiceBase, IDisposable
{
    /// <summary>
    /// Holds initialization success
    /// </summary>
    private readonly bool _bInitializationSucceed;

    /// <summary>
    /// Gets a value indicating whether the database service has been successfully initialized.
    /// </summary>
    // ReSharper disable once ConvertToAutoProperty
    public override bool IsInitialized => _bInitializationSucceed;

    private readonly IMongoDatabase? _mongoDB;
    private readonly MongoClient? _mongoClient;

    private readonly Dictionary<string, IMongoCollection<BsonDocument>> _tableMap = [];
    private readonly Lock _tableMapDictionaryLock = new();

    /// <summary>
    /// DatabaseServiceMongoDB: Constructor for MongoDB connection using host and port
    /// </summary>
    /// <param name="mongoHost">MongoDB Host</param>
    /// <param name="mongoPort">MongoDB Port</param>
    /// <param name="mongoDatabase">MongoDB Database Name</param>
    /// <param name="errorMessageAction">Error messages will be pushed to this action</param>
    public DatabaseServiceMongo(
        string mongoHost,
        int mongoPort,
        string mongoDatabase,
        Action<string>? errorMessageAction = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(mongoHost);
        ArgumentException.ThrowIfNullOrWhiteSpace(mongoDatabase);

        try
        {
            _mongoClient = new MongoClient($"mongodb://{mongoHost}:{mongoPort}");
            _mongoDB = _mongoClient.GetDatabase(mongoDatabase);
            _bInitializationSucceed = _mongoDB != null;
        }
        catch (Exception e)
        {
            errorMessageAction?.Invoke($"DatabaseServiceMongoDB->Constructor: {e.Message} \n Trace: {e.StackTrace}");
            _bInitializationSucceed = false;
        }
    }

    /// <summary>
    /// DatabaseServiceMongoDB: Constructor for MongoDB connection using connection string
    /// </summary>
    /// <param name="connectionString">MongoDB connection string</param>
    /// <param name="mongoDatabase">MongoDB Database Name</param>
    /// <param name="errorMessageAction">Error messages will be pushed to this action</param>
    public DatabaseServiceMongo(
        string connectionString,
        string mongoDatabase,
        Action<string>? errorMessageAction = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(mongoDatabase);

        try
        {
            _mongoClient = new MongoClient(connectionString);
            _mongoDB = _mongoClient.GetDatabase(mongoDatabase);
            _bInitializationSucceed = _mongoDB != null;
        }
        catch (Exception e)
        {
            errorMessageAction?.Invoke($"DatabaseServiceMongoDB->Constructor: {e.Message} \n Trace: {e.StackTrace}");
            _bInitializationSucceed = false;
        }
    }

    /// <summary>
    /// DatabaseServiceMongoDB: Advanced constructor for MongoDB with configuration JSON
    /// </summary>
    public DatabaseServiceMongo(
        string mongoClientConfigJson,
        string mongoPassword,
        string mongoDatabase,
        Action<string>? errorMessageAction = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(mongoClientConfigJson);
        ArgumentException.ThrowIfNullOrWhiteSpace(mongoDatabase);

        try
        {
            var clientConfigString = mongoClientConfigJson;

            // Parse the Client Config Json if it's a base64 encoded (for running on local environment with launchSettings.json)
            Span<byte> buffer = stackalloc byte[clientConfigString.Length];
            if (Convert.TryFromBase64String(clientConfigString, buffer, out var bytesParsed))
            {
                if (bytesParsed > 0)
                {
                    clientConfigString = Encoding.UTF8.GetString(buffer[..bytesParsed]);
                }
            }

            var clientConfigJObject = JObject.Parse(clientConfigString);

            var hostTokens = clientConfigJObject.SelectTokens("$...hostname");
            var hosts = hostTokens.Select(item => item.ToObject<string>().NotNull()).ToList();

            var portTokens = clientConfigJObject.SelectTokens("$....port");
            var ports = portTokens.Select(item => item.ToObject<int>()).ToList();

            var replicaSetName = clientConfigJObject.SelectToken("replicaSets[0]._id")?.ToObject<string>();
            var databaseName = clientConfigJObject.SelectToken("auth.usersWanted[0].db")?.ToObject<string>();
            var userName = clientConfigJObject.SelectToken("auth.usersWanted[0].user")?.ToObject<string>();
            var authMechanism = clientConfigJObject.SelectToken("auth.autoAuthMechanism")?.ToObject<string>();

            const int mongoDBPort = 27017;

            var serverList = hosts.Select((host, i) =>
                new MongoServerAddress(host, i < ports.Count ? ports[i] : mongoDBPort)).ToList();

            if (databaseName != null && userName != null && authMechanism != null)
            {
                var internalIdentity = new MongoInternalIdentity(databaseName, userName);
                var passwordEvidence = new PasswordEvidence(mongoPassword);
                var mongoCredential = new MongoCredential(authMechanism, internalIdentity, passwordEvidence);

                var settings = new MongoClientSettings
                {
                    Servers = serverList,
                    ReplicaSetName = replicaSetName,
                    Credential = mongoCredential
                };

                _mongoClient = new MongoClient(settings);
            }
            else
            {
                var settings = new MongoClientSettings
                {
                    Servers = serverList,
                    ReplicaSetName = replicaSetName
                };

                _mongoClient = new MongoClient(settings);
            }

            _mongoDB = _mongoClient.GetDatabase(mongoDatabase);
            _bInitializationSucceed = _mongoDB != null;
        }
        catch (Exception e)
        {
            errorMessageAction?.Invoke($"DatabaseServiceMongoDB->Constructor: {e.Message} \n Trace: {e.StackTrace}");
            _bInitializationSucceed = false;
        }
    }

    private async Task<bool> TableExistsAsync(string tableName, CancellationToken cancellationToken)
    {
        if (_mongoDB == null) return false;

        var filter = new BsonDocument("name", tableName);
        var options = new ListCollectionNamesOptions { Filter = filter };

        return await (await _mongoDB.ListCollectionNamesAsync(options, cancellationToken)).AnyAsync(cancellationToken: cancellationToken);
    }

    /// <summary>
    /// If given table (collection) does not exist in the database, it will create a new table (collection)
    /// </summary>
    private async Task<bool> TryCreateTableAsync(string tableName, CancellationToken cancellationToken = default)
    {
        try
        {
            if (_mongoDB == null) return false;

            if (!await TableExistsAsync(tableName, cancellationToken))
            {
                await _mongoDB.CreateCollectionAsync(tableName, cancellationToken: cancellationToken);
            }
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }

    private async Task<IMongoCollection<BsonDocument>?> GetTableAsync(string tableName, CancellationToken cancellationToken = default)
    {
        lock (_tableMapDictionaryLock)
        {
            if (_tableMap.TryGetValue(tableName, out var existingTable))
            {
                return existingTable;
            }
        }

        if (!await TryCreateTableAsync(tableName, cancellationToken))
        {
            return null;
        }

        var tableObj = _mongoDB?.GetCollection<BsonDocument>(tableName);
        if (tableObj != null)
        {
            lock (_tableMapDictionaryLock)
            {
                _tableMap.TryAdd(tableName, tableObj);
            }
        }

        return tableObj;
    }

    private static FilterDefinition<BsonDocument> BuildEqFilter(string keyName, PrimitiveType keyValue) => keyValue.Kind switch
    {
        PrimitiveTypeKind.Double => Builders<BsonDocument>.Filter.Eq(keyName, keyValue.AsDouble),
        PrimitiveTypeKind.Boolean => Builders<BsonDocument>.Filter.Eq(keyName, keyValue.AsBoolean),
        PrimitiveTypeKind.Integer => Builders<BsonDocument>.Filter.Eq(keyName, keyValue.AsInteger),
        PrimitiveTypeKind.ByteArray => Builders<BsonDocument>.Filter.Eq(keyName, Convert.ToBase64String(keyValue.AsByteArray)), // Convert to Base64 to match JSON storage
        PrimitiveTypeKind.String => Builders<BsonDocument>.Filter.Eq(keyName, keyValue.AsString),
        _ => Builders<BsonDocument>.Filter.Eq(keyName, keyValue.ToString())
    };

    #region Modern Async API

    /// <inheritdoc />
    protected override async Task<OperationResult<bool>> ItemExistsCoreAsync(
        string tableName,
        DbKey key,
        IEnumerable<DbAttributeCondition>? conditions = null,
        bool isCalledFromSanityCheck = false,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<bool>.Failure("Failed to get table", HttpStatusCode.InternalServerError);
            }

            var filter = BuildEqFilter(key.Name, key.Value);

            var conditionBuilt = false;
            if (conditions != null && BuildConditionsFilter(out var finalFilter, conditions, filter))
            {
                conditionBuilt = true;
                if (await FindOneAsync(table, finalFilter, cancellationToken) != null)
                {
                    return OperationResult<bool>.Success(true);
                }
            }

            if (await FindOneAsync(table, filter, cancellationToken) == null)
            {
                return OperationResult<bool>.Failure("Item not found.", HttpStatusCode.NotFound);
            }
            return conditionBuilt
                ? OperationResult<bool>.Failure("Conditions are not satisfied.", HttpStatusCode.PreconditionFailed)
                : OperationResult<bool>.Success(true);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure($"DatabaseServiceMongoDB->ItemExistsAsync: {ex.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<JObject?>> GetItemCoreAsync(
        string tableName,
        DbKey key,
        string[]? attributesToRetrieve = null,
        bool isCalledInternally = false,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<JObject?>.Failure("Failed to get table", HttpStatusCode.InternalServerError);
            }

            var filter = BuildEqFilter(key.Name, key.Value);
            var document = await FindOneAsync(table, filter, cancellationToken);

            if (document == null)
            {
                return OperationResult<JObject?>.Success(null);
            }

            var result = BsonToJObject(document);
            if (result != null)
            {
                AddKeyToJson(result, key.Name, key.Value);
                ApplyOptions(result);
            }

            return OperationResult<JObject?>.Success(result);
        }
        catch (Exception ex)
        {
            return OperationResult<JObject?>.Failure($"DatabaseServiceMongoDB->GetItemAsync: {ex.Message}", HttpStatusCode.InternalServerError);
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

            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<IReadOnlyList<JObject>>.Failure("Failed to get table", HttpStatusCode.InternalServerError);
            }

            var filter = keys.Aggregate(
                Builders<BsonDocument>.Filter.Empty,
                (current, key) => current == Builders<BsonDocument>.Filter.Empty
                    ? BuildEqFilter(key.Name, key.Value)
                    : Builders<BsonDocument>.Filter.Or(current, BuildEqFilter(key.Name, key.Value)));

            var documents = await table.Find(filter).ToListAsync(cancellationToken);
            var results = new List<JObject>();

            foreach (var document in documents)
            {
                var createdJson = BsonToJObject(document);
                if (createdJson != null)
                {
                    foreach (var key in keys)
                    {
                        if (document.TryGetElement(key.Name, out var valueElement))
                        {
                            AddKeyToJson(createdJson, key.Name, BsonValueToPrimitiveType(valueElement.Value));
                        }
                    }

                    ApplyOptions(createdJson);
                    results.Add(createdJson);
                }
            }

            return OperationResult<IReadOnlyList<JObject>>.Success(results.AsReadOnly());
        }
        catch (Exception e)
        {
            return OperationResult<IReadOnlyList<JObject>>.Failure($"DatabaseServiceMongoDB->GetItemsAsync: {e.Message}", HttpStatusCode.InternalServerError);
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
        bool isCalledFromPostDropTable = false,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<JObject?>.Failure("Failed to get table", HttpStatusCode.InternalServerError);
            }

            var filter = BuildEqFilter(key.Name, key.Value);
            if (conditions != null && BuildConditionsFilter(out filter, conditions, filter))
            {
                if (!await HasTableMatchingResultWithFilterAsync(table, filter, cancellationToken))
                {
                    return OperationResult<JObject?>.Failure("Condition not satisfied", HttpStatusCode.PreconditionFailed);
                }
            }

            JObject? returnItem = null;
            if (returnBehavior == DbReturnItemBehavior.ReturnOldValues)
            {
                var document = await FindOneAsync(table, BuildEqFilter(key.Name, key.Value), cancellationToken);
                if (document != null)
                {
                    returnItem = BsonToJObject(document);
                    if (returnItem != null)
                    {
                        AddKeyToJson(returnItem, key.Name, key.Value);
                        ApplyOptions(returnItem);
                    }
                }
            }

            await table.DeleteOneAsync(BuildEqFilter(key.Name, key.Value), cancellationToken);
            return OperationResult<JObject?>.Success(returnItem);
        }
        catch (Exception ex)
        {
            return OperationResult<JObject?>.Failure($"DatabaseServiceMongoDB->DeleteItemAsync: {ex.Message}", HttpStatusCode.InternalServerError);
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

            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<JObject?>.Failure("Failed to get table", HttpStatusCode.InternalServerError);
            }

            var filter = BuildEqFilter(key.Name, key.Value);

            if (conditions != null && BuildConditionsFilter(out var finalFilter, conditions, filter))
            {
                if (!await HasTableMatchingResultWithFilterAsync(table, finalFilter, cancellationToken))
                {
                    return OperationResult<JObject?>.Failure("Condition not satisfied", HttpStatusCode.PreconditionFailed);
                }
            }

            JObject? returnItem = null;
            if (returnBehavior == DbReturnItemBehavior.ReturnOldValues)
            {
                var document = await FindOneAsync(table, filter, cancellationToken);
                if (document != null)
                {
                    returnItem = BsonToJObject(document);
                    if (returnItem != null)
                    {
                        AddKeyToJson(returnItem, key.Name, key.Value);
                        ApplyOptions(returnItem);
                    }
                }
            }

            var elementsToAddList = elementsToAdd.Select(element => element.Kind switch
            {
                PrimitiveTypeKind.String => (object)element.AsString,
                PrimitiveTypeKind.Integer => element.AsInteger,
                PrimitiveTypeKind.Double => element.AsDouble,
                PrimitiveTypeKind.ByteArray => element.AsByteArray,
                _ => element.ToString()
            }).ToList();

            var update = Builders<BsonDocument>.Update.PushEach(arrayAttributeName, elementsToAddList);

            var updateTask = table.UpdateOneAsync(filter, update, new UpdateOptions { IsUpsert = true }, cancellationToken);
            if (!isCalledFromPostInsert)
            {
                var postInsertTask = PostInsertItemAsync(tableName, key, cancellationToken);

                await Task.WhenAll(updateTask, postInsertTask);

                var postInsertResult = await postInsertTask;
                if (!postInsertResult.IsSuccessful)
                {
                    return OperationResult<JObject?>.Failure(
                        $"PostInsertItemAsync failed with: {postInsertResult.ErrorMessage}",
                        postInsertResult.StatusCode);
                }
            }
            else await updateTask;

            if (returnBehavior == DbReturnItemBehavior.ReturnNewValues)
            {
                var document = await FindOneAsync(table, filter, cancellationToken);
                if (document != null)
                {
                    returnItem = BsonToJObject(document);
                    if (returnItem != null)
                    {
                        AddKeyToJson(returnItem, key.Name, key.Value);
                        ApplyOptions(returnItem);
                    }
                }
            }

            return OperationResult<JObject?>.Success(returnItem);
        }
        catch (Exception ex)
        {
            return OperationResult<JObject?>.Failure($"DatabaseServiceMongoDB->AddElementsToArrayAsync: {ex.Message}", HttpStatusCode.InternalServerError);
        }
    }

    private static bool BuildConditionsFilter(
        out FilterDefinition<BsonDocument> finalFilter,
        IEnumerable<DbAttributeCondition> conditions,
        FilterDefinition<BsonDocument>? baseFilter = null)
    {
        var anyFilterAdded = false;
        finalFilter = baseFilter ?? Builders<BsonDocument>.Filter.Empty;

        foreach (var condition in conditions)
        {
            var conditionFilter = BuildFilterFromCondition(condition);
            if (conditionFilter != null)
            {
                anyFilterAdded = true;
                finalFilter = Builders<BsonDocument>.Filter.And(finalFilter, conditionFilter);
            }
        }

        return anyFilterAdded;
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

            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<JObject?>.Failure("Failed to get table", HttpStatusCode.InternalServerError);
            }

            var filter = BuildEqFilter(key.Name, key.Value);
            if (conditions != null && BuildConditionsFilter(out var finalFilter, conditions, filter))
            {
                if (!await HasTableMatchingResultWithFilterAsync(table, finalFilter, cancellationToken))
                {
                    return OperationResult<JObject?>.Failure("Condition not satisfied", HttpStatusCode.PreconditionFailed);
                }
            }

            JObject? returnItem = null;
            if (returnBehavior == DbReturnItemBehavior.ReturnOldValues)
            {
                var document = await FindOneAsync(table, filter, cancellationToken);
                if (document != null)
                {
                    returnItem = BsonToJObject(document);
                    if (returnItem != null)
                    {
                        AddKeyToJson(returnItem, key.Name, key.Value);
                        ApplyOptions(returnItem);
                    }
                }
            }

            var elementsToRemoveList = elementsToRemove.Select(element => element.Kind switch
            {
                PrimitiveTypeKind.String => (object)element.AsString,
                PrimitiveTypeKind.Integer => element.AsInteger,
                PrimitiveTypeKind.Double => element.AsDouble,
                PrimitiveTypeKind.ByteArray => element.AsByteArray,
                _ => element.ToString()
            }).ToList();

            var update = Builders<BsonDocument>.Update.PullAll(arrayAttributeName, elementsToRemoveList);
            await table.UpdateOneAsync(filter, update, cancellationToken: cancellationToken);

            if (returnBehavior == DbReturnItemBehavior.ReturnNewValues)
            {
                var document = await FindOneAsync(table, filter, cancellationToken);
                if (document != null)
                {
                    returnItem = BsonToJObject(document);
                    if (returnItem != null)
                    {
                        AddKeyToJson(returnItem, key.Name, key.Value);
                        ApplyOptions(returnItem);
                    }
                }
            }

            return OperationResult<JObject?>.Success(returnItem);
        }
        catch (Exception ex)
        {
            return OperationResult<JObject?>.Failure($"DatabaseServiceMongoDB->RemoveElementsFromArrayAsync: {ex.Message}", HttpStatusCode.InternalServerError);
        }
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
        try
        {
            var sanityCheckResult = await AttributeNamesSanityCheck(key, tableName, new JObject { [numericAttributeName] = new JArray() }, cancellationToken);
            if (!sanityCheckResult.IsSuccessful)
            {
                return OperationResult<double>.Failure(sanityCheckResult.ErrorMessage, sanityCheckResult.StatusCode);
            }

            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<double>.Failure("Failed to get table", HttpStatusCode.InternalServerError);
            }

            var filter = BuildEqFilter(key.Name, key.Value);

            if (conditions != null && BuildConditionsFilter(out var finalFilter, conditions, filter))
            {
                if (!await HasTableMatchingResultWithFilterAsync(table, finalFilter, cancellationToken))
                {
                    return OperationResult<double>.Failure("Condition not satisfied", HttpStatusCode.PreconditionFailed);
                }
            }

            var update = Builders<BsonDocument>.Update.Inc(numericAttributeName, incrementValue);
            var options = new FindOneAndUpdateOptions<BsonDocument>
            {
                ReturnDocument = ReturnDocument.After,
                IsUpsert = true
            };

            var findUpdateTask = table.FindOneAndUpdateAsync(filter, update, options, cancellationToken);
            var postInsertTask = PostInsertItemAsync(tableName, key, cancellationToken);

            await Task.WhenAll(findUpdateTask, postInsertTask);

            var postInsertResult = await postInsertTask;
            if (!postInsertResult.IsSuccessful)
            {
                return OperationResult<double>.Failure($"PostInsertItemAsync failed with {postInsertResult.ErrorMessage}", postInsertResult.StatusCode);
            }

            var document = await findUpdateTask;
            if (document != null && document.TryGetValue(numericAttributeName, out var value))
            {
                return OperationResult<double>.Success(value.AsDouble);
            }

            return OperationResult<double>.Failure("Failed to get updated value", HttpStatusCode.InternalServerError);
        }
        catch (Exception ex)
        {
            return OperationResult<double>.Failure($"DatabaseServiceMongoDB->IncrementAttributeAsync: {ex.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<(IReadOnlyList<string> Keys, IReadOnlyList<JObject> Items)>> ScanTableCoreAsync(
        string tableName,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<(IReadOnlyList<string>, IReadOnlyList<JObject>)>.Failure("Failed to get table", HttpStatusCode.InternalServerError);
            }

            var filter = Builders<BsonDocument>.Filter.Empty;

            var findTask = table.Find(filter).ToListAsync(cancellationToken);
            var getKeysTask = GetTableKeysCoreAsync(tableName, true, cancellationToken);

            await Task.WhenAll(getKeysTask, findTask);

            var getKeysResult = await getKeysTask;
            if (!getKeysResult.IsSuccessful)
                return OperationResult<(IReadOnlyList<string>, IReadOnlyList<JObject>)>.Failure(getKeysResult.ErrorMessage, getKeysResult.StatusCode);

            var processResult = ProcessScanResults(await findTask);
            if (!processResult.IsSuccessful)
                return OperationResult<(IReadOnlyList<string>, IReadOnlyList<JObject>)>.Failure(processResult.ErrorMessage, processResult.StatusCode);

            return OperationResult<(IReadOnlyList<string> Keys, IReadOnlyList<JObject> Items)>.Success((getKeysResult.Data, processResult.Data));
        }
        catch (Exception e)
        {
            return OperationResult<(IReadOnlyList<string>, IReadOnlyList<JObject>)>
                .Failure($"DatabaseServiceMongoDB->ScanTableAsync: {e.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    protected override async
        Task<OperationResult<
            (IReadOnlyList<string>? Keys,
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
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<(IReadOnlyList<string>?, IReadOnlyList<JObject>, string?, long?)>.Failure("Failed to get table", HttpStatusCode.InternalServerError);
            }

            var filter = Builders<BsonDocument>.Filter.Empty;

            var scanTask = Task.Run(async () =>
            {
                var totalCount = await table.CountDocumentsAsync(filter, cancellationToken: cancellationToken);

                var skip = 0;
                if (int.TryParse(pageToken, out var pageTokenInt))
                {
                    skip = pageTokenInt;
                }

                var items = await table.Find(filter)
                    .Skip(skip)
                    .Limit(pageSize)
                    .ToListAsync(cancellationToken);
                return (totalCount, skip, items);
            }, cancellationToken);

            var getKeysTask = GetTableKeysCoreAsync(tableName, true, cancellationToken);

            await Task.WhenAll(getKeysTask, scanTask);

            var getKeysResult = await getKeysTask;
            if (!getKeysResult.IsSuccessful)
                return OperationResult<(IReadOnlyList<string>?, IReadOnlyList<JObject>, string?, long?)>.Failure(getKeysResult.ErrorMessage, getKeysResult.StatusCode);

            var scanResult = await scanTask;

            var processResult = ProcessScanResults(scanResult.items);
            if (!processResult.IsSuccessful)
                return OperationResult<(IReadOnlyList<string>?, IReadOnlyList<JObject>, string?, long?)>.Failure(processResult.ErrorMessage, processResult.StatusCode);

            var nextPageToken = scanResult.skip + pageSize < scanResult.totalCount ? (scanResult.skip + pageSize).ToString() : null;
            return OperationResult<(IReadOnlyList<string>?, IReadOnlyList<JObject>, string?, long?)>.Success(
                (getKeysResult.Data, processResult.Data, nextPageToken, scanResult.totalCount));
        }
        catch (Exception e)
        {
            return OperationResult<(IReadOnlyList<string>?, IReadOnlyList<JObject>, string?, long?)>.Failure(
                $"DatabaseServiceMongoDB->ScanTablePaginatedAsync: {e.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<(IReadOnlyList<string> Keys, IReadOnlyList<JObject> Items)>> ScanTableWithFilterCoreAsync(
        string tableName,
        IEnumerable<DbAttributeCondition> filterConditions,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<(IReadOnlyList<string>, IReadOnlyList<JObject>)>.Failure("Failed to get table", HttpStatusCode.InternalServerError);
            }

            BuildConditionsFilter(out var filter, filterConditions);

            var getKeysTask = GetTableKeysCoreAsync(tableName, true, cancellationToken);
            var findTask = table.Find(filter).ToListAsync(cancellationToken);

            await Task.WhenAll(getKeysTask, findTask);

            var getKeysResult = await getKeysTask;
            if (!getKeysResult.IsSuccessful)
                return OperationResult<(IReadOnlyList<string>, IReadOnlyList<JObject>)>.Failure(getKeysResult.ErrorMessage, getKeysResult.StatusCode);

            var processResult = ProcessScanResults(await findTask);
            if (!processResult.IsSuccessful)
                return OperationResult<(IReadOnlyList<string>, IReadOnlyList<JObject>)>.Failure(processResult.ErrorMessage, processResult.StatusCode);

            return OperationResult<(IReadOnlyList<string> Keys, IReadOnlyList<JObject> Items)>.Success((getKeysResult.Data, processResult.Data));
        }
        catch (Exception e)
        {
            return OperationResult<(IReadOnlyList<string>, IReadOnlyList<JObject>)>.Failure(
                $"DatabaseServiceMongoDB->ScanTableWithFilterAsync: {e.Message}", HttpStatusCode.InternalServerError);
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
        IEnumerable<DbAttributeCondition> filterConditions,
        int pageSize,
        string? pageToken = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<(IReadOnlyList<string>?, IReadOnlyList<JObject>, string?, long?)>.Failure("Failed to get table", HttpStatusCode.InternalServerError);
            }

            BuildConditionsFilter(out var filter, filterConditions);

            var scanTask = Task.Run(async () =>
            {
                var totalCount = await table.CountDocumentsAsync(filter, cancellationToken: cancellationToken);

                var skip = 0;
                if (int.TryParse(pageToken, out var pageTokenInt))
                {
                    skip = pageTokenInt;
                }

                var items = await table.Find(filter)
                    .Skip(skip)
                    .Limit(pageSize)
                    .ToListAsync(cancellationToken);

                return (totalCount, skip, items);
            }, cancellationToken);

            var getKeysTask = GetTableKeysCoreAsync(tableName, true, cancellationToken);

            await Task.WhenAll(getKeysTask, scanTask);

            var getKeysResult = await getKeysTask;
            if (!getKeysResult.IsSuccessful)
                return OperationResult<(IReadOnlyList<string>?, IReadOnlyList<JObject>, string?, long?)>.Failure(getKeysResult.ErrorMessage, getKeysResult.StatusCode);

            var scanResult = await scanTask;

            var processResult = ProcessScanResults(scanResult.items);
            if (!processResult.IsSuccessful)
                return OperationResult<(IReadOnlyList<string>?, IReadOnlyList<JObject>, string?, long?)>.Failure(processResult.ErrorMessage, processResult.StatusCode);

            var nextPageToken = scanResult.skip + pageSize < scanResult.totalCount ? (scanResult.skip + pageSize).ToString() : null;
            return OperationResult<(IReadOnlyList<string>?, IReadOnlyList<JObject>, string?, long?)>.Success(
                (getKeysResult.Data, processResult.Data, nextPageToken, scanResult.totalCount));
        }
        catch (Exception e)
        {
            return OperationResult<(IReadOnlyList<string>?, IReadOnlyList<JObject>, string?, long?)>.Failure(
                $"DatabaseServiceMongoDB->ScanTableWithFilterPaginatedAsync: {e.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<IReadOnlyList<string>>> GetTableNamesCoreAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            if (_mongoDB == null)
            {
                return OperationResult<IReadOnlyList<string>>.Failure("DatabaseServiceMongoDB->GetTableNamesAsync: MongoDB is null.", HttpStatusCode.ServiceUnavailable);
            }

            var tableNames = new List<string>();
            using var cursor = await _mongoDB.ListCollectionNamesAsync(cancellationToken: cancellationToken);

            while (await cursor.MoveNextAsync(cancellationToken))
            {
                tableNames.AddRange(cursor.Current);
            }

            return OperationResult<IReadOnlyList<string>>.Success(tableNames.AsReadOnly());
        }
        catch (Exception ex)
        {
            return OperationResult<IReadOnlyList<string>>.Failure($"DatabaseServiceMongoDB->GetTableNamesAsync: {ex.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<bool>> DropTableCoreAsync(string tableName, CancellationToken cancellationToken = default)
    {
        try
        {
            if (_mongoDB == null)
            {
                return OperationResult<bool>.Failure("DatabaseServiceMongoDB->DropTableAsync: MongoDB is null.", HttpStatusCode.ServiceUnavailable);
            }

            // Check if collection exists before attempting to drop it
            if (!await TableExistsAsync(tableName, cancellationToken))
            {
                // Collection doesn't exist, consider this a successful deletion
                return OperationResult<bool>.Success(true);
            }

            // Drop the collection
            var dropCollectionTask = _mongoDB.DropCollectionAsync(tableName, cancellationToken);
            var postDropTableTask = PostDropTableAsync(tableName, cancellationToken);

            await Task.WhenAll(dropCollectionTask, postDropTableTask);

            var postDropTableResult = await postDropTableTask;
            if (!postDropTableResult.IsSuccessful)
            {
                return OperationResult<bool>.Failure(
                    $"PostDropTableAsync has failed with {postDropTableResult.ErrorMessage}",
                    postDropTableResult.StatusCode);
            }

            // Remove from table mapping cache
            lock (_tableMapDictionaryLock)
            {
                _tableMap.Remove(tableName);
            }

            return OperationResult<bool>.Success(true);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure($"DatabaseServiceMongoDB->DropTableAsync: {ex.Message}", HttpStatusCode.InternalServerError);
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
        IEnumerable<DbAttributeCondition>? conditions = null,
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

            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<JObject?>.Failure("Failed to get table", HttpStatusCode.InternalServerError);
            }

            var filter = BuildEqFilter(key.Name, key.Value);

            if (putOrUpdateItemType == PutOrUpdateItemType.PutItem)
            {
                if (!shouldOverrideIfExists)
                {
                    if (await HasTableMatchingResultWithFilterAsync(table, filter, cancellationToken))
                    {
                        return OperationResult<JObject?>.Failure("Item already exists", HttpStatusCode.Conflict);
                    }
                }
            }
            else // UpdateItem
            {
                if (conditions != null && BuildConditionsFilter(out var finalFilter, conditions, filter))
                {
                    if (!await HasTableMatchingResultWithFilterAsync(table, finalFilter, cancellationToken))
                    {
                        return OperationResult<JObject?>.Failure("Condition not satisfied", HttpStatusCode.PreconditionFailed);
                    }
                }
            }

            JObject? returnItem = null;
            if (returnBehavior == DbReturnItemBehavior.ReturnOldValues)
            {
                var document = await FindOneAsync(table, filter, cancellationToken);
                if (document != null)
                {
                    returnItem = BsonToJObject(document);
                    if (returnItem != null)
                    {
                        AddKeyToJson(returnItem, key.Name, key.Value);
                        ApplyOptions(returnItem);
                    }
                }
            }

            var newObject = new JObject(newItem);
            AddKeyToJson(newObject, key.Name, key.Value);

            // Use $set for preventing element name validation exceptions
            var updateDocument = new BsonDocument { { "$set", JObjectToBson(newObject) } };
            var updateTask = table.UpdateOneAsync(filter, updateDocument, new UpdateOptions { IsUpsert = true }, cancellationToken);

            if (putOrUpdateItemType == PutOrUpdateItemType.PutItem)
            {
                var postInsertTask = PostInsertItemAsync(tableName, key, cancellationToken);

                await Task.WhenAll(updateTask, postInsertTask);

                var postInsertResult = await postInsertTask;
                if (!postInsertResult.IsSuccessful)
                {
                    return OperationResult<JObject?>.Failure($"PutItemAsync succeeded, however PostInsertItemAsync failed with: {postInsertResult.ErrorMessage}", postInsertResult.StatusCode);
                }
            }
            else
            {
                await updateTask;
            }

            if (returnBehavior == DbReturnItemBehavior.ReturnNewValues)
            {
                returnItem = newObject;
            }

            return OperationResult<JObject?>.Success(returnItem);
        }
        catch (Exception ex)
        {
            return OperationResult<JObject?>.Failure($"DatabaseServiceMongoDB->PutOrUpdateItemAsync: {ex.Message}", HttpStatusCode.InternalServerError);
        }
    }

    private OperationResult<IReadOnlyList<JObject>> ProcessScanResults(List<BsonDocument> documents)
    {
        try
        {
            var tempResults = new ConcurrentBag<JObject>();

            Parallel.ForEach(documents, document =>
            {
                var createdJson = BsonToJObject(document);
                if (createdJson == null) return;

                ApplyOptions(createdJson);
                tempResults.Add(createdJson);
            });

            return OperationResult<IReadOnlyList<JObject>>.Success(tempResults.ToList().AsReadOnly());
        }
        catch (JsonReaderException e)
        {
            return OperationResult<IReadOnlyList<JObject>>.Failure($"JSON parsing error: {e.Message}", HttpStatusCode.InternalServerError);
        }
        catch (Exception e)
        {
            return OperationResult<IReadOnlyList<JObject>>.Failure($"Processing error: {e.Message}", HttpStatusCode.InternalServerError);
        }
    }

    private static async Task<BsonDocument?> FindOneAsync(
        IMongoCollection<BsonDocument> table,
        FilterDefinition<BsonDocument> filter,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var cursor = await table.FindAsync(filter, cancellationToken: cancellationToken);
            return await cursor.FirstOrDefaultAsync(cancellationToken);
        }
        catch (Exception)
        {
            return null;
        }
    }

    private static async Task<bool> HasTableMatchingResultWithFilterAsync(
        IMongoCollection<BsonDocument> table,
        FilterDefinition<BsonDocument> filter,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var cursor = await table.FindAsync(filter, cancellationToken: cancellationToken);
            return await cursor.AnyAsync(cancellationToken);
        }
        catch (Exception)
        {
            return false;
        }
    }

    private static PrimitiveType BsonValueToPrimitiveType(BsonValue bsonValue)
    {
        return bsonValue.BsonType switch
        {
            BsonType.String => new PrimitiveType(bsonValue.AsString),
            BsonType.Int32 => new PrimitiveType(bsonValue.AsInt32),
            BsonType.Int64 => new PrimitiveType(bsonValue.AsInt64),
            BsonType.Double => new PrimitiveType(bsonValue.AsDouble),
            BsonType.Binary => new PrimitiveType(bsonValue.AsByteArray),
            _ => new PrimitiveType(bsonValue.ToJson())
        };
    }

    private static FilterDefinition<BsonDocument>? BuildFilterFromCondition(DbAttributeCondition condition)
    {
        return condition.ConditionType switch
        {
            DbAttributeConditionType.AttributeExists =>
                Builders<BsonDocument>.Filter.Exists(condition.AttributeName),
            DbAttributeConditionType.AttributeNotExists =>
                Builders<BsonDocument>.Filter.Exists(condition.AttributeName, false),
            _ when condition is DbValueCondition valueCondition => BuildValueFilter(valueCondition),
            _ when condition is DbArrayElementCondition arrayCondition => BuildArrayElementFilter(arrayCondition),
            _ => null
        };
    }

    private static FilterDefinition<BsonDocument>? BuildValueFilter(DbValueCondition condition)
    {
        var value = condition.Value.Kind switch
        {
            PrimitiveTypeKind.String => (object)condition.Value.AsString,
            PrimitiveTypeKind.Integer => condition.Value.AsInteger,
            PrimitiveTypeKind.Double => condition.Value.AsDouble,
            PrimitiveTypeKind.ByteArray => condition.Value.AsByteArray,
            _ => condition.Value.ToString()
        };

        return condition.ConditionType switch
        {
            DbAttributeConditionType.AttributeEquals =>
                Builders<BsonDocument>.Filter.Eq(condition.AttributeName, value),
            DbAttributeConditionType.AttributeNotEquals =>
                Builders<BsonDocument>.Filter.Ne(condition.AttributeName, value),
            DbAttributeConditionType.AttributeGreater =>
                Builders<BsonDocument>.Filter.Gt(condition.AttributeName, value),
            DbAttributeConditionType.AttributeGreaterOrEqual =>
                Builders<BsonDocument>.Filter.Gte(condition.AttributeName, value),
            DbAttributeConditionType.AttributeLess =>
                Builders<BsonDocument>.Filter.Lt(condition.AttributeName, value),
            DbAttributeConditionType.AttributeLessOrEqual =>
                Builders<BsonDocument>.Filter.Lte(condition.AttributeName, value),
            _ => null
        };
    }

    private static FilterDefinition<BsonDocument>? BuildArrayElementFilter(DbArrayElementCondition condition)
    {
        object[] elementValue = condition.ElementValue.Kind switch
        {
            PrimitiveTypeKind.String => [condition.ElementValue.AsString],
            PrimitiveTypeKind.Integer => [condition.ElementValue.AsInteger],
            PrimitiveTypeKind.Double => [condition.ElementValue.AsDouble],
            PrimitiveTypeKind.ByteArray => [condition.ElementValue.AsByteArray],
            _ => [condition.ElementValue.ToString()]
        };

        return condition.ConditionType switch
        {
            DbAttributeConditionType.ArrayElementExists =>
                Builders<BsonDocument>.Filter.AnyIn(condition.AttributeName, elementValue),
            DbAttributeConditionType.ArrayElementNotExists =>
                Builders<BsonDocument>.Filter.AnyNin(condition.AttributeName, elementValue),
            _ => null
        };
    }

    private static JObject? BsonToJObject(BsonDocument document)
    {
        try
        {
            // Remove database id as it is not part of what we store
            document.Remove("_id");

            // Use relaxed mode to convert numbers to natural JSON format instead of extended format
            var jsonString = document.ToJson(new JsonWriterSettings { OutputMode = JsonOutputMode.RelaxedExtendedJson });
            return JObject.Parse(jsonString);
        }
        catch
        {
            return null;
        }
    }

    private static BsonDocument JObjectToBson(JObject jsonObject)
    {
        // Direct conversion using BSON serialization
        var jsonString = jsonObject.ToString();
        return BsonDocument.Parse(jsonString);
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

    #endregion

    public void Dispose()
    {
        _mongoClient?.Dispose();
        // ReSharper disable once GCSuppressFinalizeForTypeWithoutDestructor
        GC.SuppressFinalize(this);
    }
}
