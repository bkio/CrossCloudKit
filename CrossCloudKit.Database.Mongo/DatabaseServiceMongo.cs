// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

//
// Note: It's essential that methods defined in IDatabaseService are not called directly from ...CoreAsync methods.
// Instead, call CoreAsync methods when needed.
//

using System.Collections.Concurrent;
using System.Net;
using System.Text;
using CrossCloudKit.Interfaces;
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
    /// <param name="memoryService">This memory service will be used to ensure global atomicity on functions</param>
    /// <param name="errorMessageAction">Error messages will be pushed to this action</param>
    public DatabaseServiceMongo(
        string mongoHost,
        int mongoPort,
        string mongoDatabase,
        IMemoryService memoryService,
        Action<string>? errorMessageAction = null) : base(memoryService)
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
    /// <param name="memoryService">This memory service will be used to ensure global atomicity on functions</param>
    /// <param name="errorMessageAction">Error messages will be pushed to this action</param>
    public DatabaseServiceMongo(
        string connectionString,
        string mongoDatabase,
        IMemoryService memoryService,
        Action<string>? errorMessageAction = null) : base(memoryService)
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
    /// DatabaseServiceMongoDB: Advanced constructor for MongoDB initialization using a client configuration JSON.
    /// </summary>
    /// <param name="mongoClientConfigJson">
    /// JSON configuration string for MongoDB client.
    /// Can be provided either directly as JSON or as a base64-encoded JSON (commonly used in local environments via <c>launchSettings.json</c>).
    /// Example structure may include hostnames, ports, replica set info, authentication mechanism, and user credentials.
    /// </param>
    /// <param name="mongoPassword">
    /// Password string used for authentication.
    /// Applied together with <c>auth.usersWanted</c> configuration values (database, username, and auth mechanism) if present in the JSON.
    /// </param>
    /// <param name="mongoDatabase">
    /// The target MongoDB database name to connect to.
    /// If the provided JSON contains different database values, this parameter takes precedence for selecting the working database.
    /// </param>
    /// <param name="memoryService">
    /// An implementation of <see cref="IMemoryService"/> used to ensure global atomicity across database operations.
    /// Prevents race conditions when multiple functions attempt to access or modify the same data.
    /// </param>
    /// <param name="errorMessageAction">
    /// Optional delegate for error handling.
    /// If provided, exceptions encountered during initialization (e.g., invalid JSON, network issues, authentication errors) are passed to this callback.
    /// If <c>null</c>, errors are silently handled by setting the initialization state to failed.
    /// </param>
    /// <remarks>
    /// <para>
    /// This constructor is intended for scenarios where MongoDB configuration is provided in a structured JSON format,
    /// often used in enterprise setups with replica sets, multiple hosts, or authentication requirements.
    /// </para>
    /// <para>
    /// Authentication:
    /// If the configuration JSON contains valid values for <c>db</c>, <c>user</c>, and <c>autoAuthMechanism</c>,
    /// a <see cref="MongoCredential"/> is created using <paramref name="mongoPassword"/>.
    /// Otherwise, the client is initialized without explicit credentials.
    /// </para>
    /// <para>
    /// Ports:
    /// Each host can specify its own port via <c>port</c>. If no port is specified, the default MongoDB port (27017) is used.
    /// </para>
    /// <para>
    /// Errors:
    /// Any errors during JSON parsing, credential building, or client creation will result in <c>_bInitializationSucceed</c> being set to <c>false</c>.
    /// </para>
    /// </remarks>
    public DatabaseServiceMongo(
        string mongoClientConfigJson,
        string mongoPassword,
        string mongoDatabase,
        IMemoryService memoryService,
        Action<string>? errorMessageAction = null) : base(memoryService)
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

    private Task<bool> TableExists(string tableName)
    {
        if (_mongoDB == null) return Task.FromResult(false);

        var filter = new BsonDocument("name", tableName);
        var options = new ListCollectionNamesOptions { Filter = filter };

        return Task.FromResult(_mongoDB.ListCollectionNames(options).Any());
    }

    /// <summary>
    /// If given table (collection) does not exist in the database, it will create a new table (collection)
    /// </summary>
    private async Task<bool> TryCreateTableAsync(string tableName, CancellationToken cancellationToken = default)
    {
        try
        {
            if (_mongoDB == null) return false;

            if (!await TableExists(tableName))
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
        ConditionCoupling? conditions = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<bool>.Failure("Failed to get table", HttpStatusCode.InternalServerError);
            }

            return await ExistenceAndConditionMatchCheckAsync(table, BuildEqFilter(key.Name, key.Value), conditions, cancellationToken);
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
        ConditionCoupling? conditions = null,
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
        ConditionCoupling? conditions = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<JObject?>.Failure("Failed to get table", HttpStatusCode.InternalServerError);
            }

            var existenceAndConditionCheckResult = await ExistenceAndConditionMatchCheckAsync(
                table,
                BuildEqFilter(key.Name, key.Value),
                conditions,
                cancellationToken);
            if (!existenceAndConditionCheckResult.IsSuccessful)
            {
                return existenceAndConditionCheckResult.StatusCode == HttpStatusCode.NotFound
                    ? OperationResult<JObject?>.Success(null)
                    : OperationResult<JObject?>.Failure(existenceAndConditionCheckResult.ErrorMessage, existenceAndConditionCheckResult.StatusCode);
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

            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<JObject?>.Failure("Failed to get table", HttpStatusCode.InternalServerError);
            }

            var filter = BuildEqFilter(key.Name, key.Value);

            var existenceAndConditionCheckResult = await ExistenceAndConditionMatchCheckAsync(table, filter, conditions, cancellationToken);
            if (!existenceAndConditionCheckResult.IsSuccessful && existenceAndConditionCheckResult.StatusCode != HttpStatusCode.NotFound)
                return OperationResult<JObject?>.Failure(existenceAndConditionCheckResult.ErrorMessage, existenceAndConditionCheckResult.StatusCode);

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
                returnItem ??= new JObject();
            }

            var elementsToAddList = elementsToAdd.Select(element => element.Kind switch
            {
                PrimitiveTypeKind.String => (object)element.AsString,
                PrimitiveTypeKind.Integer => element.AsInteger,
                PrimitiveTypeKind.Boolean => element.AsBoolean,
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
        ConditionCoupling conditions,
        FilterDefinition<BsonDocument>? baseFilter = null)
    {
        var conditionFilter = BuildConditionCouplingFilter(conditions);
        if (conditionFilter == null)
        {
            finalFilter = baseFilter ?? Builders<BsonDocument>.Filter.Empty;
            return false;
        }

        finalFilter = baseFilter != null
            ? Builders<BsonDocument>.Filter.And(baseFilter, conditionFilter)
            : conditionFilter;

        return true;
    }

    private static FilterDefinition<BsonDocument>? BuildConditionCouplingFilter(ConditionCoupling conditions)
    {
        return conditions.CouplingType switch
        {
            ConditionCouplingType.Empty => null,
            ConditionCouplingType.Single when conditions.SingleCondition != null =>
                BuildFilterFromCondition(conditions.SingleCondition),
            ConditionCouplingType.And when conditions is { First: not null, Second: not null } =>
                BuildAndFilter(conditions.First, conditions.Second),
            ConditionCouplingType.Or when conditions is { First: not null, Second: not null } =>
                BuildOrFilter(conditions.First, conditions.Second),
            _ => null
        };
    }

    private static FilterDefinition<BsonDocument>? BuildAndFilter(ConditionCoupling first, ConditionCoupling second)
    {
        var firstFilter = BuildConditionCouplingFilter(first);
        var secondFilter = BuildConditionCouplingFilter(second);

        if (firstFilter == null && secondFilter == null)
            return null;
        if (firstFilter == null)
            return secondFilter;
        if (secondFilter == null)
            return firstFilter;

        return Builders<BsonDocument>.Filter.And(firstFilter, secondFilter);
    }

    private static FilterDefinition<BsonDocument>? BuildOrFilter(ConditionCoupling first, ConditionCoupling second)
    {
        var firstFilter = BuildConditionCouplingFilter(first);
        var secondFilter = BuildConditionCouplingFilter(second);

        if (firstFilter == null && secondFilter == null)
            return null;
        if (firstFilter == null)
            return secondFilter;
        if (secondFilter == null)
            return firstFilter;

        return Builders<BsonDocument>.Filter.Or(firstFilter, secondFilter);
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<JObject?>> RemoveElementsFromArrayCoreAsync(
        string tableName,
        DbKey key,
        string arrayAttributeName,
        PrimitiveType[] elementsToRemove,
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

            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<JObject?>.Failure("Failed to get table", HttpStatusCode.InternalServerError);
            }

            var filter = BuildEqFilter(key.Name, key.Value);

            var existenceAndConditionCheckResult = await ExistenceAndConditionMatchCheckAsync(table, filter, conditions, cancellationToken);
            if (!existenceAndConditionCheckResult.IsSuccessful)
                return existenceAndConditionCheckResult.StatusCode == HttpStatusCode.NotFound
                        ? OperationResult<JObject?>.Success(null)
                        : OperationResult<JObject?>.Failure(existenceAndConditionCheckResult.ErrorMessage, existenceAndConditionCheckResult.StatusCode);

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
                PrimitiveTypeKind.Boolean => element.AsBoolean,
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

            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<double>.Failure("Failed to get table", HttpStatusCode.InternalServerError);
            }

            var filter = BuildEqFilter(key.Name, key.Value);

            var existenceAndConditionCheckResult = await ExistenceAndConditionMatchCheckAsync(table, filter, conditions, cancellationToken);
            if (!existenceAndConditionCheckResult.IsSuccessful && existenceAndConditionCheckResult.StatusCode != HttpStatusCode.NotFound)
                return OperationResult<double>.Failure(existenceAndConditionCheckResult.ErrorMessage, existenceAndConditionCheckResult.StatusCode);

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
            var getKeysTask = GetTableKeysCoreAsync(tableName, cancellationToken);

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

            var getKeysTask = GetTableKeysCoreAsync(tableName, cancellationToken);

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
        ConditionCoupling filterConditions,
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

            var getKeysTask = GetTableKeysCoreAsync(tableName, cancellationToken);
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
        ConditionCoupling filterConditions,
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

            var getKeysTask = GetTableKeysCoreAsync(tableName, cancellationToken);

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

            tableNames = tableNames.Where(t => !t.StartsWith(SystemTableNamePrefix)).ToList();

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
            if (!await TableExists(tableName))
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
                    if (await CheckForExistenceWithFilter(table, filter, cancellationToken))
                    {
                        return OperationResult<JObject?>.Failure("Item already exists", HttpStatusCode.Conflict);
                    }
                }
            }
            else // UpdateItem
            {
                var existenceAndConditionCheckResult = await ExistenceAndConditionMatchCheckAsync(table, filter, conditions, cancellationToken);
                if (!existenceAndConditionCheckResult.IsSuccessful)
                    return OperationResult<JObject?>.Failure(existenceAndConditionCheckResult.ErrorMessage, existenceAndConditionCheckResult.StatusCode);
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

    private static async Task<OperationResult<bool>> ExistenceAndConditionMatchCheckAsync(
        IMongoCollection<BsonDocument> table,
        FilterDefinition<BsonDocument> filter,
        ConditionCoupling? conditions = null,
        CancellationToken cancellationToken = default)
    {
        if (conditions == null || !BuildConditionsFilter(out var finalFilter, conditions, filter))
        {
            return await CheckForExistenceWithFilter(table, filter, cancellationToken)
                ? OperationResult<bool>.Success(true)
                : OperationResult<bool>.Failure("Not found.", HttpStatusCode.NotFound);
        }

        var existsTask = CheckForExistenceWithFilter(table, filter, cancellationToken);
        var conditionCheckTask = CheckForExistenceWithFilter(table, finalFilter, cancellationToken);
        await Task.WhenAll(existsTask, conditionCheckTask);

        return !await existsTask
            ? OperationResult<bool>.Failure("Not found.", HttpStatusCode.NotFound)
            : !await conditionCheckTask
                ? OperationResult<bool>.Failure("Condition not satisfied.", HttpStatusCode.PreconditionFailed)
                : OperationResult<bool>.Success(true);
    }

    private static async Task<bool> CheckForExistenceWithFilter(
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

    private static FilterDefinition<BsonDocument>? BuildFilterFromCondition(Condition condition)
    {
        return condition.ConditionType switch
        {
            ConditionType.AttributeExists =>
                Builders<BsonDocument>.Filter.Exists(condition.AttributeName),
            ConditionType.AttributeNotExists =>
                Builders<BsonDocument>.Filter.Exists(condition.AttributeName, false),
            _ when condition is ValueCondition valueCondition => BuildValueFilter(valueCondition),
            _ when condition is ArrayCondition arrayCondition => BuildArrayElementFilter(arrayCondition),
            _ => null
        };
    }

    private static FilterDefinition<BsonDocument>? BuildValueFilter(ValueCondition condition)
    {
        var value = condition.Value.Kind switch
        {
            PrimitiveTypeKind.String => (object)condition.Value.AsString,
            PrimitiveTypeKind.Integer => condition.Value.AsInteger,
            PrimitiveTypeKind.Boolean => condition.Value.AsBoolean,
            PrimitiveTypeKind.Double => condition.Value.AsDouble,
            PrimitiveTypeKind.ByteArray => condition.Value.AsByteArray,
            _ => condition.Value.ToString()
        };

        return condition.ConditionType switch
        {
            ConditionType.AttributeEquals =>
                Builders<BsonDocument>.Filter.Eq(condition.AttributeName, value),
            ConditionType.AttributeNotEquals =>
                Builders<BsonDocument>.Filter.Ne(condition.AttributeName, value),
            ConditionType.AttributeGreater =>
                Builders<BsonDocument>.Filter.Gt(condition.AttributeName, value),
            ConditionType.AttributeGreaterOrEqual =>
                Builders<BsonDocument>.Filter.Gte(condition.AttributeName, value),
            ConditionType.AttributeLess =>
                Builders<BsonDocument>.Filter.Lt(condition.AttributeName, value),
            ConditionType.AttributeLessOrEqual =>
                Builders<BsonDocument>.Filter.Lte(condition.AttributeName, value),
            _ => null
        };
    }

    private static FilterDefinition<BsonDocument>? BuildArrayElementFilter(ArrayCondition condition)
    {
        object[] elementValue = condition.ElementValue.Kind switch
        {
            PrimitiveTypeKind.String => [condition.ElementValue.AsString],
            PrimitiveTypeKind.Integer => [condition.ElementValue.AsInteger],
            PrimitiveTypeKind.Boolean => [condition.ElementValue.AsBoolean],
            PrimitiveTypeKind.Double => [condition.ElementValue.AsDouble],
            PrimitiveTypeKind.ByteArray => [condition.ElementValue.AsByteArray],
            _ => [condition.ElementValue.ToString()]
        };

        return condition.ConditionType switch
        {
            ConditionType.ArrayElementExists =>
                Builders<BsonDocument>.Filter.AnyIn(condition.AttributeName, elementValue),
            ConditionType.ArrayElementNotExists =>
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
    public override Condition AttributeExists(string attributeName) =>
        new ExistenceCondition(ConditionType.AttributeExists, attributeName);

    /// <inheritdoc />
    public override Condition AttributeNotExists(string attributeName) =>
        new ExistenceCondition(ConditionType.AttributeNotExists, attributeName);

    /// <inheritdoc />
    public override Condition AttributeEquals(string attributeName, PrimitiveType value) =>
        new ValueCondition(ConditionType.AttributeEquals, attributeName, value);

    /// <inheritdoc />
    public override Condition AttributeNotEquals(string attributeName, PrimitiveType value) =>
        new ValueCondition(ConditionType.AttributeNotEquals, attributeName, value);

    /// <inheritdoc />
    public override Condition AttributeIsGreaterThan(string attributeName, PrimitiveType value) =>
        new ValueCondition(ConditionType.AttributeGreater, attributeName, value);

    /// <inheritdoc />
    public override Condition AttributeIsGreaterOrEqual(string attributeName, PrimitiveType value) =>
        new ValueCondition(ConditionType.AttributeGreaterOrEqual, attributeName, value);

    /// <inheritdoc />
    public override Condition AttributeIsLessThan(string attributeName, PrimitiveType value) =>
        new ValueCondition(ConditionType.AttributeLess, attributeName, value);

    /// <inheritdoc />
    public override Condition AttributeIsLessOrEqual(string attributeName, PrimitiveType value) =>
        new ValueCondition(ConditionType.AttributeLessOrEqual, attributeName, value);

    /// <inheritdoc />
    public override Condition ArrayElementExists(string attributeName, PrimitiveType elementValue) =>
        new ArrayCondition(ConditionType.ArrayElementExists, attributeName, elementValue);

    /// <inheritdoc />
    public override Condition ArrayElementNotExists(string attributeName, PrimitiveType elementValue) =>
        new ArrayCondition(ConditionType.ArrayElementNotExists, attributeName, elementValue);

    #endregion

    public void Dispose()
    {
        _mongoClient?.Dispose();
        // ReSharper disable once GCSuppressFinalizeForTypeWithoutDestructor
        GC.SuppressFinalize(this);
    }
}
