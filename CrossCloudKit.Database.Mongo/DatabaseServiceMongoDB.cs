// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

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

public sealed class DatabaseServiceMongoDB : DatabaseServiceBase, IDatabaseService, IDisposable
{
    /// <summary>
    /// Holds initialization success
    /// </summary>
    private readonly bool _bInitializationSucceed;

    /// <summary>
    /// Gets a value indicating whether the database service has been successfully initialized.
    /// </summary>
    // ReSharper disable once ConvertToAutoProperty
    public bool IsInitialized => _bInitializationSucceed;

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
    public DatabaseServiceMongoDB(
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
    public DatabaseServiceMongoDB(
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
    public DatabaseServiceMongoDB(
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

    private bool TableExists(string tableName)
    {
        if (_mongoDB == null) return false;

        var filter = new BsonDocument("name", tableName);
        var options = new ListCollectionNamesOptions { Filter = filter };

        return _mongoDB.ListCollectionNames(options).Any();
    }

    /// <summary>
    /// If given table (collection) does not exist in the database, it will create a new table (collection)
    /// </summary>
    private async Task<bool> TryCreateTableAsync(string tableName, CancellationToken cancellationToken = default)
    {
        try
        {
            if (_mongoDB == null) return false;

            if (!TableExists(tableName))
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
        PrimitiveTypeKind.Integer => Builders<BsonDocument>.Filter.Eq(keyName, keyValue.AsInteger),
        PrimitiveTypeKind.ByteArray => Builders<BsonDocument>.Filter.Eq(keyName, Convert.ToBase64String(keyValue.AsByteArray)), // Convert to Base64 to match JSON storage
        PrimitiveTypeKind.String => Builders<BsonDocument>.Filter.Eq(keyName, keyValue.AsString),
        _ => Builders<BsonDocument>.Filter.Eq(keyName, keyValue.ToString())
    };

    #region Modern Async API

    /// <inheritdoc />
    public async Task<OperationResult<bool>> ItemExistsAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        DbAttributeCondition? condition = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<bool>.Failure("Failed to get table", HttpStatusCode.InternalServerError);
            }

            var filter = BuildEqFilter(keyName, keyValue);

            if (condition != null)
            {
                var conditionFilter = BuildFilterFromCondition(condition);
                if (conditionFilter != null)
                {
                    filter = Builders<BsonDocument>.Filter.And(filter, conditionFilter);
                }
            }

            var document = await FindOneAsync(table, filter, cancellationToken);
            return OperationResult<bool>.Success(document != null);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure($"DatabaseServiceMongoDB->ItemExistsAsync: {ex.Message}", HttpStatusCode.InternalServerError);
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
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<JObject?>.Failure("Failed to get table", HttpStatusCode.InternalServerError);
            }

            var filter = BuildEqFilter(keyName, keyValue);
            var document = await FindOneAsync(table, filter, cancellationToken);

            if (document == null)
            {
                return OperationResult<JObject?>.Success(null);
            }

            var result = BsonToJObject(document);
            if (result != null)
            {
                AddKeyToJson(result, keyName, keyValue);
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
    public async Task<OperationResult<IReadOnlyList<JObject>>> GetItemsAsync(
        string tableName,
        string keyName,
        PrimitiveType[] keyValues,
        string[]? attributesToRetrieve = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (keyValues.Length == 0)
            {
                return OperationResult<IReadOnlyList<JObject>>.Success([]);
            }

            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<IReadOnlyList<JObject>>.Failure("Failed to get table", HttpStatusCode.InternalServerError);
            }

            var filter = keyValues.Aggregate(
                Builders<BsonDocument>.Filter.Empty,
                (current, value) => current == Builders<BsonDocument>.Filter.Empty
                    ? BuildEqFilter(keyName, value)
                    : Builders<BsonDocument>.Filter.Or(current, BuildEqFilter(keyName, value)));

            var documents = await table.Find(filter).ToListAsync(cancellationToken);
            var results = new List<JObject>();

            foreach (var document in documents)
            {
                var createdJson = BsonToJObject(document);
                if (createdJson != null)
                {
                    if (document.TryGetElement(keyName, out var valueElement))
                    {
                        AddKeyToJson(createdJson, keyName, BsonValueToPrimitiveType(valueElement.Value));
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
    public async Task<OperationResult<JObject?>> PutItemAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        JObject item,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
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
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        DbAttributeCondition? condition = null,
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
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        DbAttributeCondition? condition = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<JObject?>.Failure("Failed to get table", HttpStatusCode.InternalServerError);
            }

            var filter = BuildEqFilter(keyName, keyValue);

            if (condition != null)
            {
                var conditionFilter = BuildFilterFromCondition(condition);
                if (conditionFilter != null)
                {
                    filter = Builders<BsonDocument>.Filter.And(filter, conditionFilter);

                    if (!await HasTableMatchingResultWithFilterAsync(table, filter, cancellationToken))
                    {
                        return OperationResult<JObject?>.Failure("Condition not satisfied", HttpStatusCode.PreconditionFailed);
                    }
                }
            }

            JObject? returnItem = null;
            if (returnBehavior == DbReturnItemBehavior.ReturnOldValues)
            {
                var document = await FindOneAsync(table, BuildEqFilter(keyName, keyValue), cancellationToken);
                if (document != null)
                {
                    returnItem = BsonToJObject(document);
                    if (returnItem != null)
                    {
                        AddKeyToJson(returnItem, keyName, keyValue);
                        ApplyOptions(returnItem);
                    }
                }
            }

            await table.DeleteOneAsync(BuildEqFilter(keyName, keyValue), cancellationToken);
            return OperationResult<JObject?>.Success(returnItem);
        }
        catch (Exception ex)
        {
            return OperationResult<JObject?>.Failure($"DatabaseServiceMongoDB->DeleteItemAsync: {ex.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<JObject?>> AddElementsToArrayAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        string arrayAttributeName,
        PrimitiveType[] elementsToAdd,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        DbAttributeCondition? condition = null,
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

            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<JObject?>.Failure("Failed to get table", HttpStatusCode.InternalServerError);
            }

            var filter = BuildEqFilter(keyName, keyValue);

            if (condition != null)
            {
                var conditionFilter = BuildFilterFromCondition(condition);
                if (conditionFilter != null)
                {
                    var finalFilter = Builders<BsonDocument>.Filter.And(filter, conditionFilter);
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
                        AddKeyToJson(returnItem, keyName, keyValue);
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
            await table.UpdateOneAsync(filter, update, new UpdateOptions { IsUpsert = true }, cancellationToken);

            if (returnBehavior == DbReturnItemBehavior.ReturnNewValues)
            {
                var document = await FindOneAsync(table, filter, cancellationToken);
                if (document != null)
                {
                    returnItem = BsonToJObject(document);
                    if (returnItem != null)
                    {
                        AddKeyToJson(returnItem, keyName, keyValue);
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

    /// <inheritdoc />
    public async Task<OperationResult<JObject?>> RemoveElementsFromArrayAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        string arrayAttributeName,
        PrimitiveType[] elementsToRemove,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        DbAttributeCondition? condition = null,
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

            var filter = BuildEqFilter(keyName, keyValue);

            if (condition != null)
            {
                var conditionFilter = BuildFilterFromCondition(condition);
                if (conditionFilter != null)
                {
                    var finalFilter = Builders<BsonDocument>.Filter.And(filter, conditionFilter);
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
                        AddKeyToJson(returnItem, keyName, keyValue);
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
                        AddKeyToJson(returnItem, keyName, keyValue);
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
    public async Task<OperationResult<double>> IncrementAttributeAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        string numericAttributeName,
        double incrementValue,
        DbAttributeCondition? condition = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<double>.Failure("Failed to get table", HttpStatusCode.InternalServerError);
            }

            var filter = BuildEqFilter(keyName, keyValue);

            if (condition != null)
            {
                var conditionFilter = BuildFilterFromCondition(condition);
                if (conditionFilter != null)
                {
                    var finalFilter = Builders<BsonDocument>.Filter.And(filter, conditionFilter);
                    if (!await HasTableMatchingResultWithFilterAsync(table, finalFilter, cancellationToken))
                    {
                        return OperationResult<double>.Failure("Condition not satisfied", HttpStatusCode.PreconditionFailed);
                    }
                }
            }

            var update = Builders<BsonDocument>.Update.Inc(numericAttributeName, incrementValue);
            var options = new FindOneAndUpdateOptions<BsonDocument>
            {
                ReturnDocument = ReturnDocument.After,
                IsUpsert = true
            };

            var document = await table.FindOneAndUpdateAsync(filter, update, options, cancellationToken);

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
    public async Task<OperationResult<IReadOnlyList<JObject>>> ScanTableAsync(
        string tableName,
        string[] keyNames,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<IReadOnlyList<JObject>>.Failure("Failed to get table", HttpStatusCode.InternalServerError);
            }

            var filter = Builders<BsonDocument>.Filter.Empty;
            var documents = await table.Find(filter).ToListAsync(cancellationToken);

            return ProcessScanResults(keyNames, documents);
        }
        catch (Exception e)
        {
            return OperationResult<IReadOnlyList<JObject>>.Failure($"DatabaseServiceMongoDB->ScanTableAsync: {e.Message}", HttpStatusCode.InternalServerError);
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
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<(IReadOnlyList<JObject>, string?, long?)>.Failure("Failed to get table", HttpStatusCode.InternalServerError);
            }

            var filter = Builders<BsonDocument>.Filter.Empty;
            var totalCount = await table.CountDocumentsAsync(filter, cancellationToken: cancellationToken);

            var skip = 0;
            if (int.TryParse(pageToken, out var pageTokenInt))
            {
                skip = pageTokenInt;
            }

            var documents = await table.Find(filter)
                .Skip(skip)
                .Limit(pageSize)
                .ToListAsync(cancellationToken);

            var scanResult = ProcessScanResults(keyNames, documents);
            if (!scanResult.IsSuccessful)
            {
                return OperationResult<(IReadOnlyList<JObject>, string?, long?)>.Failure(scanResult.ErrorMessage, scanResult.StatusCode);
            }

            var nextPageToken = skip + pageSize < totalCount ? (skip + pageSize).ToString() : null;
            return OperationResult<(IReadOnlyList<JObject>, string?, long?)>.Success(
                (scanResult.Data, nextPageToken, totalCount));
        }
        catch (Exception e)
        {
            return OperationResult<(IReadOnlyList<JObject>, string?, long?)>.Failure(
                $"DatabaseServiceMongoDB->ScanTablePaginatedAsync: {e.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<IReadOnlyList<JObject>>> ScanTableWithFilterAsync(
        string tableName,
        string[] keyNames,
        DbAttributeCondition filterCondition,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<IReadOnlyList<JObject>>.Failure("Failed to get table", HttpStatusCode.InternalServerError);
            }

            var filter = BuildFilterFromCondition(filterCondition) ?? Builders<BsonDocument>.Filter.Empty;
            var documents = await table.Find(filter).ToListAsync(cancellationToken);

            return ProcessScanResults(keyNames, documents);
        }
        catch (Exception e)
        {
            return OperationResult<IReadOnlyList<JObject>>.Failure(
                $"DatabaseServiceMongoDB->ScanTableWithFilterAsync: {e.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<(IReadOnlyList<JObject> Items, string? NextPageToken, long? TotalCount)>> ScanTableWithFilterPaginatedAsync(
        string tableName,
        string[] keyNames,
        DbAttributeCondition filterCondition,
        int pageSize,
        string? pageToken = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<(IReadOnlyList<JObject>, string?, long?)>.Failure("Failed to get table", HttpStatusCode.InternalServerError);
            }

            var filter = BuildFilterFromCondition(filterCondition) ?? Builders<BsonDocument>.Filter.Empty;
            var totalCount = await table.CountDocumentsAsync(filter, cancellationToken: cancellationToken);

            var skip = 0;
            if (int.TryParse(pageToken, out var pageTokenInt))
            {
                skip = pageTokenInt;
            }

            var documents = await table.Find(filter)
                .Skip(skip)
                .Limit(pageSize)
                .ToListAsync(cancellationToken);

            var scanResult = ProcessScanResults(keyNames, documents);
            if (!scanResult.IsSuccessful)
            {
                return OperationResult<(IReadOnlyList<JObject>, string?, long?)>.Failure(scanResult.ErrorMessage, scanResult.StatusCode);
            }

            var nextPageToken = skip + pageSize < totalCount ? (skip + pageSize).ToString() : null;
            return OperationResult<(IReadOnlyList<JObject>, string?, long?)>.Success(
                (scanResult.Data, nextPageToken, totalCount));
        }
        catch (Exception e)
        {
            return OperationResult<(IReadOnlyList<JObject>, string?, long?)>.Failure(
                $"DatabaseServiceMongoDB->ScanTableWithFilterPaginatedAsync: {e.Message}", HttpStatusCode.InternalServerError);
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
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        DbAttributeCondition? conditionExpression = null,
        bool shouldOverrideIfExists = false,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<JObject?>.Failure("Failed to get table", HttpStatusCode.InternalServerError);
            }

            var filter = BuildEqFilter(keyName, keyValue);

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
                if (conditionExpression != null)
                {
                    var conditionFilter = BuildFilterFromCondition(conditionExpression);
                    if (conditionFilter != null)
                    {
                        var finalFilter = Builders<BsonDocument>.Filter.And(filter, conditionFilter);
                        if (!await HasTableMatchingResultWithFilterAsync(table, finalFilter, cancellationToken))
                        {
                            return OperationResult<JObject?>.Failure("Condition not satisfied", HttpStatusCode.PreconditionFailed);
                        }
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
                        AddKeyToJson(returnItem, keyName, keyValue);
                        ApplyOptions(returnItem);
                    }
                }
            }

            var newObject = new JObject(newItem);
            AddKeyToJson(newObject, keyName, keyValue);

            // Use $set for preventing element name validation exceptions
            var updateDocument = new BsonDocument { { "$set", JObjectToBson(newObject) } };
            await table.UpdateOneAsync(filter, updateDocument, new UpdateOptions { IsUpsert = true }, cancellationToken);

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

    private OperationResult<IReadOnlyList<JObject>> ProcessScanResults(string[] possibleKeyNames, List<BsonDocument> documents)
    {
        try
        {
            var tempResults = new ConcurrentBag<JObject>();

            Parallel.ForEach(documents, document =>
            {
                var createdJson = BsonToJObject(document);
                if (createdJson != null)
                {
                    foreach (var keyName in possibleKeyNames)
                    {
                        if (document.TryGetElement(keyName, out var value))
                        {
                            AddKeyToJson(createdJson, keyName, BsonValueToPrimitiveType(value.Value));
                            break;
                        }
                    }

                    ApplyOptions(createdJson);
                    tempResults.Add(createdJson);
                }
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

    #region Override AddKeyToJson for MongoDB-specific key handling

    /// <summary>
    /// Override AddKeyToJson to handle MongoDB-specific key formats
    /// For MongoDB, we need to ensure the key format in JSON matches how it was stored in BSON
    /// </summary>
    private new static void AddKeyToJson(JObject destination, string keyName, PrimitiveType keyValue)
    {
        // For MongoDB, we need to be consistent with how BSON stores and retrieves primitive types
        // Use the same conversion logic as BsonValueToPrimitiveType to ensure consistency
        destination[keyName] = keyValue.Kind switch
        {
            PrimitiveTypeKind.String => keyValue.AsString,
            PrimitiveTypeKind.Integer => keyValue.AsInteger,
            PrimitiveTypeKind.Double => keyValue.AsDouble,
            PrimitiveTypeKind.ByteArray => Convert.ToBase64String(keyValue.AsByteArray),// For byte arrays, we need to match how MongoDB BSON handles them
                                                                                                // MongoDB stores byte arrays as binary data, which JSON.NET can't represent directly
                                                                                                // So we convert to Base64 string for JSON representation
            _ => keyValue.ToString(),
        };
    }

    #endregion

    #region Condition Builders

    public DbAttributeCondition BuildAttributeExistsCondition(string attributeName) =>
        new DbExistenceCondition(DbAttributeConditionType.AttributeExists, attributeName);
    public DbAttributeCondition BuildAttributeNotExistsCondition(string attributeName) =>
        new DbExistenceCondition(DbAttributeConditionType.AttributeNotExists, attributeName);
    public DbAttributeCondition BuildAttributeEqualsCondition(string attributeName, PrimitiveType value) =>
        new DbValueCondition(DbAttributeConditionType.AttributeEquals, attributeName, value);
    public DbAttributeCondition BuildAttributeNotEqualsCondition(string attributeName, PrimitiveType value) =>
        new DbValueCondition(DbAttributeConditionType.AttributeNotEquals, attributeName, value);
    public DbAttributeCondition BuildAttributeGreaterCondition(string attributeName, PrimitiveType value) =>
        new DbValueCondition(DbAttributeConditionType.AttributeGreater, attributeName, value);
    public DbAttributeCondition BuildAttributeGreaterOrEqualCondition(string attributeName, PrimitiveType value) =>
        new DbValueCondition(DbAttributeConditionType.AttributeGreaterOrEqual, attributeName, value);
    public DbAttributeCondition BuildAttributeLessCondition(string attributeName, PrimitiveType value) =>
        new DbValueCondition(DbAttributeConditionType.AttributeLess, attributeName, value);
    public DbAttributeCondition BuildAttributeLessOrEqualCondition(string attributeName, PrimitiveType value) =>
        new DbValueCondition(DbAttributeConditionType.AttributeLessOrEqual, attributeName, value);
    public DbAttributeCondition BuildArrayElementExistsCondition(string attributeName, PrimitiveType elementValue) =>
        new DbArrayElementCondition(DbAttributeConditionType.ArrayElementExists, attributeName, elementValue);
    public DbAttributeCondition BuildArrayElementNotExistsCondition(string attributeName, PrimitiveType elementValue) =>
        new DbArrayElementCondition(DbAttributeConditionType.ArrayElementNotExists, attributeName, elementValue);

    #endregion

    public void Dispose()
    {
        _mongoClient?.Dispose();
        // ReSharper disable once GCSuppressFinalizeForTypeWithoutDestructor
        GC.SuppressFinalize(this);
    }
}
