// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Text;
using Cloud.Interfaces;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Driver;
using Utilities.Common;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

namespace Cloud.Database.Mongo;

public class DatabaseServiceMongoDB : DatabaseServiceBase, IDatabaseService, IDisposable
{
    /// <summary>
    /// Holds initialization success
    /// </summary>
    private readonly bool bInitializationSucceed;

    /// <summary>
    /// Gets a value indicating whether the database service has been successfully initialized.
    /// </summary>
    public bool IsInitialized => bInitializationSucceed;

    private readonly IMongoDatabase? MongoDB;
    private readonly MongoClient? _mongoClient;

    private readonly Dictionary<string, IMongoCollection<BsonDocument>> TableMap = [];
    private readonly Lock TableMap_DictionaryLock = new();

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
            MongoDB = _mongoClient.GetDatabase(mongoDatabase);
            bInitializationSucceed = MongoDB != null;
        }
        catch (Exception e)
        {
            errorMessageAction?.Invoke($"DatabaseServiceMongoDB->Constructor: {e.Message} \n Trace: {e.StackTrace}");
            bInitializationSucceed = false;
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
            MongoDB = _mongoClient.GetDatabase(mongoDatabase);
            bInitializationSucceed = MongoDB != null;
        }
        catch (Exception e)
        {
            errorMessageAction?.Invoke($"DatabaseServiceMongoDB->Constructor: {e.Message} \n Trace: {e.StackTrace}");
            bInitializationSucceed = false;
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
            if (Convert.TryFromBase64String(clientConfigString, buffer, out int bytesParsed))
            {
                if (bytesParsed > 0)
                {
                    clientConfigString = Encoding.UTF8.GetString(buffer[..bytesParsed]);
                }
            }

            var clientConfigJObject = JObject.Parse(clientConfigString);

            var hostTokens = clientConfigJObject.SelectTokens("$...hostname");
            var hosts = hostTokens.Select(item => item.ToObject<string>()!).ToList();

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

            MongoDB = _mongoClient.GetDatabase(mongoDatabase);
            bInitializationSucceed = MongoDB != null;
        }
        catch (Exception e)
        {
            errorMessageAction?.Invoke($"DatabaseServiceMongoDB->Constructor: {e.Message} \n Trace: {e.StackTrace}");
            bInitializationSucceed = false;
        }
    }

    private bool TableExists(string tableName)
    {
        if (MongoDB == null) return false;

        var filter = new BsonDocument("name", tableName);
        var options = new ListCollectionNamesOptions { Filter = filter };

        return MongoDB.ListCollectionNames(options).Any();
    }

    /// <summary>
    /// If given table (collection) does not exist in the database, it will create a new table (collection)
    /// </summary>
    private async Task<bool> TryCreateTableAsync(string tableName, CancellationToken cancellationToken = default)
    {
        try
        {
            if (MongoDB == null) return false;
            
            if (!TableExists(tableName))
            {
                await MongoDB.CreateCollectionAsync(tableName, cancellationToken: cancellationToken);
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
        lock (TableMap_DictionaryLock)
        {
            if (TableMap.TryGetValue(tableName, out var existingTable))
            {
                return existingTable;
            }
        }

        if (!await TryCreateTableAsync(tableName, cancellationToken))
        {
            return null;
        }

        var tableObj = MongoDB?.GetCollection<BsonDocument>(tableName);
        if (tableObj != null)
        {
            lock (TableMap_DictionaryLock)
            {
                TableMap.TryAdd(tableName, tableObj);
            }
        }

        return tableObj;
    }

    private static FilterDefinition<BsonDocument> BuildEqFilter(string keyName, PrimitiveType keyValue) => keyValue.Kind switch
    {
        PrimitiveTypeKind.Double => Builders<BsonDocument>.Filter.Eq(keyName, keyValue.AsDouble),
        PrimitiveTypeKind.Integer => Builders<BsonDocument>.Filter.Eq(keyName, keyValue.AsInteger),
        PrimitiveTypeKind.ByteArray => Builders<BsonDocument>.Filter.Eq(keyName, keyValue.AsByteArray),
        PrimitiveTypeKind.String => Builders<BsonDocument>.Filter.Eq(keyName, keyValue.AsString),
        _ => Builders<BsonDocument>.Filter.Eq(keyName, keyValue.ToString())
    };

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
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return DatabaseResult<bool>.Failure("Failed to get table");
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
            return DatabaseResult<bool>.Success(document != null);
        }
        catch (Exception ex)
        {
            return DatabaseResult<bool>.Failure($"DatabaseServiceMongoDB->ItemExistsAsync: {ex.Message}");
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
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return DatabaseResult<JObject?>.Failure("Failed to get table");
            }

            var filter = BuildEqFilter(keyName, keyValue);
            var document = await FindOneAsync(table, filter, cancellationToken);

            if (document == null)
            {
                return DatabaseResult<JObject?>.Success(null);
            }

            var result = BsonToJObject(document);
            if (result != null)
            {
                AddKeyToJson(result, keyName, keyValue);
                ApplyOptions(result);
            }

            return DatabaseResult<JObject?>.Success(result);
        }
        catch (Exception ex)
        {
            return DatabaseResult<JObject?>.Failure($"DatabaseServiceMongoDB->GetItemAsync: {ex.Message}");
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
        try
        {
            if (keyValues.Length == 0)
            {
                return DatabaseResult<IReadOnlyList<JObject>>.Success([]);
            }

            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return DatabaseResult<IReadOnlyList<JObject>>.Failure("Failed to get table");
            }

            var filter = keyValues.Aggregate<PrimitiveType, FilterDefinition<BsonDocument>>(
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

            return DatabaseResult<IReadOnlyList<JObject>>.Success(results.AsReadOnly());
        }
        catch (Exception e)
        {
            return DatabaseResult<IReadOnlyList<JObject>>.Failure($"DatabaseServiceMongoDB->GetItemsAsync: {e.Message}");
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
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return DatabaseResult<JObject?>.Failure("Failed to get table");
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
                        return DatabaseResult<JObject?>.Failure("Condition not satisfied");
                    }
                }
            }

            JObject? returnItem = null;
            if (returnBehavior == ReturnItemBehavior.ReturnOldValues)
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
            return DatabaseResult<JObject?>.Success(returnItem);
        }
        catch (Exception ex)
        {
            return DatabaseResult<JObject?>.Failure($"DatabaseServiceMongoDB->DeleteItemAsync: {ex.Message}");
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
        try
        {
            if (elementsToAdd.Length == 0)
            {
                return DatabaseResult<JObject?>.Failure("ElementsToAdd must contain values.");
            }

            var expectedKind = elementsToAdd[0].Kind;
            if (elementsToAdd.Any(element => element.Kind != expectedKind))
            {
                return DatabaseResult<JObject?>.Failure("All elements must have the same type.");
            }

            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return DatabaseResult<JObject?>.Failure("Failed to get table");
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
                        return DatabaseResult<JObject?>.Failure("Condition not satisfied");
                    }
                }
            }

            JObject? returnItem = null;
            if (returnBehavior == ReturnItemBehavior.ReturnOldValues)
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

            if (returnBehavior == ReturnItemBehavior.ReturnNewValues)
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

            return DatabaseResult<JObject?>.Success(returnItem);
        }
        catch (Exception ex)
        {
            return DatabaseResult<JObject?>.Failure($"DatabaseServiceMongoDB->AddElementsToArrayAsync: {ex.Message}");
        }
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
        try
        {
            if (elementsToRemove.Length == 0)
            {
                return DatabaseResult<JObject?>.Failure("ElementsToRemove must contain values.");
            }

            var expectedKind = elementsToRemove[0].Kind;
            if (elementsToRemove.Any(element => element.Kind != expectedKind))
            {
                return DatabaseResult<JObject?>.Failure("All elements must have the same type.");
            }

            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return DatabaseResult<JObject?>.Failure("Failed to get table");
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
                        return DatabaseResult<JObject?>.Failure("Condition not satisfied");
                    }
                }
            }

            JObject? returnItem = null;
            if (returnBehavior == ReturnItemBehavior.ReturnOldValues)
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

            if (returnBehavior == ReturnItemBehavior.ReturnNewValues)
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

            return DatabaseResult<JObject?>.Success(returnItem);
        }
        catch (Exception ex)
        {
            return DatabaseResult<JObject?>.Failure($"DatabaseServiceMongoDB->RemoveElementsFromArrayAsync: {ex.Message}");
        }
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
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return DatabaseResult<double>.Failure("Failed to get table");
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
                        return DatabaseResult<double>.Failure("Condition not satisfied");
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
                return DatabaseResult<double>.Success(value.AsDouble);
            }

            return DatabaseResult<double>.Failure("Failed to get updated value");
        }
        catch (Exception ex)
        {
            return DatabaseResult<double>.Failure($"DatabaseServiceMongoDB->IncrementAttributeAsync: {ex.Message}");
        }
    }

    /// <summary>
    /// Scans a table and returns all items.
    /// </summary>
    public async Task<DatabaseResult<IReadOnlyList<JObject>>> ScanTableAsync(
        string tableName,
        string[] keyNames,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return DatabaseResult<IReadOnlyList<JObject>>.Failure("Failed to get table");
            }

            var filter = Builders<BsonDocument>.Filter.Empty;
            var documents = await table.Find(filter).ToListAsync(cancellationToken);

            return ProcessScanResults(keyNames, documents);
        }
        catch (Exception e)
        {
            return DatabaseResult<IReadOnlyList<JObject>>.Failure($"DatabaseServiceMongoDB->ScanTableAsync: {e.Message}");
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
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return DatabaseResult<(IReadOnlyList<JObject>, string?, long?)>.Failure("Failed to get table");
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
                return DatabaseResult<(IReadOnlyList<JObject>, string?, long?)>.Failure(scanResult.ErrorMessage!);
            }

            var nextPageToken = skip + pageSize < totalCount ? (skip + pageSize).ToString() : null;
            return DatabaseResult<(IReadOnlyList<JObject>, string?, long?)>.Success(
                (scanResult.Data!, nextPageToken, totalCount));
        }
        catch (Exception e)
        {
            return DatabaseResult<(IReadOnlyList<JObject>, string?, long?)>.Failure(
                $"DatabaseServiceMongoDB->ScanTablePaginatedAsync: {e.Message}");
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
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return DatabaseResult<IReadOnlyList<JObject>>.Failure("Failed to get table");
            }

            var filter = BuildFilterFromCondition(filterCondition) ?? Builders<BsonDocument>.Filter.Empty;
            var documents = await table.Find(filter).ToListAsync(cancellationToken);

            return ProcessScanResults(keyNames, documents);
        }
        catch (Exception e)
        {
            return DatabaseResult<IReadOnlyList<JObject>>.Failure(
                $"DatabaseServiceMongoDB->ScanTableWithFilterAsync: {e.Message}");
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
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return DatabaseResult<(IReadOnlyList<JObject>, string?, long?)>.Failure("Failed to get table");
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
                return DatabaseResult<(IReadOnlyList<JObject>, string?, long?)>.Failure(scanResult.ErrorMessage!);
            }

            var nextPageToken = skip + pageSize < totalCount ? (skip + pageSize).ToString() : null;
            return DatabaseResult<(IReadOnlyList<JObject>, string?, long?)>.Success(
                (scanResult.Data!, nextPageToken, totalCount));
        }
        catch (Exception e)
        {
            return DatabaseResult<(IReadOnlyList<JObject>, string?, long?)>.Failure(
                $"DatabaseServiceMongoDB->ScanTableWithFilterPaginatedAsync: {e.Message}");
        }
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
        bool shouldOverrideIfExists = false,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return DatabaseResult<JObject?>.Failure("Failed to get table");
            }

            var filter = BuildEqFilter(keyName, keyValue);

            if (putOrUpdateItemType == PutOrUpdateItemType.PutItem)
            {
                if (!shouldOverrideIfExists)
                {
                    if (await HasTableMatchingResultWithFilterAsync(table, filter, cancellationToken))
                    {
                        return DatabaseResult<JObject?>.Failure("Item already exists");
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
                            return DatabaseResult<JObject?>.Failure("Condition not satisfied");
                        }
                    }
                }
            }

            JObject? returnItem = null;
            if (returnBehavior == ReturnItemBehavior.ReturnOldValues)
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

            if (returnBehavior == ReturnItemBehavior.ReturnNewValues)
            {
                returnItem = newObject;
            }

            return DatabaseResult<JObject?>.Success(returnItem);
        }
        catch (Exception ex)
        {
            return DatabaseResult<JObject?>.Failure($"DatabaseServiceMongoDB->PutOrUpdateItemAsync: {ex.Message}");
        }
    }

    private DatabaseResult<IReadOnlyList<JObject>> ProcessScanResults(string[] possibleKeyNames, List<BsonDocument> documents)
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

            return DatabaseResult<IReadOnlyList<JObject>>.Success(tempResults.ToList().AsReadOnly());
        }
        catch (JsonReaderException e)
        {
            return DatabaseResult<IReadOnlyList<JObject>>.Failure($"JSON parsing error: {e.Message}");
        }
        catch (Exception e)
        {
            return DatabaseResult<IReadOnlyList<JObject>>.Failure($"Processing error: {e.Message}");
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
            BsonType.Int32 => new PrimitiveType((long)bsonValue.AsInt32),
            BsonType.Int64 => new PrimitiveType(bsonValue.AsInt64),
            BsonType.Double => new PrimitiveType(bsonValue.AsDouble),
            BsonType.Binary => new PrimitiveType(bsonValue.AsByteArray),
            _ => new PrimitiveType(bsonValue.ToJson())
        };
    }

    private static FilterDefinition<BsonDocument>? BuildFilterFromCondition(DatabaseAttributeCondition condition)
    {
        return condition.ConditionType switch
        {
            DatabaseAttributeConditionType.AttributeExists => 
                Builders<BsonDocument>.Filter.Exists(condition.AttributeName, true),
            DatabaseAttributeConditionType.AttributeNotExists => 
                Builders<BsonDocument>.Filter.Exists(condition.AttributeName, false),
            _ when condition is ValueCondition valueCondition => BuildValueFilter(valueCondition),
            _ when condition is ArrayElementCondition arrayCondition => BuildArrayElementFilter(arrayCondition),
            _ => null
        };
    }

    private static FilterDefinition<BsonDocument>? BuildValueFilter(ValueCondition condition)
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
            DatabaseAttributeConditionType.AttributeEquals => 
                Builders<BsonDocument>.Filter.Eq(condition.AttributeName, value),
            DatabaseAttributeConditionType.AttributeNotEquals => 
                Builders<BsonDocument>.Filter.Ne(condition.AttributeName, value),
            DatabaseAttributeConditionType.AttributeGreater => 
                Builders<BsonDocument>.Filter.Gt(condition.AttributeName, value),
            DatabaseAttributeConditionType.AttributeGreaterOrEqual => 
                Builders<BsonDocument>.Filter.Gte(condition.AttributeName, value),
            DatabaseAttributeConditionType.AttributeLess => 
                Builders<BsonDocument>.Filter.Lt(condition.AttributeName, value),
            DatabaseAttributeConditionType.AttributeLessOrEqual => 
                Builders<BsonDocument>.Filter.Lte(condition.AttributeName, value),
            _ => null
        };
    }

    private static FilterDefinition<BsonDocument>? BuildArrayElementFilter(ArrayElementCondition condition)
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
            DatabaseAttributeConditionType.ArrayElementExists => 
                Builders<BsonDocument>.Filter.AnyIn(condition.AttributeName, elementValue),
            DatabaseAttributeConditionType.ArrayElementNotExists => 
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
        if (Options.AutoSortArrays == AutoSortArrays.Yes)
        {
            JsonUtilities.SortJObject(jsonObject, Options.AutoConvertRoundableFloatToInt == AutoConvertRoundableFloatToInt.Yes);
        }
        else if (Options.AutoConvertRoundableFloatToInt == AutoConvertRoundableFloatToInt.Yes)
        {
            JsonUtilities.ConvertRoundFloatToIntAllInJObject(jsonObject);
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

    public void Dispose()
    {
        _mongoClient?.Dispose();
        GC.SuppressFinalize(this);
    }
}
