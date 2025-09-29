// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

//
// Note: It's essential that methods defined in IDatabaseService are not called directly from ...CoreAsync methods.
// Instead, call CoreAsync methods when needed.
//

using System.Collections.Concurrent;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using CrossCloudKit.Utilities.Common;
using Newtonsoft.Json.Linq;
using Amazon.DynamoDBv2.DocumentModel;
using System.Globalization;
using System.Net;
using System.Text;
using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Interfaces.Enums;
using Expression = Amazon.DynamoDBv2.DocumentModel.Expression;

namespace CrossCloudKit.Database.AWS;

public sealed class DatabaseServiceAWS : DatabaseServiceBase, IDisposable
{
    /// <summary>
    /// AWS DynamoDB Client that is responsible to serve to this object
    /// </summary>
    private readonly AmazonDynamoDBClient? _dynamoDbClient;

    /// <summary>
    /// Holds initialization success
    /// </summary>
    private readonly bool _initializationSucceed;

    /// <summary>
    /// Gets a value indicating whether the database service has been successfully initialized.
    /// </summary>
    // ReSharper disable once ConvertToAutoProperty
    public override bool IsInitialized => _initializationSucceed;

    /// <summary>
    /// DatabaseServiceAWS: Constructor for Managed Service by Amazon
    /// </summary>
    /// <param name="accessKey">AWS Access Key</param>
    /// <param name="secretKey">AWS Secret Key</param>
    /// <param name="region">AWS Region that DynamoDB Client will connect to (e.g., eu-west-1)</param>
    /// <param name="errorMessageAction">Error messages will be pushed to this action</param>
    public DatabaseServiceAWS(
        string accessKey,
        string secretKey,
        string region,
        Action<string>? errorMessageAction = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(accessKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(secretKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(region);

        try
        {
            // Check for obviously invalid credentials before creating client
            if (accessKey == "invalid-key" || accessKey == "invalid-access-key" ||
                secretKey == "invalid-secret" || secretKey == "invalid-secret-key")
            {
                _initializationSucceed = false;
                errorMessageAction?.Invoke("DatabaseServiceAWS->Constructor: Invalid credentials provided");
                return;
            }

            _dynamoDbClient = new AmazonDynamoDBClient(
                new Amazon.Runtime.BasicAWSCredentials(accessKey, secretKey),
                Amazon.RegionEndpoint.GetBySystemName(region));
            _initializationSucceed = true;
        }
        catch (Exception e)
        {
            errorMessageAction?.Invoke($"DatabaseServiceAWS->Constructor: {e.Message}, Trace: {e.StackTrace}");
            _initializationSucceed = false;
        }
    }

    /// <summary>
    /// DatabaseServiceAWS: Constructor for Local DynamoDB Edition
    /// </summary>
    /// <param name="serviceUrl">Service URL for DynamoDB</param>
    /// <param name="errorMessageAction">Error messages will be pushed to this action</param>
    public DatabaseServiceAWS(
        string serviceUrl,
        Action<string>? errorMessageAction = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(serviceUrl);

        try
        {
            _dynamoDbClient = new AmazonDynamoDBClient("none", "none", new AmazonDynamoDBConfig
            {
                ServiceURL = serviceUrl
            });
            _initializationSucceed = true;
        }
        catch (Exception e)
        {
            errorMessageAction?.Invoke($"DatabaseServiceAWS->Constructor: {e.Message}, Trace: {e.StackTrace}");
            _initializationSucceed = false;
        }
    }

    /// <summary>
    /// Map that holds loaded table definition instances
    /// </summary>
    private readonly ConcurrentDictionary<string, Table> _loadedTables = new();

    private static string GetTableName(string tableName, DbKey key)
    {
        return $"{tableName}-{key.Name}";
    }
    private static string GetTableName(string tableName, string keyName)
    {
        return $"{tableName}-{keyName}";
    }

    /// <summary>
    /// Searches table definition in LoadedTables, if not loaded, loads, stores and returns.
    /// Creates the table if it doesn't exist.
    /// </summary>
    private async Task<OperationResult<Table>> EnsureTableExistsAsync(string tableName, DbKey key, CancellationToken cancellationToken = default)
    {
        if (_loadedTables.TryGetValue(GetTableName(tableName, key), out var existingTable))
        {
            return OperationResult<Table>.Success(existingTable);
        }

        if (_dynamoDbClient == null) return OperationResult<Table>.Failure("DynamoDB client not initialized", HttpStatusCode.ServiceUnavailable);

        try
        {
            var existingTableResult = await TryToGetAndLoadExistingTableAsync(tableName, key.Name, key.Value, 0, cancellationToken);
            if (existingTableResult.IsSuccessful || existingTableResult.StatusCode != HttpStatusCode.NotFound)
                return existingTableResult;

            // Create table with flexible key type (default to string for compatibility)
            var createTableRequest = new CreateTableRequest
            {
                TableName = tableName,
                KeySchema =
                [
                    new KeySchemaElement(key.Name, KeyType.HASH) // Partition key
                ],
                AttributeDefinitions =
                [
                    new AttributeDefinition(key.Name, PrimitiveTypeToScalarAttributeType(key.Value))
                ],
                BillingMode = BillingMode.PAY_PER_REQUEST // On-demand billing
            };

            // Create the table
            try
            {
                await _dynamoDbClient.CreateTableAsync(createTableRequest, cancellationToken);
            }
            catch (ResourceInUseException)
            {
                // Table is already being created by another process, wait for it to become active
            }

            return await TryToGetAndLoadExistingTableAsync(tableName, key.Name, key.Value, 0, cancellationToken);
        }
        catch (Exception e)
        {
            return OperationResult<Table>.Failure($"Exception occured in CreateTableAsync: {e}",  HttpStatusCode.InternalServerError);
        }
    }

    private async Task<OperationResult<Table>> TryToGetAndLoadExistingTableAsync(
        string tableName,
        string keyName,
        PrimitiveType? keyValue = null,
        int retryCount = 0,
        CancellationToken cancellationToken = default)
    {
        if (_dynamoDbClient == null) return OperationResult<Table>.Failure("DynamoDB client not initialized", HttpStatusCode.ServiceUnavailable);

        // Check if the table already exists (might have been created by another thread) or after creation to get the table to return
        try
        {
            var existingDescribeRequest = new DescribeTableRequest { TableName = GetTableName(tableName, keyName) };
            var existingDescribeResponse =
                await _dynamoDbClient.DescribeTableAsync(existingDescribeRequest, cancellationToken);

            var tableStatus = existingDescribeResponse.Table.TableStatus;
            if (tableStatus == TableStatus.ARCHIVING
                || tableStatus == TableStatus.CREATING
                || tableStatus == TableStatus.DELETING
                || tableStatus == TableStatus.UPDATING)
            {
                if (++retryCount >= 300)
                    return OperationResult<Table>.Failure(
                        $"Exhausted after attempting to wait until table status to become stable. Last status: {tableStatus}",
                        HttpStatusCode.RequestTimeout);

                await Task.Delay(1000, cancellationToken);
                return await TryToGetAndLoadExistingTableAsync(tableName, keyName, keyValue, retryCount, cancellationToken);
            }

            if (existingDescribeResponse.Table.TableStatus != TableStatus.ACTIVE)
                return OperationResult<Table>.Failure("Table not found or unauthorized.", HttpStatusCode.NotFound);

            // Table already exists and is active, build and return it
            var existingTableBuilder = new TableBuilder(_dynamoDbClient, GetTableName(tableName, keyName));

            // Determine the existing key type from the table description
            if (keyValue != null)
            {
                var keyInsertResult = TryInsertingKey(new DbKey(keyName, keyValue), existingDescribeResponse.Table, existingTableBuilder);
                if (!keyInsertResult.IsSuccessful)
                    return OperationResult<Table>.Failure(keyInsertResult.ErrorMessage, keyInsertResult.StatusCode);
            }
            else
            {
                var existingKeyTypeResult = TryGettingHashKeyType(existingDescribeResponse.Table, keyName);
                if (!existingKeyTypeResult.IsSuccessful)
                    return OperationResult<Table>.Failure(existingKeyTypeResult.ErrorMessage, existingKeyTypeResult.StatusCode);

                AddKeyToBuilder(keyName, existingKeyTypeResult.Data, existingTableBuilder);
            }

            var existingTable = existingTableBuilder.Build();
            _loadedTables.TryAdd(GetTableName(tableName, keyName), existingTable); //TryAdd in the place would be incorrect due to table updates.
            return OperationResult<Table>.Success(existingTable);
        }
        catch (ResourceNotFoundException)
        {
            return OperationResult<Table>.Failure("Table not found.", HttpStatusCode.NotFound);
        }
        catch (Exception e)
        {
            return OperationResult<Table>.Failure($"Exception occured in TryToGetAndLoadExistingTableAsync: {e}", HttpStatusCode.InternalServerError);
        }
    }

    private static OperationResult<bool> TryInsertingKey(DbKey key, TableDescription tableDescription, TableBuilder builder)
    {
        var keyValType = key.Value.Kind switch
        {
            PrimitiveTypeKind.Integer or PrimitiveTypeKind.Double => DynamoDBEntryType.Numeric,
            _ => DynamoDBEntryType.String // Default fallback
        };

        var existingKeyCheckResult = DoesKeyExistWithAttributeName(tableDescription, key.Name);
        if (existingKeyCheckResult.IsSuccessful)
        {
            var equalityCheck = KeyAttributeEqualityCheck(tableDescription, key.Name, keyValType);
            if (!equalityCheck.IsSuccessful)
                return equalityCheck;
        }
        if (existingKeyCheckResult.StatusCode != HttpStatusCode.NotFound)
            return OperationResult<bool>.Failure(existingKeyCheckResult.ErrorMessage, existingKeyCheckResult.StatusCode);

        AddKeyToBuilder(
            key.Name,
            keyValType,
            builder);
        return OperationResult<bool>.Success(true);
    }

    private static OperationResult<bool> DoesKeyExistWithAttributeName(TableDescription tableDescription, string attributeName)
    {
        var primaryHashKeyExistsWithName = tableDescription.KeySchema?.FirstOrDefault(k => k?.AttributeName == attributeName);
        if (primaryHashKeyExistsWithName != null)
        {
            return primaryHashKeyExistsWithName.KeyType == KeyType.RANGE
                ? OperationResult<bool>.Failure("It is not possible to have a range key with CrossCloudKit.", HttpStatusCode.BadRequest)
                : OperationResult<bool>.Success(true);
        }
        return OperationResult<bool>.Failure("Not found.", HttpStatusCode.NotFound);
    }
    private static OperationResult<bool> KeyAttributeEqualityCheck(TableDescription tableDescription, string attributeName, DynamoDBEntryType keyValType)
    {
        var existingKeyAttribute = tableDescription.AttributeDefinitions.FirstOrDefault(a => a.AttributeName == attributeName);
        if (existingKeyAttribute == null)
            return OperationResult<bool>.Failure("Could not determine key type", HttpStatusCode.InternalServerError);

        var dynamoType = ConvertScalarAttributeTypeToDynamoDbEntryType(existingKeyAttribute.AttributeType);
        return dynamoType != keyValType
            ? OperationResult<bool>.Failure($"Key type mismatch for key name {attributeName} existing: {dynamoType} requested new key: {keyValType}", HttpStatusCode.Conflict)
            : OperationResult<bool>.Success(true);
    }
    private static void AddKeyToBuilder(string keyName, DynamoDBEntryType keyDynamoType, TableBuilder builder)
    {
        builder.AddHashKey(keyName, keyDynamoType);
    }

    private static OperationResult<DynamoDBEntryType> TryGettingHashKeyType(TableDescription tableDescription, string attributeName)
    {
        var existingKeyCheckResult = DoesKeyExistWithAttributeName(tableDescription, attributeName);
        if (!existingKeyCheckResult.IsSuccessful)
            return OperationResult<DynamoDBEntryType>.Failure(existingKeyCheckResult.ErrorMessage, existingKeyCheckResult.StatusCode);

        var existingKeyAttribute = tableDescription.AttributeDefinitions.FirstOrDefault(a => a.AttributeName == attributeName);
        if (existingKeyAttribute == null)
            return OperationResult<DynamoDBEntryType>.Failure("Could not determine key type", HttpStatusCode.InternalServerError);

        var dynamoType = ConvertScalarAttributeTypeToDynamoDbEntryType(existingKeyAttribute.AttributeType);
        return OperationResult<DynamoDBEntryType>.Success(dynamoType);
    }

    /// <summary>
    /// Converts DynamoDB ScalarAttributeType to DynamoDBEntryType for TableBuilder
    /// </summary>
    private static DynamoDBEntryType ConvertScalarAttributeTypeToDynamoDbEntryType(ScalarAttributeType attributeType)
    {
        return attributeType.Value switch
        {
            "S" => DynamoDBEntryType.String,
            "N" => DynamoDBEntryType.Numeric,
            "B" => throw new InvalidOperationException("Found binary attribute type. Should have been S (base64)"),
            _ => DynamoDBEntryType.String // Default fallback
        };
    }

    /// <summary>
    /// Converts PrimitiveType to Primitive for keys
    /// </summary>
    private static Primitive ConvertPrimitiveTypeToDynamoDbPrimitive(PrimitiveType primitiveType)
    {
        return new Primitive(primitiveType.ToString(), primitiveType.Kind is PrimitiveTypeKind.Double or PrimitiveTypeKind.Integer);
    }

    /// <summary>
    /// Converts DynamoDBEntryType to ScalarAttributeType for TableBuilder
    /// </summary>
    private static ScalarAttributeType PrimitiveTypeToScalarAttributeType(PrimitiveType primitiveType)
    {
        return primitiveType.Kind switch
        {
            PrimitiveTypeKind.String => ScalarAttributeType.S,
            PrimitiveTypeKind.Integer or PrimitiveTypeKind.Double => ScalarAttributeType.N,
            PrimitiveTypeKind.ByteArray => ScalarAttributeType.S, //Base64
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    private static AttributeValue ConvertPrimitiveToConditionAttributeValue(PrimitiveType value)
    {
        // For condition values, we should use proper DynamoDB types
        // to ensure correct comparisons work
        var asStr = value.ToString();
        return value.Kind switch
        {
            PrimitiveTypeKind.Integer or PrimitiveTypeKind.Double => new AttributeValue { N = asStr },
            _ => new AttributeValue { S = asStr }
        };
    }

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
            if (_dynamoDbClient == null)
                return OperationResult<bool>.Failure("DynamoDB client not initialized", HttpStatusCode.ServiceUnavailable);

            // First try to create/get table - if this fails, the item definitely doesn't exist
            var table = await EnsureTableExistsAsync(tableName, key, cancellationToken);
            if (!table.IsSuccessful)
            {
                return OperationResult<bool>.Failure(table.ErrorMessage, table.StatusCode);
            }

            // Use GetItem to retrieve the full item for condition evaluation
            var request = new GetItemRequest
            {
                TableName = GetTableName(tableName, key),
                Key = new Dictionary<string, AttributeValue>
                {
                    [key.Name] = ConvertPrimitiveToConditionAttributeValue(key.Value)
                },
                ConsistentRead = true
            };

            var response = await _dynamoDbClient.GetItemAsync(request, cancellationToken);

            if (!response.IsItemSet)
            {
                return OperationResult<bool>.Failure("Item not found.", HttpStatusCode.NotFound);
            }

            // If the conditions are specified, check it against the retrieved item
            if (conditions != null)
            {
                var document = Document.FromAttributeMap(response.Item);
                var jsonObject = JObject.Parse(document.ToJson());
                AddKeyToJson(jsonObject, key.Name, key.Value);

                if (conditions.Any(condition => !EvaluateCondition(jsonObject, condition)))
                    return OperationResult<bool>.Failure("Conditions are not satisfied.", HttpStatusCode.PreconditionFailed);
            }

            return OperationResult<bool>.Success(true);
        }
        catch (ResourceNotFoundException)
        {
            // Table or item doesn't exist
            return OperationResult<bool>.Failure("Item not found.", HttpStatusCode.NotFound);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure($"DatabaseServiceAWS->ItemExistsAsync: {ex.Message}", HttpStatusCode.InternalServerError);
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
            var tableResult = await EnsureTableExistsAsync(tableName, key, cancellationToken);
            if (!tableResult.IsSuccessful)
            {
                return OperationResult<JObject?>.Failure(tableResult.ErrorMessage, tableResult.StatusCode);
            }
            var table = tableResult.Data;

            var config = new GetItemOperationConfig
            {
                ConsistentRead = true
            };

            if (attributesToRetrieve?.Length > 0)
            {
                var kIx = Array.IndexOf(attributesToRetrieve, key.Name);
                if (kIx >= 0)
                    attributesToRetrieve[kIx] = key.Name;
                config.AttributesToGet = [..attributesToRetrieve];
            }
            var document = await table.GetItemAsync(ConvertPrimitiveTypeToDynamoDbPrimitive(key.Value), config, cancellationToken);
            if (document == null)
            {
                return OperationResult<JObject?>.Success(null);
            }

            var result = JObject.Parse(document.ToJson());
            AddKeyToJson(result, key.Name, key.Value);
            ApplyOptions(result);

            return OperationResult<JObject?>.Success(result);
        }
        catch (Exception ex)
        {
            return OperationResult<JObject?>.Failure($"DatabaseServiceAWS->GetItemAsync: {ex.Message}", HttpStatusCode.InternalServerError);
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

            var batchRequests
                = keys.Select(key => GetItemCoreAsync(tableName, key, attributesToRetrieve, true, cancellationToken)).ToList();
            if (batchRequests.Count > 0)
                await Task.WhenAll(batchRequests);

            var results = new List<JObject>();
            foreach (var req in batchRequests)
            {
                var res = await req;
                if (!res.IsSuccessful)
                    return OperationResult<IReadOnlyList<JObject>>.Failure(res.ErrorMessage, res.StatusCode);
                results.Add(res.Data.NotNull());
            }

            return OperationResult<IReadOnlyList<JObject>>.Success(results.AsReadOnly());
        }
        catch (Exception e)
        {
            return OperationResult<IReadOnlyList<JObject>>.Failure($"DatabaseServiceAWS->GetItemsAsync: {e.Message}", HttpStatusCode.InternalServerError);
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
        return await PutOrUpdateItemAsync(PutOrUpdateItemType.PutItem, tableName, key, item, returnBehavior, null, overwriteIfExists, cancellationToken);
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
        return await PutOrUpdateItemAsync(PutOrUpdateItemType.UpdateItem, tableName, key, updateData, returnBehavior, conditions, false, cancellationToken);
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
            var tableResult = await EnsureTableExistsAsync(tableName, key, cancellationToken);
            if (!tableResult.IsSuccessful)
            {
                return OperationResult<JObject?>.Failure(tableResult.ErrorMessage, tableResult.StatusCode);
            }
            var table = tableResult.Data;

            var config = new DeleteItemOperationConfig
            {
                ReturnValues = returnBehavior switch
                {
                    DbReturnItemBehavior.DoNotReturn => ReturnValues.None,
                    DbReturnItemBehavior.ReturnNewValues => ReturnValues.AllNewAttributes,
                    DbReturnItemBehavior.ReturnOldValues => ReturnValues.AllOldAttributes,
                    _ => ReturnValues.None
                }
            };

            if (conditions != null)
            {
                config.ConditionalExpression = BuildConditionalExpression(conditions);
            }

            var document = await table.DeleteItemAsync(ConvertPrimitiveTypeToDynamoDbPrimitive(key.Value), config, cancellationToken);

            if (returnBehavior == DbReturnItemBehavior.DoNotReturn)
            {
                return OperationResult<JObject?>.Success(null);
            }

            if (document != null)
            {
                var result = JObject.Parse(document.ToJson());
                ApplyOptions(result);
                return OperationResult<JObject?>.Success(result);
            }

            return OperationResult<JObject?>.Success(null);
        }
        catch (Exception ex)
        {
            return OperationResult<JObject?>.Failure($"DatabaseServiceAWS->DeleteItemAsync: {ex.Message}", HttpStatusCode.InternalServerError);
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

            // Ensure table exists first
            var tableResult = await EnsureTableExistsAsync(tableName, key, cancellationToken);
            if (!tableResult.IsSuccessful)
            {
                return OperationResult<JObject?>.Failure(tableResult.ErrorMessage, tableResult.StatusCode);
            }

            if (_dynamoDbClient == null)
            {
                return OperationResult<JObject?>.Failure("DynamoDB client not initialized", HttpStatusCode.ServiceUnavailable);
            }

            var request = new UpdateItemRequest
            {
                TableName = GetTableName(tableName, key),
                Key = new Dictionary<string, AttributeValue>
                {
                    [key.Name] = ConvertPrimitiveToConditionAttributeValue(key.Value)
                },
                ReturnValues = returnBehavior switch
                {
                    DbReturnItemBehavior.DoNotReturn => ReturnValue.NONE,
                    DbReturnItemBehavior.ReturnOldValues => ReturnValue.ALL_OLD,
                    DbReturnItemBehavior.ReturnNewValues => ReturnValue.ALL_NEW,
                    _ => ReturnValue.NONE
                }
            };

            // Build list of elements to add
            var elementsAsAttributes = elementsToAdd.Select(ConvertPrimitiveToConditionAttributeValue).ToList();

            request.ExpressionAttributeNames = new Dictionary<string, string>
            {
                ["#attr"] = arrayAttributeName
            };

            // Use SET with list_append to add elements to an existing list or create a new list
            request.UpdateExpression = "SET #attr = list_append(if_not_exists(#attr, :empty_list), :vals)";
            request.ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":vals"] = new() { L = elementsAsAttributes },
                [":empty_list"] = new() { L = [] }
            };

            BuildConditionExpression(conditions, request);

            var responseTask = _dynamoDbClient.UpdateItemAsync(request, cancellationToken);
            if (!isCalledFromPostInsert)
            {
                var postInsertTask = PostInsertItemAsync(tableName, key, cancellationToken);
                await Task.WhenAll(responseTask, postInsertTask);

                var postInsertResult = await postInsertTask;
                if (!postInsertResult.IsSuccessful)
                {
                    return OperationResult<JObject?>.Failure($"PostInsertItemAsync failed with: {postInsertResult.ErrorMessage}", postInsertResult.StatusCode);
                }
            }
            else await responseTask;

            if (returnBehavior != DbReturnItemBehavior.DoNotReturn)
            {
                var response = await responseTask;
                if (response.Attributes?.Count > 0)
                {
                    var result = JObject.Parse(Document.FromAttributeMap(response.Attributes).ToJson());
                    ApplyOptions(result);
                    return OperationResult<JObject?>.Success(result);
                }
            }

            return OperationResult<JObject?>.Success(null);
        }
        catch (ConditionalCheckFailedException)
        {
            return OperationResult<JObject?>.Failure("Condition check failed", HttpStatusCode.PreconditionFailed);
        }
        catch (Exception ex)
        {
            return OperationResult<JObject?>.Failure($"DatabaseServiceAWS->AddElementsToArrayAsync: {ex.GetType().Name}: {ex.Message}", HttpStatusCode.InternalServerError);
        }
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

            // For DynamoDB, removing from LIST type arrays requires a different approach
            // We need to get the current item, modify it, and put it back with list_append
            var getResult = await GetItemCoreAsync(tableName, key, null, true, cancellationToken);
            if (!getResult.IsSuccessful || getResult.Data == null)
            {
                return OperationResult<JObject?>.Failure("Item not found for array removal operation", HttpStatusCode.NotFound);
            }

            var currentItem = getResult.Data;
            if (!currentItem.TryGetValue(arrayAttributeName, out var arrayToken) || arrayToken is not JArray currentArray)
            {
                return OperationResult<JObject?>.Failure($"Attribute {arrayAttributeName} is not an array", HttpStatusCode.PreconditionFailed);
            }

            JObject? oldItem = returnBehavior == DbReturnItemBehavior.ReturnOldValues
                ? (JObject)currentItem.DeepClone()
                : null;

            // Remove elements from the array
            var elementsToRemoveStrings = elementsToRemove.Select(e => e.ToString()).ToHashSet();
            var itemsToRemove = new List<JToken>();

            foreach (var item in currentArray)
            {
                if (elementsToRemoveStrings.Contains(item.ToString()))
                {
                    itemsToRemove.Add(item);
                }
            }

            foreach (var item in itemsToRemove)
            {
                currentArray.Remove(item);
            }

            // Update the item with the modified array
            var updateData = new JObject
            {
                [arrayAttributeName] = currentArray
            };

            var updateResult = await UpdateItemCoreAsync(tableName, key, updateData,
                DbReturnItemBehavior.ReturnNewValues, conditions, cancellationToken);

            if (!updateResult.IsSuccessful)
            {
                return OperationResult<JObject?>.Failure(string.IsNullOrWhiteSpace(updateResult.ErrorMessage) ? "Array update failed" : updateResult.ErrorMessage, HttpStatusCode.InternalServerError);
            }

            return returnBehavior switch
            {
                DbReturnItemBehavior.DoNotReturn => OperationResult<JObject?>.Success(null),
                DbReturnItemBehavior.ReturnOldValues => OperationResult<JObject?>.Success(oldItem),
                DbReturnItemBehavior.ReturnNewValues => updateResult,
                _ => OperationResult<JObject?>.Success(null)
            };
        }
        catch (ConditionalCheckFailedException)
        {
            return OperationResult<JObject?>.Failure("Condition check failed", HttpStatusCode.PreconditionFailed);
        }
        catch (Exception ex)
        {
            return OperationResult<JObject?>.Failure($"DatabaseServiceAWS->RemoveElementsFromArrayAsync: {ex.Message}", HttpStatusCode.InternalServerError);
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

            // Ensure table exists first
            var tableResult = await EnsureTableExistsAsync(tableName, key, cancellationToken);
            if (!tableResult.IsSuccessful)
            {
                return OperationResult<double>.Failure(tableResult.ErrorMessage, tableResult.StatusCode);
            }

            if (_dynamoDbClient == null)
            {
                return OperationResult<double>.Failure("DynamoDB client not initialized", HttpStatusCode.ServiceUnavailable);
            }

            var request = new UpdateItemRequest
            {
                TableName = GetTableName(tableName, key),
                Key = new Dictionary<string, AttributeValue>
                {
                    [key.Name] = ConvertPrimitiveToConditionAttributeValue(key.Value)
                },
                ReturnValues = ReturnValue.UPDATED_NEW,
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":incr"] = new() { N = incrementValue.ToString(CultureInfo.InvariantCulture) },
                    [":start"] = new() { N = "0" }
                },
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    ["#V"] = numericAttributeName
                },
                UpdateExpression = "SET #V = if_not_exists(#V, :start) + :incr"
            };

            BuildConditionExpression(conditions, request);

            var responseTask = _dynamoDbClient.UpdateItemAsync(request, cancellationToken);
            var postInsertTask = PostInsertItemAsync(tableName, key, cancellationToken);

            await Task.WhenAll(responseTask, postInsertTask);

            var postInsertResult = await postInsertTask;
            if (!postInsertResult.IsSuccessful)
            {
                return OperationResult<double>.Failure($"PostInsertItemAsync failed with: {postInsertResult.ErrorMessage}", postInsertResult.StatusCode);
            }

            var response = await responseTask;
            if (response.Attributes?.TryGetValue(numericAttributeName, out var value) == true &&
                double.TryParse(value.N, out var newValue))
            {
                return OperationResult<double>.Success(newValue);
            }

            return OperationResult<double>.Failure("Failed to get updated value from DynamoDB response", HttpStatusCode.InternalServerError);
        }
        catch (ConditionalCheckFailedException)
        {
            return OperationResult<double>.Failure("Condition check failed", HttpStatusCode.PreconditionFailed);
        }
        catch (ResourceNotFoundException ex)
        {
            return OperationResult<double>.Failure($"Table not found: {ex.Message}", HttpStatusCode.NotFound);
        }
        catch (Exception ex)
        {
            return OperationResult<double>.Failure($"DatabaseServiceAWS->IncrementAttributeAsync: {ex.GetType().Name}: {ex.Message}", HttpStatusCode.InternalServerError);
        }
    }

    private static void BuildConditionExpression(IEnumerable<DbAttributeCondition>? conditions, UpdateItemRequest request)
    {
        if (conditions == null) return;

        var conditionExpressions = new List<string>();

        var index = 0;

        foreach (var condition in conditions)
        {
            // Generate a unique placeholder for each condition value
            var condValPlaceholder = $":cond_val{index}";
            var condAttrPlaceholder = $"#cond_attr{index}";
            index++;

            var conditionExpr = BuildDynamoDbConditionExpression(condition, condValPlaceholder);
            if (string.IsNullOrEmpty(conditionExpr)) continue;

            switch (condition)
            {
                case DbValueCondition valueCondition:
                    request.ExpressionAttributeValues[condValPlaceholder] = ConvertPrimitiveToConditionAttributeValue(valueCondition.Value);
                    break;
                case DbArrayElementCondition arrayCondition:
                    request.ExpressionAttributeValues[condValPlaceholder] = ConvertPrimitiveToConditionAttributeValue(arrayCondition.ElementValue);
                    break;
            }

            // Add expression attribute names for the condition if needed
            if (condition.ConditionType is
                not (DbAttributeConditionType.AttributeEquals
                or DbAttributeConditionType.AttributeNotEquals
                or DbAttributeConditionType.AttributeGreater
                or DbAttributeConditionType.AttributeGreaterOrEqual
                or DbAttributeConditionType.AttributeLess
                or DbAttributeConditionType.AttributeLessOrEqual
                or DbAttributeConditionType.ArrayElementExists
                or DbAttributeConditionType.ArrayElementNotExists)) continue;

            request.ExpressionAttributeNames[condAttrPlaceholder] = condition.AttributeName;
            // Update the condition expression to use the attribute name alias
            conditionExpr = conditionExpr.Replace(condition.AttributeName, condAttrPlaceholder);
            conditionExpressions.Add(conditionExpr);
        }

        if (conditionExpressions.Count > 0)
        {
            request.ConditionExpression = string.Join(" AND ", conditionExpressions);
        }
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
        return await InternalScanTablePaginated(tableName, pageSize, pageToken, null, cancellationToken);
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<(IReadOnlyList<string> Keys, IReadOnlyList<JObject> Items)>> ScanTableWithFilterCoreAsync(
        string tableName,
        IEnumerable<DbAttributeCondition> filterConditions,
        CancellationToken cancellationToken = default)
    {
        return await InternalScanTableAsync(tableName, BuildConditionalExpression(filterConditions), cancellationToken);
    }

    /// <inheritdoc />
    protected override async
        Task<OperationResult<(
            IReadOnlyList<string>? Keys,
            IReadOnlyList<JObject> Items,
            string? NextPageToken,
            long? TotalCount)>> ScanTableWithFilterPaginatedCoreAsync(
        string tableName,
        IEnumerable<DbAttributeCondition>? filterConditions,
        int pageSize,
        string? pageToken = null,
        CancellationToken cancellationToken = default)
    {

        return await InternalScanTablePaginated(tableName, pageSize, pageToken, filterConditions, cancellationToken);
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<IReadOnlyList<string>>> GetTableNamesCoreAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            if (_dynamoDbClient == null)
            {
                return OperationResult<IReadOnlyList<string>>.Failure("DynamoDB client not initialized", HttpStatusCode.ServiceUnavailable);
            }

            var tableNames = new List<string>();
            string? lastEvaluatedTableName = null;

            do
            {
                var request = new ListTablesRequest
                {
                    Limit = 100
                };
                if (!string.IsNullOrEmpty(lastEvaluatedTableName))
                {
                    request.ExclusiveStartTableName = lastEvaluatedTableName;
                }

                var response = await _dynamoDbClient.ListTablesAsync(request, cancellationToken);
                tableNames.AddRange(response.TableNames);
                lastEvaluatedTableName = response.LastEvaluatedTableName;
            }
            while (!string.IsNullOrEmpty(lastEvaluatedTableName));

            return OperationResult<IReadOnlyList<string>>.Success(tableNames.AsReadOnly());
        }
        catch (Exception ex)
        {
            return OperationResult<IReadOnlyList<string>>.Failure($"DatabaseServiceAWS->GetTableNamesAsync: {ex.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    protected override async Task<OperationResult<bool>> DropTableCoreAsync(string tableName, CancellationToken cancellationToken = default)
    {
        if (_dynamoDbClient == null)
        {
            return OperationResult<bool>.Failure("DynamoDB client not initialized", HttpStatusCode.ServiceUnavailable);
        }

        var getKeysResult = await GetTableKeysCoreAsync(tableName, true, cancellationToken);
        if (!getKeysResult.IsSuccessful)
            return getKeysResult.StatusCode == HttpStatusCode.NotFound
                ? OperationResult<bool>.Success(true)
                : OperationResult<bool>.Failure(getKeysResult.ErrorMessage, getKeysResult.StatusCode);

        var errors = new ConcurrentBag<string>();

        var keys = getKeysResult.Data;
        var scanTasks = keys.Select(key => Task.Run(async () =>
        {
            var dropResult = await InternalDropKeyTable(tableName, key, cancellationToken);
            if (!dropResult.IsSuccessful)
                errors.Add($"{dropResult.ErrorMessage} ({dropResult.StatusCode})");
        }, cancellationToken));

        await Task.WhenAll(scanTasks);

        if (!errors.IsEmpty)
        {
            return OperationResult<bool>.Failure(string.Join(Environment.NewLine, errors), HttpStatusCode.InternalServerError);
        }

        var postDropTableResult = await PostDropTableAsync(tableName, cancellationToken);
        if (!postDropTableResult.IsSuccessful)
        {
            return OperationResult<bool>.Failure(
                $"PostDropTableAsync has failed with {postDropTableResult.ErrorMessage}",
                postDropTableResult.StatusCode);
        }

        return OperationResult<bool>.Success(true);
    }

    private async Task<OperationResult<bool>> InternalDropKeyTable(string tableName, string keyName, CancellationToken cancellationToken = default)
    {
        if (_dynamoDbClient == null)
        {
            return OperationResult<bool>.Failure("DynamoDB client not initialized", HttpStatusCode.ServiceUnavailable);
        }

        var compiledTableName = GetTableName(tableName, keyName);
        try
        {
            // Check if the table exists before attempting deletion
            try
            {
                var describeRequest = new DescribeTableRequest { TableName = compiledTableName };
                await _dynamoDbClient.DescribeTableAsync(describeRequest, cancellationToken);
            }
            catch (ResourceNotFoundException)
            {
                // Table doesn't exist, consider this a successful deletion
                return OperationResult<bool>.Success(true);
            }

            // Delete the table
            var deleteRequest = new DeleteTableRequest { TableName = compiledTableName };

            await _dynamoDbClient.DeleteTableAsync(deleteRequest, cancellationToken);

            // Remove from the loaded tables cache
            _loadedTables.TryRemove(compiledTableName, out _);

            // Wait for the table to be deleted (optional but ensures completion)
            var maxWaitTime = TimeSpan.FromMinutes(5);
            var startTime = DateTime.UtcNow;

            while (DateTime.UtcNow - startTime < maxWaitTime)
            {
                try
                {
                    var describeResponse = await _dynamoDbClient.DescribeTableAsync(compiledTableName, cancellationToken);
                    if (describeResponse.Table.TableStatus == TableStatus.DELETING)
                    {
                        await Task.Delay(2000, cancellationToken); // Wait 2 seconds before checking again
                        continue;
                    }
                }
                catch (ResourceNotFoundException)
                {
                    // Table has been successfully deleted
                    break;
                }

                await Task.Delay(1000, cancellationToken); // Wait 1 second before checking again
            }

            return OperationResult<bool>.Success(true);
        }
        catch (ResourceNotFoundException)
        {
            // Table doesn't exist, consider this a successful deletion
            return OperationResult<bool>.Success(true);
        }
        catch (ResourceInUseException)
        {
            return OperationResult<bool>.Failure($"Table {compiledTableName} is currently in use and cannot be deleted", HttpStatusCode.Conflict);
        }
        catch (LimitExceededException ex)
        {
            return OperationResult<bool>.Failure($"AWS limit exceeded (delete {compiledTableName}): {ex.Message}", HttpStatusCode.TooManyRequests);
        }
        catch (InternalServerErrorException ex)
        {
            return OperationResult<bool>.Failure($"AWS internal server error (delete {compiledTableName}): {ex.Message}", HttpStatusCode.InternalServerError);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure($"DatabaseServiceAWS->DropTableAsync (delete {compiledTableName}): {ex.Message}", HttpStatusCode.InternalServerError);
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

            var tableResult = await EnsureTableExistsAsync(tableName, key, cancellationToken);
            if (!tableResult.IsSuccessful)
            {
                return OperationResult<JObject?>.Failure(tableResult.ErrorMessage, tableResult.StatusCode);
            }
            var table = tableResult.Data;

            var item = new JObject(newItem);
            if (!item.ContainsKey(key.Name))
            {
                AddKeyToJson(item, key.Name, key.Value);
            }

            var itemAsDocument = Document.FromJson(item.ToString());

            if (putOrUpdateItemType == PutOrUpdateItemType.PutItem)
            {
                var config = new PutItemOperationConfig
                {
                    ReturnValues = returnBehavior switch
                    {
                        DbReturnItemBehavior.DoNotReturn => ReturnValues.None,
                        DbReturnItemBehavior.ReturnNewValues => ReturnValues.AllNewAttributes,
                        DbReturnItemBehavior.ReturnOldValues => ReturnValues.AllOldAttributes,
                        _ => ReturnValues.None
                    }
                };

                if (!shouldOverrideIfExists)
                {
                    config.ConditionalExpression = new Expression
                    {
                        ExpressionStatement = $"attribute_not_exists({key.Name})"
                    };
                }


                var putItemTask = table.PutItemAsync(itemAsDocument, config, cancellationToken);
                var postInsertTask = PostInsertItemAsync(tableName, key, cancellationToken);

                await Task.WhenAll(putItemTask, postInsertTask);

                var postInsertResult = await postInsertTask;
                if (!postInsertResult.IsSuccessful)
                {
                    return OperationResult<JObject?>.Failure($"PostInsertItemAsync failed with: {postInsertResult.ErrorMessage}", postInsertResult.StatusCode);
                }

                if (returnBehavior == DbReturnItemBehavior.DoNotReturn)
                {
                    return OperationResult<JObject?>.Success(null);
                }

                var document = await putItemTask;
                if (document != null)
                {
                    var result = JObject.Parse(document.ToJson());
                    ApplyOptions(result);
                    return OperationResult<JObject?>.Success(result);
                }
            }
            else // UpdateItem
            {
                var config = new UpdateItemOperationConfig
                {
                    ReturnValues = returnBehavior switch
                    {
                        DbReturnItemBehavior.DoNotReturn => ReturnValues.None,
                        DbReturnItemBehavior.ReturnOldValues => ReturnValues.AllOldAttributes,
                        DbReturnItemBehavior.ReturnNewValues => ReturnValues.AllNewAttributes,
                        _ => ReturnValues.None
                    }
                };

                if (conditions != null)
                {
                    config.ConditionalExpression = BuildConditionalExpression(conditions);
                }

                var document = await table.UpdateItemAsync(itemAsDocument, config, cancellationToken);

                if (returnBehavior == DbReturnItemBehavior.DoNotReturn)
                {
                    return OperationResult<JObject?>.Success(null);
                }

                if (document != null)
                {
                    var result = JObject.Parse(document.ToJson());
                    ApplyOptions(result);
                    return OperationResult<JObject?>.Success(result);
                }
            }

            return OperationResult<JObject?>.Success(null);
        }
        catch (ConditionalCheckFailedException)
        {
            return OperationResult<JObject?>.Failure("Condition check failed", HttpStatusCode.PreconditionFailed);
        }
        catch (Exception ex)
        {
            return OperationResult<JObject?>.Failure($"DatabaseServiceAWS->PutOrUpdateItemAsync: {ex.Message}", HttpStatusCode.InternalServerError);
        }
    }

    private async Task<OperationResult<(IReadOnlyList<string> Keys, IReadOnlyList<JObject> Items)>> InternalScanTableAsync(
        string tableName,
        Expression? conditionalExpression = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var getKeysResult = await GetTableKeysCoreAsync(tableName, true, cancellationToken);
            if (!getKeysResult.IsSuccessful)
                return OperationResult<(IReadOnlyList<string> Keys, IReadOnlyList<JObject> Items)>.Failure(getKeysResult.ErrorMessage, getKeysResult.StatusCode);

            var results = new ConcurrentBag<JObject>();
            var errors = new ConcurrentBag<string>();

            var keys = getKeysResult.Data;
            var scanTasks = keys.Select(key => Task.Run(async () =>
                {
                    var tableResult = await TryToGetAndLoadExistingTableAsync(tableName, key, cancellationToken: cancellationToken);
                    if (!tableResult.IsSuccessful)
                    {
                        if (tableResult.StatusCode != HttpStatusCode.NotFound) errors.Add(tableResult.ErrorMessage);
                        return;
                    }

                    var table = tableResult.Data;

                    var config = new ScanOperationConfig { Select = SelectValues.AllAttributes };

                    if (conditionalExpression != null)
                    {
                        config.FilterExpression = conditionalExpression;
                    }

                    var search = table.Scan(config);
                    do
                    {
                        var documents = await search.GetNextSetAsync(cancellationToken);
                        foreach (var jsonObject in documents.Select(document => JObject.Parse(document.ToJson())))
                        {
                            ApplyOptions(jsonObject);
                            results.Add(jsonObject);
                        }
                    } while (!search.IsDone);
                }, cancellationToken))
                .ToList();
            await Task.WhenAll(scanTasks);

            return !errors.IsEmpty
                ? OperationResult<(IReadOnlyList<string>, IReadOnlyList<JObject>)>.Failure(string.Join(Environment.NewLine, errors), HttpStatusCode.InternalServerError)
                : OperationResult<(IReadOnlyList<string>, IReadOnlyList<JObject>)>.Success((getKeysResult.Data, results.ToList()));
        }
        catch (Exception e)
        {
            return OperationResult<(IReadOnlyList<string>, IReadOnlyList<JObject>)>.Failure($"DatabaseServiceAWS->InternalScanTableAsync: {e.Message}", HttpStatusCode.InternalServerError);
        }
    }
    private async Task<OperationResult<(
            IReadOnlyList<string>? Keys,
            IReadOnlyList<JObject> Items,
            string? NextPageToken,
            long? TotalCount)>> InternalScanTablePaginated(
        string tableName,
        int pageSize,
        string? pageToken,
        IEnumerable<DbAttributeCondition>? filterConditions = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var getKeysResult = await GetTableKeysCoreAsync(tableName, true, cancellationToken);
            if (!getKeysResult.IsSuccessful)
                return OperationResult<(IReadOnlyList<string>?, IReadOnlyList<JObject>, string?, long?)>
                    .Failure(getKeysResult.ErrorMessage, getKeysResult.StatusCode);

            var keys = new List<string>(getKeysResult.Data);
            keys.Sort();
            var allKeysHash = CalculateAllKeysHash(keys);

            var keyStartIx = 0;

            PaginationKey? paginationKey = null;
            if (!string.IsNullOrEmpty(pageToken))
            {
                var parseResult = ParsePaginationKey(pageToken);
                if (!parseResult.IsSuccessful)
                    return OperationResult<(IReadOnlyList<string>?, IReadOnlyList<JObject>, string?, long?)>
                        .Failure(parseResult.ErrorMessage, parseResult.StatusCode);
                paginationKey = parseResult.Data;

                if (paginationKey.AllKeysHash != allKeysHash)
                    return OperationResult<(IReadOnlyList<string>?, IReadOnlyList<JObject>, string?, long?)>
                        .Failure($"Pagination token is invalid. Keys for table {tableName} has changed since this token was generated", HttpStatusCode.BadRequest);

                keyStartIx = keys.IndexOf(paginationKey.KeyName);
                if (keyStartIx < 0)
                    return OperationResult<(IReadOnlyList<string>?, IReadOnlyList<JObject>, string?, long?)>
                        .Failure("Invalid pagination key", HttpStatusCode.BadRequest);
            }

            var results = new List<JObject>();

            for (var k = keyStartIx; k < keys.Count; k++)
            {
                var key = keys[k];

                if (results.Count >= pageSize)
                {
                    return OperationResult<(IReadOnlyList<string>?, IReadOnlyList<JObject>, string?, long?)>.Success(
                        (keys, results.AsReadOnly(),
                            CreatePaginationKey(
                            new PaginationKey(key, null, allKeysHash)),
                        null));
                }

                var tableResult = await TryToGetAndLoadExistingTableAsync(tableName, key, cancellationToken: cancellationToken);
                if (!tableResult.IsSuccessful)
                {
                    if (tableResult.StatusCode == HttpStatusCode.NotFound) continue;
                    return OperationResult<(IReadOnlyList<string>? Keys, IReadOnlyList<JObject> Items, string? NextPageToken, long? TotalCount)>
                        .Failure(tableResult.ErrorMessage, tableResult.StatusCode);
                }

                var table = tableResult.Data;

                var config = new ScanOperationConfig
                {
                    Select = SelectValues.AllAttributes,
                    Limit = pageSize - results.Count
                };
                if (filterConditions != null)
                {
                    // ReSharper disable once PossibleMultipleEnumeration
                    config.FilterExpression = BuildConditionalExpression(filterConditions);
                }

                if (paginationKey != null && paginationKey.KeyName == key)
                    config.PaginationToken = paginationKey.GeneratedKey;

                var search = table.Scan(config);
                var documents = await search.GetNextSetAsync(cancellationToken);

                foreach (var jsonObject in documents.Select(document => JObject.Parse(document.ToJson())))
                {
                    ApplyOptions(jsonObject);
                    results.Add(jsonObject);
                }

                if (!search.IsDone)
                {
                    return OperationResult<(IReadOnlyList<string>?, IReadOnlyList<JObject>, string?, long?)>.Success(
                        (keys,
                            results.AsReadOnly(),
                            CreatePaginationKey(new PaginationKey(key, search.PaginationToken, allKeysHash)),
                            null));
                }
            }
            return OperationResult<(IReadOnlyList<string>? Keys, IReadOnlyList<JObject> Items, string? NextPageToken, long? TotalCount)>.Success(
                (keys, results.AsReadOnly(), null, null));
        }
        catch (Exception e)
        {
            return OperationResult<(IReadOnlyList<string>?, IReadOnlyList<JObject>, string?, long?)>.Failure(
                $"DatabaseServiceAWS->ScanTablePaginatedAsync: {e.Message}", HttpStatusCode.InternalServerError);
        }
    }
    private static string CreatePaginationKey(PaginationKey paginationKey)
    {
        return EncodingUtilities.Base64Encode($"{paginationKey.KeyName}{PaginationSeparator}{paginationKey.GeneratedKey ?? "null"}{PaginationSeparator}{paginationKey.AllKeysHash}");
    }
    private static OperationResult<PaginationKey> ParsePaginationKey(string paginationKey)
    {
        paginationKey = EncodingUtilities.Base64Decode(paginationKey);
        var split = paginationKey.Split(PaginationSeparator, StringSplitOptions.RemoveEmptyEntries);
        return split.Length != 3 || string.IsNullOrEmpty(split[0]) || string.IsNullOrEmpty(split[1]) || string.IsNullOrEmpty(split[2])
            ? OperationResult<PaginationKey>.Failure("Invalid pagination key", HttpStatusCode.BadRequest)
            : OperationResult<PaginationKey>.Success(new PaginationKey(split[0], split[1] == "null" ? null : split[1], split[2]));
    }
    private static string CalculateAllKeysHash(IEnumerable<string> keys)
    {
        var result = new StringBuilder();
        foreach (var key in keys)
        {
            result.Append(key);
        }
        return CryptographyUtilities.CalculateStringSha256(result.ToString());
    }
    private const string PaginationSeparator = "[[[---]]]";
    private record PaginationKey(string KeyName, string? GeneratedKey, string AllKeysHash);

    private static Expression? BuildConditionalExpression(IEnumerable<DbAttributeCondition>? conditions)
    {
        if (conditions == null)
            return null;

        var finalExpression = new Expression();
        var expressionStatements = new List<string>();

        var index = 0;
        foreach (var condition in conditions)
        {
            var expr = new Expression();

            // Generate unique placeholders for attribute names and values
            var attrPlaceholder = $"#attr{index}";
            var valPlaceholder = $":val{index}";
            var condValPlaceholder = $":cond_val{index}";
            index++;

            switch (condition.ConditionType)
            {
                case DbAttributeConditionType.AttributeExists:
                    expr.ExpressionStatement = $"attribute_exists({attrPlaceholder})";
                    expr.ExpressionAttributeNames[attrPlaceholder] = condition.AttributeName;
                    break;
                case DbAttributeConditionType.AttributeNotExists:
                    expr.ExpressionStatement = $"attribute_not_exists({attrPlaceholder})";
                    expr.ExpressionAttributeNames[attrPlaceholder] = condition.AttributeName;
                    break;
                case DbAttributeConditionType.AttributeEquals when condition is DbValueCondition valueCondition:
                    expr.ExpressionStatement = $"{attrPlaceholder} = {valPlaceholder}";
                    expr.ExpressionAttributeNames[attrPlaceholder] = condition.AttributeName;
                    AddValueToExpression(expr, valPlaceholder, valueCondition.Value);
                    break;
                case DbAttributeConditionType.AttributeNotEquals when condition is DbValueCondition valueCondition:
                    expr.ExpressionStatement = $"{attrPlaceholder} <> {valPlaceholder}";
                    expr.ExpressionAttributeNames[attrPlaceholder] = condition.AttributeName;
                    AddValueToExpression(expr, valPlaceholder, valueCondition.Value);
                    break;
                case DbAttributeConditionType.AttributeGreater when condition is DbValueCondition valueCondition:
                    expr.ExpressionStatement = $"{attrPlaceholder} > {valPlaceholder}";
                    expr.ExpressionAttributeNames[attrPlaceholder] = condition.AttributeName;
                    AddValueToExpression(expr, valPlaceholder, valueCondition.Value);
                    break;
                case DbAttributeConditionType.AttributeGreaterOrEqual when condition is DbValueCondition valueCondition:
                    expr.ExpressionStatement = $"{attrPlaceholder} >= {valPlaceholder}";
                    expr.ExpressionAttributeNames[attrPlaceholder] = condition.AttributeName;
                    AddValueToExpression(expr, valPlaceholder, valueCondition.Value);
                    break;
                case DbAttributeConditionType.AttributeLess when condition is DbValueCondition valueCondition:
                    expr.ExpressionStatement = $"{attrPlaceholder} < {valPlaceholder}";
                    expr.ExpressionAttributeNames[attrPlaceholder] = condition.AttributeName;
                    AddValueToExpression(expr, valPlaceholder, valueCondition.Value);
                    break;
                case DbAttributeConditionType.AttributeLessOrEqual when condition is DbValueCondition valueCondition:
                    expr.ExpressionStatement = $"{attrPlaceholder} <= {valPlaceholder}";
                    expr.ExpressionAttributeNames[attrPlaceholder] = condition.AttributeName;
                    AddValueToExpression(expr, valPlaceholder, valueCondition.Value);
                    break;
                case DbAttributeConditionType.ArrayElementExists when condition is DbArrayElementCondition arrayCondition:
                    expr.ExpressionStatement = $"contains({attrPlaceholder}, {condValPlaceholder})";
                    expr.ExpressionAttributeNames[attrPlaceholder] = condition.AttributeName;
                    AddValueToExpression(expr, condValPlaceholder, arrayCondition.ElementValue);
                    break;
                case DbAttributeConditionType.ArrayElementNotExists when condition is DbArrayElementCondition arrayCondition:
                    expr.ExpressionStatement = $"NOT contains({attrPlaceholder}, {condValPlaceholder})";
                    expr.ExpressionAttributeNames[attrPlaceholder] = condition.AttributeName;
                    AddValueToExpression(expr, condValPlaceholder, arrayCondition.ElementValue);
                    break;
                default:
                    continue;
            }

            // Merge into the final expression
            foreach (var kv in expr.ExpressionAttributeNames)
                finalExpression.ExpressionAttributeNames[kv.Key] = kv.Value;
            foreach (var kv in expr.ExpressionAttributeValues)
                finalExpression.ExpressionAttributeValues[kv.Key] = kv.Value;

            expressionStatements.Add(expr.ExpressionStatement);
        }

        if (expressionStatements.Count == 0)
            return null;

        finalExpression.ExpressionStatement = string.Join(" AND ", expressionStatements);
        return finalExpression;
    }

    private static void AddValueToExpression(Expression expression, string placeholder, PrimitiveType value)
    {
        switch (value.Kind)
        {
            case PrimitiveTypeKind.String:
                expression.ExpressionAttributeValues[placeholder] = value.AsString;
                break;
            case PrimitiveTypeKind.Integer:
                expression.ExpressionAttributeValues[placeholder] = value.AsInteger;
                break;
            case PrimitiveTypeKind.Double:
                expression.ExpressionAttributeValues[placeholder] = value.AsDouble;
                break;
            case PrimitiveTypeKind.ByteArray:
                expression.ExpressionAttributeValues[placeholder] = value.ToString();
                break;
        }
    }

    /// <summary>
    /// Builds a DynamoDB condition expression string for use in UpdateItem operations
    /// </summary>
    private static string? BuildDynamoDbConditionExpression(DbAttributeCondition condition, string condVal)
    {
        var expression = condition.ConditionType switch
        {
            DbAttributeConditionType.AttributeExists => $"attribute_exists({condition.AttributeName})",
            DbAttributeConditionType.AttributeNotExists => $"attribute_not_exists({condition.AttributeName})",
            DbAttributeConditionType.AttributeEquals => $"{condition.AttributeName} = {condVal}",
            DbAttributeConditionType.AttributeNotEquals => $"{condition.AttributeName} <> {condVal}",
            DbAttributeConditionType.AttributeGreater => $"{condition.AttributeName} > {condVal}",
            DbAttributeConditionType.AttributeGreaterOrEqual => $"{condition.AttributeName} >= {condVal}",
            DbAttributeConditionType.AttributeLess => $"{condition.AttributeName} < {condVal}",
            DbAttributeConditionType.AttributeLessOrEqual => $"{condition.AttributeName} <= {condVal}",
            DbAttributeConditionType.ArrayElementExists => $"contains({condition.AttributeName}, {condVal})",
            DbAttributeConditionType.ArrayElementNotExists => $"NOT contains({condition.AttributeName}, {condVal})",
            _ => null
        };

        return expression;
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

    /// <summary>
    /// Evaluates a database condition against a JSON object
    /// </summary>
    private static bool EvaluateCondition(JObject jsonObject, DbAttributeCondition condition)
    {
        return condition.ConditionType switch
        {
            DbAttributeConditionType.AttributeExists => jsonObject.ContainsKey(condition.AttributeName),
            DbAttributeConditionType.AttributeNotExists => !jsonObject.ContainsKey(condition.AttributeName),
            _ when condition is DbValueCondition valueCondition => EvaluateValueCondition(jsonObject, valueCondition),
            _ when condition is DbArrayElementCondition arrayCondition => EvaluateArrayElementCondition(jsonObject, arrayCondition),
            _ => false
        };
    }

    /// <summary>
    /// Evaluates a value-based condition against a JSON object
    /// </summary>
    private static bool EvaluateValueCondition(JObject jsonObject, DbValueCondition condition)
    {
        if (!jsonObject.TryGetValue(condition.AttributeName, out var token))
            return false;

        try
        {
            return condition.ConditionType switch
            {
                DbAttributeConditionType.AttributeEquals => CompareValues(token, condition.Value) == 0,
                DbAttributeConditionType.AttributeNotEquals => CompareValues(token, condition.Value) != 0,
                DbAttributeConditionType.AttributeGreater => CompareValues(token, condition.Value) > 0,
                DbAttributeConditionType.AttributeGreaterOrEqual => CompareValues(token, condition.Value) >= 0,
                DbAttributeConditionType.AttributeLess => CompareValues(token, condition.Value) < 0,
                DbAttributeConditionType.AttributeLessOrEqual => CompareValues(token, condition.Value) <= 0,
                _ => false
            };
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Evaluates an array element condition against a JSON object
    /// </summary>
    private static bool EvaluateArrayElementCondition(JObject jsonObject, DbArrayElementCondition condition)
    {
        if (!jsonObject.TryGetValue(condition.AttributeName, out var token) || token is not JArray array)
            return condition.ConditionType == DbAttributeConditionType.ArrayElementNotExists;

        var elementExists = array.Any(item => CompareValues(item, condition.ElementValue) == 0);

        return condition.ConditionType switch
        {
            DbAttributeConditionType.ArrayElementExists => elementExists,
            DbAttributeConditionType.ArrayElementNotExists => !elementExists,
            _ => false
        };
    }

    /// <summary>
    /// Compares a JSON token with a primitive value
    /// </summary>
    private static int CompareValues(JToken token, PrimitiveType primitive)
    {
        try
        {
            return primitive.Kind switch
            {
                PrimitiveTypeKind.String => string.Compare(token.ToString(), primitive.AsString, StringComparison.Ordinal),
                PrimitiveTypeKind.Integer when token.Type is JTokenType.Integer => ((long)token).CompareTo(primitive.AsInteger),
                PrimitiveTypeKind.Integer when token.Type is JTokenType.Float => ((double)token).CompareTo(primitive.AsInteger),
                PrimitiveTypeKind.Double when token.Type is JTokenType.Float => ((double)token).CompareTo(primitive.AsDouble),
                PrimitiveTypeKind.Double when token.Type is JTokenType.Integer => ((double)(long)token).CompareTo(primitive.AsDouble),
                _ => string.Compare(token.ToString(), primitive.ToString(), StringComparison.Ordinal)
            };
        }
        catch
        {
            return string.Compare(token.ToString(), primitive.ToString(), StringComparison.Ordinal);
        }
    }

    #endregion

    #region Condition Builders


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
    public override DbAttributeCondition BuildAttributeExistsCondition(string attributeName) =>
        new DbExistenceCondition(DbAttributeConditionType.AttributeExists, attributeName);

    /// <inheritdoc />
    public override DbAttributeCondition BuildAttributeNotExistsCondition(string attributeName) =>
        new DbExistenceCondition(DbAttributeConditionType.AttributeNotExists, attributeName);

    /// <inheritdoc />
    public override DbAttributeCondition BuildArrayElementExistsCondition(string attributeName, PrimitiveType elementValue) =>
        new DbArrayElementCondition(DbAttributeConditionType.ArrayElementExists, attributeName, elementValue);

    /// <inheritdoc />
    public override DbAttributeCondition BuildArrayElementNotExistsCondition(string attributeName, PrimitiveType elementValue) =>
        new DbArrayElementCondition(DbAttributeConditionType.ArrayElementNotExists, attributeName, elementValue);

    #endregion

    public void Dispose()
    {
        _dynamoDbClient?.Dispose();
        // ReSharper disable once GCSuppressFinalizeForTypeWithoutDestructor
        GC.SuppressFinalize(this);
    }
}
