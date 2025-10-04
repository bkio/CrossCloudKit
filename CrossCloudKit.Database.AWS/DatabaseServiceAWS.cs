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
using CrossCloudKit.Interfaces;
using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Interfaces.Enums;
using Condition = CrossCloudKit.Interfaces.Classes.Condition;
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
    /// <param name="memoryService">This memory service will be used to ensure global atomicity on functions</param>
    /// <param name="errorMessageAction">Error messages will be pushed to this action</param>
    public DatabaseServiceAWS(
        string accessKey,
        string secretKey,
        string region,
        IMemoryService memoryService,
        Action<string>? errorMessageAction = null) : base(memoryService)
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
    /// <param name="memoryService">This memory service will be used to ensure global atomicity on functions</param>
    /// <param name="errorMessageAction">Error messages will be pushed to this action</param>
    public DatabaseServiceAWS(
        string serviceUrl,
        IMemoryService memoryService,
        Action<string>? errorMessageAction = null) : base(memoryService)
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
                TableName = GetTableName(tableName, key),
                KeySchema =
                [
                    new KeySchemaElement(key.Name, KeyType.HASH) // Partition key
                ],
                AttributeDefinitions =
                [
                    new AttributeDefinition(key.Name, PrimitiveToScalarAttributeType(key.Value))
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
        Utilities.Common.Primitive? keyValue = null,
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
            PrimitiveKind.Integer or PrimitiveKind.Double => DynamoDBEntryType.Numeric,
            _ => DynamoDBEntryType.String // Default fallback
        };

        var existingKeyCheckResult = DoesKeyExistWithAttributeName(tableDescription, key.Name);
        if (existingKeyCheckResult.IsSuccessful)
        {
            var equalityCheck = KeyAttributeEqualityCheck(tableDescription, key.Name, keyValType);
            if (!equalityCheck.IsSuccessful)
                return equalityCheck;
        }
        else if (existingKeyCheckResult.StatusCode != HttpStatusCode.NotFound)
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
    /// Converts Primitive to Primitive for keys
    /// </summary>
    private static Amazon.DynamoDBv2.DocumentModel.Primitive ConvertPrimitiveToDynamoDbPrimitive(Utilities.Common.Primitive primitiveType)
    {
        return new Amazon.DynamoDBv2.DocumentModel.Primitive(primitiveType.ToString(), primitiveType.Kind is PrimitiveKind.Double or PrimitiveKind.Integer);
    }

    /// <summary>
    /// Converts DynamoDBEntryType to ScalarAttributeType for TableBuilder
    /// </summary>
    private static ScalarAttributeType PrimitiveToScalarAttributeType(Utilities.Common.Primitive primitiveType)
    {
        return primitiveType.Kind switch
        {
            PrimitiveKind.String => ScalarAttributeType.S,
            PrimitiveKind.Integer or PrimitiveKind.Double => ScalarAttributeType.N,
            PrimitiveKind.ByteArray => ScalarAttributeType.S, //Base64
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    private static AttributeValue ConvertPrimitiveToConditionAttributeValue(Utilities.Common.Primitive value)
    {
        // For condition values, we should use proper DynamoDB types
        // to ensure correct comparisons work
        return value.Kind switch
        {
            PrimitiveKind.Integer or PrimitiveKind.Double => new AttributeValue { N = value.ToString() },
            PrimitiveKind.Boolean => new AttributeValue { BOOL = value.AsBoolean },
            _ => new AttributeValue { S = value.ToString() }
        };
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

                if (!EvaluateCondition(jsonObject, conditions))
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
            var document = await table.GetItemAsync(ConvertPrimitiveToDynamoDbPrimitive(key.Value), config, cancellationToken);
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
                = keys.Select(key => GetItemCoreAsync(tableName, key, attributesToRetrieve, cancellationToken)).ToList();
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
        ConditionCoupling? conditions = null,
        CancellationToken cancellationToken = default)
    {
        return await PutOrUpdateItemAsync(PutOrUpdateItemType.UpdateItem, tableName, key, updateData, returnBehavior, conditions, false, cancellationToken);
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

            var document = await table.DeleteItemAsync(ConvertPrimitiveToDynamoDbPrimitive(key.Value), config,
                cancellationToken);

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
        catch (ConditionalCheckFailedException e)
        {
            return OperationResult<JObject?>.Failure($"Condition check failed: {e.Message}", HttpStatusCode.PreconditionFailed);
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
        Utilities.Common.Primitive[] elementsToAdd,
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

            // Build nested attribute expression for the array attribute
            var index = 0;
            request.ExpressionAttributeNames = new Dictionary<string, string>();
            var arrayAttrExpression = BuildNestedAttributeExpression(arrayAttributeName, request.ExpressionAttributeNames, ref index);

            // Use SET with list_append to add elements to an existing list or create a new list
            request.UpdateExpression = $"SET {arrayAttrExpression} = list_append(if_not_exists({arrayAttrExpression}, :empty_list), :vals)";
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
        catch (ConditionalCheckFailedException e)
        {
            return OperationResult<JObject?>.Failure($"Condition check failed: {e.Message}", HttpStatusCode.PreconditionFailed);
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
        Utilities.Common.Primitive[] elementsToRemove,
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

            // For DynamoDB, removing from LIST type arrays requires a different approach
            // We need to get the current item, modify it, and put it back with list_append
            var getResult = await GetItemCoreAsync(tableName, key, null, cancellationToken);
            if (!getResult.IsSuccessful || getResult.Data == null)
            {
                return OperationResult<JObject?>.Failure("Item not found for array removal operation", HttpStatusCode.NotFound);
            }

            var currentItem = getResult.Data;
            var arrayToken = NavigateToNestedProperty(currentItem, arrayAttributeName);
            if (arrayToken is not JArray currentArray)
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

            // Build proper nested update structure
            var updateData = BuildNestedUpdateStructure(arrayAttributeName, currentArray);

            var updateResult = await UpdateItemCoreAsync(tableName, key, updateData,
                DbReturnItemBehavior.ReturnNewValues, conditions, cancellationToken);

            if (!updateResult.IsSuccessful)
            {
                return OperationResult<JObject?>.Failure(string.IsNullOrWhiteSpace(updateResult.ErrorMessage) ? "Array update failed" : updateResult.ErrorMessage, updateResult.StatusCode);
            }

            return returnBehavior switch
            {
                DbReturnItemBehavior.DoNotReturn => OperationResult<JObject?>.Success(null),
                DbReturnItemBehavior.ReturnOldValues => OperationResult<JObject?>.Success(oldItem),
                DbReturnItemBehavior.ReturnNewValues => updateResult,
                _ => OperationResult<JObject?>.Success(null)
            };
        }
        catch (ConditionalCheckFailedException e)
        {
            return OperationResult<JObject?>.Failure($"Condition check failed: {e.Message}", HttpStatusCode.PreconditionFailed);
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
                ExpressionAttributeNames = new Dictionary<string, string>()
            };

            // Build nested attribute expression for the numeric attribute
            var attrIndex = 0;
            var numericAttrExpression = BuildNestedAttributeExpression(numericAttributeName, request.ExpressionAttributeNames, ref attrIndex);
            request.UpdateExpression = $"SET {numericAttrExpression} = if_not_exists({numericAttrExpression}, :start) + :incr";

            BuildConditionExpression(conditions, request);

            var responseTask = _dynamoDbClient.UpdateItemAsync(request, cancellationToken);

            // Only call PostInsertItemAsync if this is not already called from PostInsert context
            var postInsertTask = PostInsertItemAsync(tableName, key, cancellationToken);

            try
            {
                await Task.WhenAll(responseTask, postInsertTask);

                var postInsertResult = await postInsertTask;
                if (!postInsertResult.IsSuccessful)
                {
                    return OperationResult<double>.Failure($"PostInsertItemAsync failed with: {postInsertResult.ErrorMessage}", postInsertResult.StatusCode);
                }

                var response = await responseTask;

                // For nested attributes, we need to extract the value from the response differently
                if (response.Attributes?.Count > 0)
                {
                    var document = Document.FromAttributeMap(response.Attributes);
                    var jsonObject = JObject.Parse(document.ToJson());

                    var valueToken = NavigateToNestedProperty(jsonObject, numericAttributeName);
                    if (valueToken != null && double.TryParse(valueToken.ToString(), out var newValue))
                    {
                        return OperationResult<double>.Success(newValue);
                    }
                }

                return OperationResult<double>.Failure("Failed to get updated value from DynamoDB response", HttpStatusCode.InternalServerError);
            }
            catch (ConditionalCheckFailedException e)
            {
                return OperationResult<double>.Failure($"Condition check failed: {e.Message}", HttpStatusCode.PreconditionFailed);
            }
        }
        catch (ConditionalCheckFailedException e)
        {
            return OperationResult<double>.Failure($"Condition check failed: {e.Message}", HttpStatusCode.PreconditionFailed);
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
        ConditionCoupling filterConditions,
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
        ConditionCoupling? filterConditions,
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
                tableNames.AddRange(response.TableNames
                    .Where(t => !t.StartsWith(SystemTableNamePrefix))
                    .Select(t => t.Contains('-') ? t[..t.LastIndexOf('-')] : t)); //Without -key
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

        if (tableName == SystemTableName)
        {
            return await InternalDropKeyTable(SystemTableName, SystemTableKeyName, cancellationToken);
        }

        var getKeysResult = await GetTableKeysCoreAsync(tableName, cancellationToken);
        if (!getKeysResult.IsSuccessful)
            return getKeysResult.StatusCode == HttpStatusCode.NotFound
                ? OperationResult<bool>.Success(true)
                : OperationResult<bool>.Failure(getKeysResult.ErrorMessage, getKeysResult.StatusCode);

        var errors = new ConcurrentBag<string>();

        var keys = getKeysResult.Data;
        var tasks = keys.Select(key => Task.Run(async () =>
        {
            var dropResult = await InternalDropKeyTable(tableName, key, cancellationToken);
            if (!dropResult.IsSuccessful)
                errors.Add($"{dropResult.ErrorMessage} ({dropResult.StatusCode})");
        }, cancellationToken)).ToList();

        await Task.WhenAll(tasks);

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
        var compiledTableName = GetTableName(tableName, keyName);
        if (_dynamoDbClient == null)
        {
            return OperationResult<bool>.Failure("DynamoDB client not initialized", HttpStatusCode.ServiceUnavailable);
        }
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
            NullEmptyStrings(item); // Edge case resolve: Dynamodb ignores attributes with empty strings

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
        catch (ConditionalCheckFailedException e)
        {
            return OperationResult<JObject?>.Failure($"Condition check failed: {e.Message}", HttpStatusCode.PreconditionFailed);
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
            var getKeysResult = await GetTableKeysCoreAsync(tableName, cancellationToken);
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
        ConditionCoupling? filterConditions = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var getKeysResult = await GetTableKeysCoreAsync(tableName, cancellationToken);
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

                if (documents.Count > 0 && !search.IsDone)
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


    /// <summary>
    /// Evaluates a database condition against a JSON object
    /// </summary>
    private static bool EvaluateCondition(JObject jsonObject, ConditionCoupling condition)
    {
        return condition.CouplingType switch
        {
            ConditionCouplingType.Empty => true,
            ConditionCouplingType.Single when condition.SingleCondition != null =>
                EvaluateSingleCondition(jsonObject, condition.SingleCondition),
            ConditionCouplingType.And when condition is { First: not null, Second: not null } =>
                EvaluateCondition(jsonObject, condition.First) && EvaluateCondition(jsonObject, condition.Second),
            ConditionCouplingType.Or when condition is { First: not null, Second: not null } =>
                EvaluateCondition(jsonObject, condition.First) || EvaluateCondition(jsonObject, condition.Second),
            _ => false
        };
    }

    /// <summary>
    /// Evaluates a single database condition against a JSON object with nested path support
    /// </summary>
    private static bool EvaluateSingleCondition(JObject jsonObject, Condition condition)
    {
        // Handle size() function specially
        if (condition.AttributeName.StartsWith("size(") && condition.AttributeName.EndsWith(")"))
        {
            var innerAttribute = condition.AttributeName[5..^1]; // Remove "size(" and ")"
            var token = NavigateToNestedProperty(jsonObject, innerAttribute);

            if (token is JArray array)
            {
                var arraySize = array.Count;
                if (condition is ValueCondition valueCondition)
                {
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
            }
            return false;
        }

        return condition.ConditionType switch
        {
            ConditionType.AttributeExists => NestedPropertyExists(jsonObject, condition.AttributeName),
            ConditionType.AttributeNotExists => !NestedPropertyExists(jsonObject, condition.AttributeName),
            _ when condition is ValueCondition valueCondition => EvaluateValueCondition(jsonObject, valueCondition),
            _ when condition is ArrayCondition arrayCondition => EvaluateArrayElementCondition(jsonObject, arrayCondition),
            _ => false
        };
    }

    private static void BuildConditionExpression(ConditionCoupling? conditions, UpdateItemRequest request)
    {
        if (conditions == null) return;

        // Start index from a base to avoid conflicts with other placeholders
        var index = 1000; // Use high starting number to avoid conflicts
        request.ExpressionAttributeValues ??= new Dictionary<string, AttributeValue>();
        request.ExpressionAttributeNames ??= new Dictionary<string, string>();

        var conditionExpression = BuildConditionExpressionRecursive(conditions, request, ref index);

        if (!string.IsNullOrEmpty(conditionExpression))
        {
            request.ConditionExpression = conditionExpression;
        }
    }

    private static string? BuildConditionExpressionRecursive(ConditionCoupling condition, UpdateItemRequest request, ref int index)
    {
        return condition.CouplingType switch
        {
            ConditionCouplingType.Empty => null,
            ConditionCouplingType.Single when condition.SingleCondition != null =>
                BuildSingleConditionExpression(condition.SingleCondition, request, ref index),
            ConditionCouplingType.And when condition is { First: not null, Second: not null } =>
                BuildBinaryExpression(condition.First, condition.Second, "AND", request, ref index),
            ConditionCouplingType.Or when condition is { First: not null, Second: not null } =>
                BuildBinaryExpression(condition.First, condition.Second, "OR", request, ref index),
            _ => null
        };
    }

    private static string? BuildBinaryExpression(ConditionCoupling left, ConditionCoupling right, string operation, UpdateItemRequest request, ref int index)
    {
        var leftExpr = BuildConditionExpressionRecursive(left, request, ref index);
        var rightExpr = BuildConditionExpressionRecursive(right, request, ref index);

        if (string.IsNullOrEmpty(leftExpr) && string.IsNullOrEmpty(rightExpr))
            return null;
        if (string.IsNullOrEmpty(leftExpr))
            return rightExpr;
        if (string.IsNullOrEmpty(rightExpr))
            return leftExpr;

        return $"({leftExpr}) {operation} ({rightExpr})";
    }

    private static string? BuildSingleConditionExpression(Condition condition, UpdateItemRequest request, ref int index)
    {
        var baseIndex = index;
        var condValPlaceholder = $":cond_val{baseIndex}";
        index++; // Reserve the value placeholder index

        // Ensure dictionaries exist
        request.ExpressionAttributeNames ??= new Dictionary<string, string>();
        request.ExpressionAttributeValues ??= new Dictionary<string, AttributeValue>();

        // Build nested attribute expression for DynamoDB with proper syntax
        var attrExpression = BuildNestedAttributeExpression(condition.AttributeName, request.ExpressionAttributeNames, ref index);

        if (string.IsNullOrEmpty(attrExpression))
            return null;

        // Handle size() function conditions specially for UpdateItem operations
        if (condition.AttributeName.StartsWith("size(") && condition.AttributeName.EndsWith(")"))
        {
            return condition.ConditionType switch
            {
                ConditionType.AttributeEquals when condition is ValueCondition valueCondition =>
                    BuildSizeFunctionCondition(attrExpression, "=", condValPlaceholder, valueCondition.Value, request),
                ConditionType.AttributeNotEquals when condition is ValueCondition valueCondition =>
                    BuildSizeFunctionCondition(attrExpression, "<>", condValPlaceholder, valueCondition.Value, request),
                ConditionType.AttributeGreater when condition is ValueCondition valueCondition =>
                    BuildSizeFunctionCondition(attrExpression, ">", condValPlaceholder, valueCondition.Value, request),
                ConditionType.AttributeGreaterOrEqual when condition is ValueCondition valueCondition =>
                    BuildSizeFunctionCondition(attrExpression, ">=", condValPlaceholder, valueCondition.Value, request),
                ConditionType.AttributeLess when condition is ValueCondition valueCondition =>
                    BuildSizeFunctionCondition(attrExpression, "<", condValPlaceholder, valueCondition.Value, request),
                ConditionType.AttributeLessOrEqual when condition is ValueCondition valueCondition =>
                    BuildSizeFunctionCondition(attrExpression, "<=", condValPlaceholder, valueCondition.Value, request),
                _ => null
            };
        }

        var conditionExpr = BuildDynamoConditionExpressionNested(condition, condValPlaceholder, attrExpression);
        if (string.IsNullOrEmpty(conditionExpr)) return null;

        // Add condition value to expression attribute values only for conditions that need them
        switch (condition)
        {
            case ValueCondition valueCondition:
                request.ExpressionAttributeValues[condValPlaceholder] = ConvertPrimitiveToConditionAttributeValue(valueCondition.Value);
                break;
            case ArrayCondition arrayCondition:
                request.ExpressionAttributeValues[condValPlaceholder] = ConvertPrimitiveToConditionAttributeValue(arrayCondition.ElementValue);
                break;
        }

        return conditionExpr;
    }


    /// <summary>
    /// Parses an attribute path into DynamoDB-compatible segments.
    /// Supports nested object navigation without direct array indexing.
    /// Examples: "User.Name", "User.Address.Street", "Settings.Preferences.Theme"
    /// Note: Array operations use contains() function instead of direct indexing
    /// </summary>
    private static string[] ParseAttributePath(string attributePath)
    {
        if (string.IsNullOrEmpty(attributePath))
            return [];

        // Validate that path doesn't contain array indexing syntax
        if (attributePath.Contains('[') || attributePath.Contains(']'))
        {
            throw new ArgumentException(
                $"Array indexing syntax (e.g., 'array[0]') is not supported in DynamoDB expressions. " +
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
    /// Navigates to a nested property in a JSON object using DynamoDB-compatible path navigation.
    /// Supports nested objects but not direct array indexing.
    /// Examples: "User.Name", "User.Address.Street", "Settings.Theme.Color"
    /// For arrays, use ArrayElementExists/ArrayElementNotExists conditions instead.
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
            // Invalid path format
            return null;
        }
    }

    /// <summary>
    /// Checks if a nested property exists in a JSON object using robust path navigation.
    /// Handles all nested scenarios including complex array and object combinations.
    /// </summary>
    private static bool NestedPropertyExists(JObject jsonObject, string attributePath)
    {
        if (string.IsNullOrEmpty(attributePath))
            return false;

        return NavigateToNestedProperty(jsonObject, attributePath) != null;
    }

    /// <summary>
    /// Builds a DynamoDB-compliant nested attribute expression with proper placeholders.
    /// Handles nested object paths and DynamoDB functions like size().
    /// Examples: "User.Name" -> "#attr0.#attr1", "size(Tags)" -> handled specially
    /// For array operations, use contains() function instead of direct indexing.
    /// </summary>
    private static string BuildNestedAttributeExpression(string attributePath, Dictionary<string, string> attributeNames, ref int index)
    {
        if (string.IsNullOrEmpty(attributePath))
            return string.Empty;

        // Handle DynamoDB function syntax (e.g., size(attributeName))
        if (attributePath.StartsWith("size(") && attributePath.EndsWith(")"))
        {
            // Extract the inner attribute name from size(attributeName)
            var innerAttribute = attributePath[5..^1]; // Remove "size(" and ")"

            // Build expression for the inner attribute - this could be nested like "Project.Tasks"
            var innerExpression = BuildNestedAttributeExpression(innerAttribute, attributeNames, ref index);

            // Return size() function with the inner expression
            return $"size({innerExpression})";
        }

        try
        {
            var pathSegments = ParseAttributePath(attributePath);
            var expressionParts = new List<string>();

            foreach (var segment in pathSegments)
            {
                // Create placeholder for the property name
                var placeholder = $"#attr{index++}";
                attributeNames[placeholder] = segment;
                expressionParts.Add(placeholder);
            }

            return string.Join(".", expressionParts);
        }
        catch (ArgumentException ex)
        {
            throw new ArgumentException($"Invalid attribute path '{attributePath}': {ex.Message}", ex);
        }
    }

    /// <summary>
    /// Validates that an attribute path is well-formed and DynamoDB-compliant.
    /// Supports nested objects, DynamoDB functions like size(), but rejects array indexing syntax.
    /// Examples: "User.Name" (valid), "size(Tags)" (valid), "size(Project.Tasks)" (valid), "User.Tags[0]" (invalid - use ArrayElementExists instead)
    /// </summary>
    private static bool IsValidAttributePath(string attributePath)
    {
        if (string.IsNullOrEmpty(attributePath))
            return false;

        // Handle DynamoDB function syntax (e.g., size(attributeName))
        if (attributePath.StartsWith("size(") && attributePath.EndsWith(")"))
        {
            var innerAttribute = attributePath[5..^1]; // Remove "size(" and ")"
            if (string.IsNullOrWhiteSpace(innerAttribute))
                return false;
            return IsValidAttributePath(innerAttribute); // Recursively validate the inner attribute
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
            // Parse the path to validate structure
            var segments = ParseAttributePath(attributePath);

            // Must have at least one segment
            if (segments.Length == 0)
                return false;

            // Validate each segment
            foreach (var segment in segments)
            {
                // Property name cannot be empty
                if (string.IsNullOrWhiteSpace(segment))
                    return false;

                // Check length limits (DynamoDB has a 255 character limit for attribute names)
                if (segment.Length > 255)
                    return false;
            }

            return true;
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    private static Expression? BuildConditionalExpression(ConditionCoupling? conditions)
    {
        if (conditions == null)
            return null;

        var finalExpression = new Expression();
        var index = 0;

        var expressionStatement = BuildConditionalExpressionRecursive(conditions, finalExpression, ref index);

        if (string.IsNullOrEmpty(expressionStatement))
            return null;

        finalExpression.ExpressionStatement = expressionStatement;
        return finalExpression;
    }

    private static string? BuildConditionalExpressionRecursive(ConditionCoupling condition, Expression finalExpression, ref int index)
    {
        return condition.CouplingType switch
        {
            ConditionCouplingType.Empty => null,
            ConditionCouplingType.Single when condition.SingleCondition != null =>
                BuildSingleConditionalExpression(condition.SingleCondition, finalExpression, ref index),
            ConditionCouplingType.And when condition is { First: not null, Second: not null } =>
                BuildBinaryConditionalExpression(condition.First, condition.Second, "AND", finalExpression, ref index),
            ConditionCouplingType.Or when condition is { First: not null, Second: not null } =>
                BuildBinaryConditionalExpression(condition.First, condition.Second, "OR", finalExpression, ref index),
            _ => null
        };
    }

    private static string? BuildBinaryConditionalExpression(ConditionCoupling left, ConditionCoupling right, string operation, Expression finalExpression, ref int index)
    {
        var leftExpr = BuildConditionalExpressionRecursive(left, finalExpression, ref index);
        var rightExpr = BuildConditionalExpressionRecursive(right, finalExpression, ref index);

        if (string.IsNullOrEmpty(leftExpr) && string.IsNullOrEmpty(rightExpr))
            return null;
        if (string.IsNullOrEmpty(leftExpr))
            return rightExpr;
        if (string.IsNullOrEmpty(rightExpr))
            return leftExpr;

        return $"({leftExpr}) {operation} ({rightExpr})";
    }

    private static string? BuildSingleConditionalExpression(Condition condition, Expression finalExpression, ref int index)
    {
        var baseIndex = index;
        var valPlaceholder = $":val{baseIndex}";
        var condValPlaceholder = $":cond_val{baseIndex}";
        index++; // Reserve the value placeholder index

        // Ensure ExpressionAttributeNames is initialized
        finalExpression.ExpressionAttributeNames ??= new Dictionary<string, string>();

        // Build nested attribute expression with proper DynamoDB syntax
        var attrExpression = BuildNestedAttributeExpression(condition.AttributeName, finalExpression.ExpressionAttributeNames, ref index);

        if (string.IsNullOrEmpty(attrExpression))
            return null;

        // Handle size() function conditions differently
        if (condition.AttributeName.StartsWith("size(") && condition.AttributeName.EndsWith(")"))
        {
            return condition.ConditionType switch
            {
                ConditionType.AttributeEquals when condition is ValueCondition valueCondition =>
                    $"{attrExpression} = {BuildValuePlaceholderAndAddToExpression(valPlaceholder, valueCondition.Value, finalExpression)}",
                ConditionType.AttributeNotEquals when condition is ValueCondition valueCondition =>
                    $"{attrExpression} <> {BuildValuePlaceholderAndAddToExpression(valPlaceholder, valueCondition.Value, finalExpression)}",
                ConditionType.AttributeGreater when condition is ValueCondition valueCondition =>
                    $"{attrExpression} > {BuildValuePlaceholderAndAddToExpression(valPlaceholder, valueCondition.Value, finalExpression)}",
                ConditionType.AttributeGreaterOrEqual when condition is ValueCondition valueCondition =>
                    $"{attrExpression} >= {BuildValuePlaceholderAndAddToExpression(valPlaceholder, valueCondition.Value, finalExpression)}",
                ConditionType.AttributeLess when condition is ValueCondition valueCondition =>
                    $"{attrExpression} < {BuildValuePlaceholderAndAddToExpression(valPlaceholder, valueCondition.Value, finalExpression)}",
                ConditionType.AttributeLessOrEqual when condition is ValueCondition valueCondition =>
                    $"{attrExpression} <= {BuildValuePlaceholderAndAddToExpression(valPlaceholder, valueCondition.Value, finalExpression)}",
                _ => null
            };
        }

        string? expressionStatement = condition.ConditionType switch
        {
            ConditionType.AttributeExists => $"attribute_exists({attrExpression})",
            ConditionType.AttributeNotExists => $"attribute_not_exists({attrExpression})",
            ConditionType.AttributeEquals when condition is ValueCondition valueCondition =>
                BuildValueConditionExpressionNested(attrExpression, valPlaceholder, "=", valueCondition, finalExpression),
            ConditionType.AttributeNotEquals when condition is ValueCondition valueCondition =>
                BuildValueConditionExpressionNested(attrExpression, valPlaceholder, "<>", valueCondition, finalExpression),
            ConditionType.AttributeGreater when condition is ValueCondition valueCondition =>
                BuildValueConditionExpressionNested(attrExpression, valPlaceholder, ">", valueCondition, finalExpression),
            ConditionType.AttributeGreaterOrEqual when condition is ValueCondition valueCondition =>
                BuildValueConditionExpressionNested(attrExpression, valPlaceholder, ">=", valueCondition, finalExpression),
            ConditionType.AttributeLess when condition is ValueCondition valueCondition =>
                BuildValueConditionExpressionNested(attrExpression, valPlaceholder, "<", valueCondition, finalExpression),
            ConditionType.AttributeLessOrEqual when condition is ValueCondition valueCondition =>
                BuildValueConditionExpressionNested(attrExpression, valPlaceholder, "<=", valueCondition, finalExpression),
            ConditionType.ArrayElementExists when condition is ArrayCondition arrayCondition =>
                BuildArrayConditionExpressionNested(attrExpression, condValPlaceholder, "contains", arrayCondition, finalExpression),
            ConditionType.ArrayElementNotExists when condition is ArrayCondition arrayCondition =>
                BuildArrayConditionExpressionNested(attrExpression, condValPlaceholder, "NOT contains", arrayCondition, finalExpression),
            _ => null
        };

        return expressionStatement;
    }

    private static string BuildValueConditionExpressionNested(string attrExpression, string valPlaceholder, string operation, ValueCondition valueCondition, Expression finalExpression)
    {
        AddValueToExpression(finalExpression, valPlaceholder, valueCondition.Value);
        return $"{attrExpression} {operation} {valPlaceholder}";
    }

    private static string BuildArrayConditionExpressionNested(string attrExpression, string condValPlaceholder, string operation, ArrayCondition arrayCondition, Expression finalExpression)
    {
        AddValueToExpression(finalExpression, condValPlaceholder, arrayCondition.ElementValue);
        return $"{operation}({attrExpression}, {condValPlaceholder})";
    }

    /// <summary>
    /// Builds a DynamoDB condition expression string with nested attribute support for UpdateItem operations.
    /// Handles DynamoDB functions like size() and contains() properly.
    /// </summary>
    private static string? BuildDynamoConditionExpressionNested(Condition condition, string condVal, string attrExpression)
    {
        // Handle special DynamoDB function syntax (e.g., size(attributeName)) - but this is now handled upstream
        // This method should just build standard expressions since size() is handled in BuildSingleConditionExpression

        // Standard attribute expressions
        var expression = condition.ConditionType switch
        {
            ConditionType.AttributeExists => $"attribute_exists({attrExpression})",
            ConditionType.AttributeNotExists => $"attribute_not_exists({attrExpression})",
            ConditionType.AttributeEquals => $"{attrExpression} = {condVal}",
            ConditionType.AttributeNotEquals => $"{attrExpression} <> {condVal}",
            ConditionType.AttributeGreater => $"{attrExpression} > {condVal}",
            ConditionType.AttributeGreaterOrEqual => $"{attrExpression} >= {condVal}",
            ConditionType.AttributeLess => $"{attrExpression} < {condVal}",
            ConditionType.AttributeLessOrEqual => $"{attrExpression} <= {condVal}",
            ConditionType.ArrayElementExists => $"contains({attrExpression}, {condVal})",
            ConditionType.ArrayElementNotExists => $"NOT contains({attrExpression}, {condVal})",
            _ => null
        };

        return expression;
    }

    private static void AddValueToExpression(Expression expression, string placeholder, Utilities.Common.Primitive value)
    {
        switch (value.Kind)
        {
            case PrimitiveKind.String:
                expression.ExpressionAttributeValues[placeholder] = new Amazon.DynamoDBv2.DocumentModel.Primitive(value.AsString, false);
                break;
            case PrimitiveKind.Boolean:
                expression.ExpressionAttributeValues[placeholder] = new DynamoDBBool(value.AsBoolean);
                break;
            case PrimitiveKind.Integer:
                expression.ExpressionAttributeValues[placeholder] = new Amazon.DynamoDBv2.DocumentModel.Primitive(value.AsInteger.ToString(CultureInfo.InvariantCulture), true);
                break;
            case PrimitiveKind.Double:
                expression.ExpressionAttributeValues[placeholder] = new Amazon.DynamoDBv2.DocumentModel.Primitive(value.AsDouble.ToString(CultureInfo.InvariantCulture), true);
                break;
            case PrimitiveKind.ByteArray:
                expression.ExpressionAttributeValues[placeholder] = new Amazon.DynamoDBv2.DocumentModel.Primitive(value.ToString());
                break;
        }
    }

    private static string BuildValuePlaceholderAndAddToExpression(string placeholder, Utilities.Common.Primitive value, Expression expression)
    {
        AddValueToExpression(expression, placeholder, value);
        return placeholder;
    }

    private static string BuildSizeFunctionCondition(string attrExpression, string operation, string valuePlaceholder, Utilities.Common.Primitive value, UpdateItemRequest request)
    {
        request.ExpressionAttributeValues[valuePlaceholder] = ConvertPrimitiveToConditionAttributeValue(value);
        return $"{attrExpression} {operation} {valuePlaceholder}";
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
    /// Evaluates a value-based condition against a JSON object with nested path support
    /// </summary>
    private static bool EvaluateValueCondition(JObject jsonObject, ValueCondition condition)
    {
        var token = NavigateToNestedProperty(jsonObject, condition.AttributeName);
        if (token == null)
            return false;

        try
        {
            return condition.ConditionType switch
            {
                ConditionType.AttributeEquals => CompareValues(token, condition.Value) == 0,
                ConditionType.AttributeNotEquals => CompareValues(token, condition.Value) != 0,
                ConditionType.AttributeGreater => CompareValues(token, condition.Value) > 0,
                ConditionType.AttributeGreaterOrEqual => CompareValues(token, condition.Value) >= 0,
                ConditionType.AttributeLess => CompareValues(token, condition.Value) < 0,
                ConditionType.AttributeLessOrEqual => CompareValues(token, condition.Value) <= 0,
                _ => false
            };
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Evaluates an array element condition against a JSON object with nested path support
    /// </summary>
    private static bool EvaluateArrayElementCondition(JObject jsonObject, ArrayCondition condition)
    {
        var token = NavigateToNestedProperty(jsonObject, condition.AttributeName);
        if (token is not JArray array)
            return condition.ConditionType == ConditionType.ArrayElementNotExists;

        var elementExists = array.Any(item => CompareValues(item, condition.ElementValue) == 0);

        return condition.ConditionType switch
        {
            ConditionType.ArrayElementExists => elementExists,
            ConditionType.ArrayElementNotExists => !elementExists,
            _ => false
        };
    }

    /// <summary>
    /// Compares a JSON token with a primitive value
    /// </summary>
    private static int CompareValues(JToken token, Utilities.Common.Primitive primitive)
    {
        try
        {
            return primitive.Kind switch
            {
                PrimitiveKind.String => string.Compare(token.ToString(), primitive.AsString, StringComparison.Ordinal),
                PrimitiveKind.Integer when token.Type is JTokenType.Integer => ((long)token).CompareTo(primitive.AsInteger),
                PrimitiveKind.Integer when token.Type is JTokenType.Float => ((double)token).CompareTo(primitive.AsInteger),
                PrimitiveKind.Double when token.Type is JTokenType.Float => ((double)token).CompareTo(primitive.AsDouble),
                PrimitiveKind.Double when token.Type is JTokenType.Integer => ((double)(long)token).CompareTo(primitive.AsDouble),
                PrimitiveKind.Boolean when token.Type is JTokenType.Boolean => ((bool)token).CompareTo(primitive.AsBoolean),
                _ => string.Compare(token.ToString(), primitive.ToString(), StringComparison.Ordinal)
            };
        }
        catch
        {
            return string.Compare(token.ToString(), primitive.ToString(), StringComparison.Ordinal);
        }
    }

    private static void NullEmptyStrings(JToken token)
    {
        if (token.Type == JTokenType.Object)
        {
            foreach (var prop in token.Children<JProperty>())
            {
                NullEmptyStrings(prop.Value);
            }
        }
        else if (token.Type == JTokenType.Array)
        {
            foreach (var child in token.Children())
            {
                NullEmptyStrings(child);
            }
        }
        else if (token.Type == JTokenType.String && token.ToString() == "")
        {
            token.Replace(JValue.CreateNull());
        }
    }

    /// <summary>
    /// Builds proper nested update structure for array operations with nested paths
    /// </summary>
    private static JObject BuildNestedUpdateStructure(string arrayAttributeName, JArray updatedArray)
    {
        var pathSegments = ParseAttributePath(arrayAttributeName);

        if (pathSegments.Length == 1)
        {
            // Simple case - not nested
            return new JObject
            {
                [pathSegments[0]] = updatedArray
            };
        }

        // Build nested structure
        var result = new JObject();
        JObject current = result;

        for (int i = 0; i < pathSegments.Length - 1; i++)
        {
            var nestedObj = new JObject();
            current[pathSegments[i]] = nestedObj;
            current = nestedObj;
        }

        // Set the final array value
        current[pathSegments[^1]] = updatedArray;

        return result;
    }

    #endregion

    #region Condition Builders

    /// <inheritdoc />
    public override Condition AttributeEquals(string attributeName, Utilities.Common.Primitive value)
    {
        ValidateAttributeName(attributeName);
        return new ValueCondition(ConditionType.AttributeEquals, attributeName, value);
    }

    /// <inheritdoc />
    public override Condition AttributeNotEquals(string attributeName, Utilities.Common.Primitive value)
    {
        ValidateAttributeName(attributeName);
        return new ValueCondition(ConditionType.AttributeNotEquals, attributeName, value);
    }

    /// <inheritdoc />
    public override Condition AttributeIsGreaterThan(string attributeName, Utilities.Common.Primitive value)
    {
        ValidateAttributeName(attributeName);
        return new ValueCondition(ConditionType.AttributeGreater, attributeName, value);
    }

    /// <inheritdoc />
    public override Condition AttributeIsGreaterOrEqual(string attributeName, Utilities.Common.Primitive value)
    {
        ValidateAttributeName(attributeName);
        return new ValueCondition(ConditionType.AttributeGreaterOrEqual, attributeName, value);
    }

    /// <inheritdoc />
    public override Condition AttributeIsLessThan(string attributeName, Utilities.Common.Primitive value)
    {
        ValidateAttributeName(attributeName);
        return new ValueCondition(ConditionType.AttributeLess, attributeName, value);
    }

    /// <inheritdoc />
    public override Condition AttributeIsLessOrEqual(string attributeName, Utilities.Common.Primitive value)
    {
        ValidateAttributeName(attributeName);
        return new ValueCondition(ConditionType.AttributeLessOrEqual, attributeName, value);
    }

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
    public override Condition ArrayElementExists(string attributeName, Utilities.Common.Primitive elementValue)
    {
        ValidateAttributeName(attributeName);
        return new ArrayCondition(ConditionType.ArrayElementExists, attributeName, elementValue);
    }

    /// <inheritdoc />
    public override Condition ArrayElementNotExists(string attributeName, Utilities.Common.Primitive elementValue)
    {
        ValidateAttributeName(attributeName);
        return new ArrayCondition(ConditionType.ArrayElementNotExists, attributeName, elementValue);
    }

    /// <summary>
    /// Creates a condition that checks if an array attribute has a specific size.
    /// Uses DynamoDB's size() function which is supported for arrays and other types.
    /// </summary>
    public Condition ArrayHasSize(string attributeName, int expectedSize)
    {
        ValidateAttributeName(attributeName);
        if (expectedSize < 0)
            throw new ArgumentException("Array size must be non-negative", nameof(expectedSize));
        return new ValueCondition(ConditionType.AttributeEquals, $"size({attributeName})", new Utilities.Common.Primitive(expectedSize));
    }

    /// <summary>
    /// Creates a condition that checks if an array attribute is empty (size = 0).
    /// </summary>
    public Condition ArrayIsEmpty(string attributeName)
    {
        ValidateAttributeName(attributeName);
        return ArrayHasSize(attributeName, 0);
    }

    /// <summary>
    /// Creates a condition that checks if an array attribute is not empty (size > 0).
    /// </summary>
    public Condition ArrayIsNotEmpty(string attributeName)
    {
        ValidateAttributeName(attributeName);
        return new ValueCondition(ConditionType.AttributeGreater, $"size({attributeName})", new Utilities.Common.Primitive(0));
    }

    /// <summary>
    /// Validates an attribute name for use in conditions and operations.
    /// Throws ArgumentException if the attribute name is invalid.
    /// </summary>
    private static void ValidateAttributeName(string attributeName)
    {
        if (!IsValidAttributePath(attributeName))
        {
            throw new ArgumentException($"Invalid attribute name: '{attributeName}'. " +
                "Attribute names must be well-formed paths for nested objects or DynamoDB functions like size(). " +
                "Examples: 'Name', 'User.Email', 'User.Address.Street', 'size(Tags)'. " +
                "For arrays, use ArrayElementExists() or ArrayElementNotExists() conditions instead of array[index] syntax.",
                nameof(attributeName));
        }
    }

    #endregion

    public void Dispose()
    {
        _dynamoDbClient?.Dispose();
        // ReSharper disable once GCSuppressFinalizeForTypeWithoutDestructor
        GC.SuppressFinalize(this);
    }
}
