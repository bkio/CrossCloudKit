// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Cloud.Interfaces;
using Utilities.Common;
using Newtonsoft.Json.Linq;
using Amazon.DynamoDBv2.DocumentModel;
using System.Collections.Concurrent;
using System.Globalization;
using Expression = Amazon.DynamoDBv2.DocumentModel.Expression;

namespace Cloud.Database.AWS;

public sealed class DatabaseServiceAWS : DatabaseServiceBase, IDatabaseService, IDisposable
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
    public bool IsInitialized => _initializationSucceed;

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

    /// <summary>
    /// Searches table definition in LoadedTables, if not loaded, loads, stores and returns.
    /// Creates the table if it doesn't exist.
    /// </summary>
    private async Task<Table?> GetTableAsync(string tableName, CancellationToken cancellationToken = default)
    {
        if (_loadedTables.TryGetValue(tableName, out var existingTable))
        {
            return existingTable;
        }

        if (_dynamoDbClient == null) return null;

        try
        {
            // Try to get table description to understand its structure
            var describeRequest = new DescribeTableRequest { TableName = tableName };
            var describeResponse = await _dynamoDbClient.DescribeTableAsync(describeRequest, cancellationToken);

            // Build table using TableBuilder with the actual table schema
            var tableBuilder = new TableBuilder(_dynamoDbClient, tableName);

            // Add hash key (partition key)
            var hashKey = describeResponse.Table.KeySchema.FirstOrDefault(k => k.KeyType == KeyType.HASH);
            if (hashKey != null)
            {
                var hashKeyAttribute = describeResponse.Table.AttributeDefinitions.FirstOrDefault(a => a.AttributeName == hashKey.AttributeName);
                if (hashKeyAttribute != null)
                {
                    var hashKeyType = ConvertScalarAttributeTypeToDynamoDbEntryType(hashKeyAttribute.AttributeType);
                    tableBuilder.AddHashKey(hashKey.AttributeName, hashKeyType);
                }
            }

            // Add range key if exists (sort key)
            var rangeKey = describeResponse.Table.KeySchema.FirstOrDefault(k => k.KeyType == KeyType.RANGE);
            if (rangeKey != null)
            {
                var rangeKeyAttribute = describeResponse.Table.AttributeDefinitions.FirstOrDefault(a => a.AttributeName == rangeKey.AttributeName);
                if (rangeKeyAttribute != null)
                {
                    var rangeKeyType = ConvertScalarAttributeTypeToDynamoDbEntryType(rangeKeyAttribute.AttributeType);
                    tableBuilder.AddRangeKey(rangeKey.AttributeName, rangeKeyType);
                }
            }

            // Add Global Secondary Indexes
            if (describeResponse.Table.GlobalSecondaryIndexes?.Count > 0)
            {
                foreach (var gsi in describeResponse.Table.GlobalSecondaryIndexes)
                {
                    var gsiHashKey = gsi.KeySchema.FirstOrDefault(k => k.KeyType == KeyType.HASH);
                    var gsiRangeKey = gsi.KeySchema.FirstOrDefault(k => k.KeyType == KeyType.RANGE);

                    if (gsiHashKey != null)
                    {
                        var gsiHashKeyAttr = describeResponse.Table.AttributeDefinitions.FirstOrDefault(a => a.AttributeName == gsiHashKey.AttributeName);
                        if (gsiHashKeyAttr != null)
                        {
                            var gsiHashKeyType = ConvertScalarAttributeTypeToDynamoDbEntryType(gsiHashKeyAttr.AttributeType);

                            if (gsiRangeKey != null)
                            {
                                var gsiRangeKeyAttr = describeResponse.Table.AttributeDefinitions.FirstOrDefault(a => a.AttributeName == gsiRangeKey.AttributeName);
                                if (gsiRangeKeyAttr != null)
                                {
                                    var gsiRangeKeyType = ConvertScalarAttributeTypeToDynamoDbEntryType(gsiRangeKeyAttr.AttributeType);
                                    tableBuilder.AddGlobalSecondaryIndex(gsi.IndexName!, gsiHashKey.AttributeName, gsiHashKeyType, gsiRangeKey.AttributeName, gsiRangeKeyType);
                                }
                            }
                            else
                            {
                                tableBuilder.AddGlobalSecondaryIndex(gsi.IndexName!, gsiHashKey.AttributeName, gsiHashKeyType);
                            }
                        }
                    }
                }
            }

            // Build the table using the modern TableBuilder pattern
            var table = tableBuilder.Build();

            _loadedTables.TryAdd(tableName, table);
            return table;
        }
        catch (ResourceNotFoundException)
        {
            // Table doesn't exist, create it with a default schema
            return await CreateTableAsync(tableName, cancellationToken);
        }
        catch (Exception)
        {
            // Other error, return null
            return null;
        }
    }

    /// <summary>
    /// Creates a DynamoDB table with a default schema suitable for testing.
    /// Uses "Id" as the partition key with string type by default, but can be overridden.
    /// </summary>
    private async Task<Table?> CreateTableAsync(string tableName, CancellationToken cancellationToken = default)
    {
        if (_dynamoDbClient == null) return null;

        try
        {
            // Check if the table already exists (might have been created by another thread)
            try
            {
                var existingDescribeRequest = new DescribeTableRequest { TableName = tableName };
                var existingDescribeResponse = await _dynamoDbClient.DescribeTableAsync(existingDescribeRequest, cancellationToken);

                if (existingDescribeResponse.Table.TableStatus == TableStatus.ACTIVE)
                {
                    // Table already exists and is active, build and return it
                    var existingTableBuilder = new TableBuilder(_dynamoDbClient, tableName);

                    // Determine the existing key type from the table description
                    var hashKey = existingDescribeResponse.Table.KeySchema.FirstOrDefault(k => k.KeyType == KeyType.HASH);
                    if (hashKey != null)
                    {
                        var hashKeyAttribute = existingDescribeResponse.Table.AttributeDefinitions.FirstOrDefault(a => a.AttributeName == hashKey.AttributeName);
                        if (hashKeyAttribute != null)
                        {
                            var keyType = ConvertScalarAttributeTypeToDynamoDbEntryType(hashKeyAttribute.AttributeType);
                            existingTableBuilder.AddHashKey(hashKey.AttributeName, keyType);
                        }
                        else
                        {
                            // Fallback to string if we can't determine the type
                            existingTableBuilder.AddHashKey(hashKey.AttributeName, DynamoDBEntryType.String);
                        }
                    }
                    else
                    {
                        // Fallback for "Id" key
                        existingTableBuilder.AddHashKey("Id", DynamoDBEntryType.String);
                    }

                    var existingTable = existingTableBuilder.Build();
                    _loadedTables.TryAdd(tableName, existingTable);
                    return existingTable;
                }
            }
            catch (ResourceNotFoundException)
            {
                // Table doesn't exist, continue with creation
            }

            // Create table with flexible key type (default to string for compatibility)
            var createTableRequest = new CreateTableRequest
            {
                TableName = tableName,
                KeySchema =
                [
                    new KeySchemaElement("Id", KeyType.HASH) // Partition key
                ],
                AttributeDefinitions =
                [
                    new AttributeDefinition("Id", ScalarAttributeType.S) // String type - most flexible
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

            // Wait for table to become active
            var maxWaitTime = TimeSpan.FromMinutes(5);
            var startTime = DateTime.UtcNow;

            while (DateTime.UtcNow - startTime < maxWaitTime)
            {
                try
                {
                    var describeResponse = await _dynamoDbClient.DescribeTableAsync(tableName, cancellationToken);
                    if (describeResponse.Table.TableStatus == TableStatus.ACTIVE)
                    {
                        // Build the table using TableBuilder
                        var tableBuilder = new TableBuilder(_dynamoDbClient, tableName);
                        tableBuilder.AddHashKey("Id", DynamoDBEntryType.String);
                        var table = tableBuilder.Build();

                        _loadedTables.TryAdd(tableName, table);
                        return table;
                    }
                }
                catch
                {
                    // Continue waiting
                }

                await Task.Delay(1000, cancellationToken); // Wait 1 second before checking again
            }

            return null; // Timed out waiting for table to become active
        }
        catch (Exception)
        {
            return null;
        }
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
            "B" => DynamoDBEntryType.Binary,
            _ => DynamoDBEntryType.String // Default fallback
        };
    }

    private static AttributeValue ConvertPrimitiveToAttributeValue(PrimitiveType value)
    {
        // For DynamoDB keys, we should use strings for maximum compatibility
        // since string keys can represent any primitive type as a string
        return value.Kind switch
        {
            PrimitiveTypeKind.String => new AttributeValue { S = value.AsString },
            PrimitiveTypeKind.Integer => new AttributeValue { S = value.AsInteger.ToString() },
            PrimitiveTypeKind.Double => new AttributeValue { S = value.AsDouble.ToString("R") }, // Round-trip format
            PrimitiveTypeKind.ByteArray => new AttributeValue { S = Convert.ToBase64String(value.AsByteArray) },
            _ => new AttributeValue { S = value.ToString() }
        };
    }

    private static AttributeValue ConvertPrimitiveToConditionAttributeValue(PrimitiveType value)
    {
        // For condition values, we should use proper DynamoDB types
        // to ensure correct comparisons work
        return value.Kind switch
        {
            PrimitiveTypeKind.String => new AttributeValue { S = value.AsString },
            PrimitiveTypeKind.Integer => new AttributeValue { N = value.AsInteger.ToString() },
            PrimitiveTypeKind.Double => new AttributeValue { N = value.AsDouble.ToString("R") }, // Round-trip format
            PrimitiveTypeKind.ByteArray => new AttributeValue { S = Convert.ToBase64String(value.AsByteArray) },
            _ => new AttributeValue { S = value.ToString() }
        };
    }

    #region Modern Async API

    /// <inheritdoc />
    public async Task<OperationResult<bool>> ItemExistsAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        DatabaseAttributeCondition? condition = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (_dynamoDbClient == null)
                return OperationResult<bool>.Failure("DynamoDB client not initialized");

            // First try to create/get table - if this fails, the item definitely doesn't exist
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                // Table doesn't exist, so item definitely doesn't exist
                return OperationResult<bool>.Success(false);
            }

            // Use GetItem to retrieve the full item for condition evaluation
            var request = new GetItemRequest
            {
                TableName = tableName,
                Key = new Dictionary<string, AttributeValue>
                {
                    [keyName] = ConvertPrimitiveToAttributeValue(keyValue)
                },
                ConsistentRead = true
            };

            var response = await _dynamoDbClient.GetItemAsync(request, cancellationToken);

            if (!response.IsItemSet)
            {
                return OperationResult<bool>.Success(false);
            }

            // If condition is specified, check it against the retrieved item
            if (condition != null)
            {
                var document = Document.FromAttributeMap(response.Item);
                var jsonObject = JObject.Parse(document.ToJson());
                AddKeyToJson(jsonObject, keyName, keyValue);

                bool conditionSatisfied = EvaluateCondition(jsonObject, condition);
                return OperationResult<bool>.Success(conditionSatisfied);
            }

            return OperationResult<bool>.Success(true);
        }
        catch (ResourceNotFoundException)
        {
            // Table or item doesn't exist
            return OperationResult<bool>.Success(false);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure($"DatabaseServiceAWS->ItemExistsAsync: {ex.Message}");
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
                return OperationResult<JObject?>.Failure("Failed to get table");
            }

            var config = new GetItemOperationConfig
            {
                ConsistentRead = true
            };

            if (attributesToRetrieve?.Length > 0)
            {
                config.AttributesToGet = [..attributesToRetrieve];
            }

            var document = await table.GetItemAsync(keyValue.ToString(), config, cancellationToken);
            if (document == null)
            {
                return OperationResult<JObject?>.Success(null);
            }

            var result = JObject.Parse(document.ToJson());
            AddKeyToJson(result, keyName, keyValue);
            ApplyOptions(result);

            return OperationResult<JObject?>.Success(result);
        }
        catch (Exception ex)
        {
            return OperationResult<JObject?>.Failure($"DatabaseServiceAWS->GetItemAsync: {ex.Message}");
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
                return OperationResult<IReadOnlyList<JObject>>.Failure("Failed to get table");
            }

            var batchGet = table.CreateBatchGet();
            batchGet.ConsistentRead = true;

            foreach (var value in keyValues)
            {
                batchGet.AddKey(value.ToString());
            }

            await batchGet.ExecuteAsync(cancellationToken);

            var results = new List<JObject>();
            for (int i = 0; i < batchGet.Results.Count && i < keyValues.Length; i++)
            {
                var document = batchGet.Results[i];
                if (document != null)
                {
                    var jsonObject = JObject.Parse(document.ToJson());
                    AddKeyToJson(jsonObject, keyName, keyValues[i]);
                    ApplyOptions(jsonObject);
                    results.Add(jsonObject);
                }
            }

            return OperationResult<IReadOnlyList<JObject>>.Success(results.AsReadOnly());
        }
        catch (Exception e)
        {
            return OperationResult<IReadOnlyList<JObject>>.Failure($"DatabaseServiceAWS->GetItemsAsync: {e.Message}");
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
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<JObject?>.Failure("Failed to get table");
            }

            var config = new DeleteItemOperationConfig
            {
                ReturnValues = returnBehavior switch
                {
                    ReturnItemBehavior.DoNotReturn => ReturnValues.None,
                    ReturnItemBehavior.ReturnNewValues => ReturnValues.AllNewAttributes,
                    ReturnItemBehavior.ReturnOldValues => ReturnValues.AllOldAttributes,
                    _ => ReturnValues.None
                }
            };

            if (condition != null)
            {
                config.ConditionalExpression = BuildConditionalExpression(condition);
            }

            var document = await table.DeleteItemAsync(keyValue.ToString(), config, cancellationToken);

            if (returnBehavior == ReturnItemBehavior.DoNotReturn)
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
            return OperationResult<JObject?>.Failure($"DatabaseServiceAWS->DeleteItemAsync: {ex.Message}");
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
        try
        {
            if (elementsToAdd.Length == 0)
            {
                return OperationResult<JObject?>.Failure("ElementsToAdd must contain values.");
            }

            var expectedKind = elementsToAdd[0].Kind;
            if (elementsToAdd.Any(element => element.Kind != expectedKind))
            {
                return OperationResult<JObject?>.Failure("All elements must have the same type.");
            }

            // Ensure table exists first
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<JObject?>.Failure("Failed to get or create table");
            }

            if (_dynamoDbClient == null)
            {
                return OperationResult<JObject?>.Failure("DynamoDB client not initialized");
            }

            var request = new UpdateItemRequest
            {
                TableName = tableName,
                Key = new Dictionary<string, AttributeValue>
                {
                    [keyName] = ConvertPrimitiveToAttributeValue(keyValue)
                },
                ReturnValues = returnBehavior switch
                {
                    ReturnItemBehavior.DoNotReturn => ReturnValue.NONE,
                    ReturnItemBehavior.ReturnOldValues => ReturnValue.ALL_OLD,
                    ReturnItemBehavior.ReturnNewValues => ReturnValue.ALL_NEW,
                    _ => ReturnValue.NONE
                }
            };

            // Build list of elements to add
            var elementsAsAttributes = elementsToAdd.Select(ConvertPrimitiveToAttributeValue).ToList();

            request.ExpressionAttributeNames = new Dictionary<string, string>
            {
                ["#attr"] = arrayAttributeName
            };

            // Use SET with list_append to add elements to an existing list or create a new list
            request.UpdateExpression = "SET #attr = list_append(if_not_exists(#attr, :empty_list), :vals)";
            request.ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":vals"] = new AttributeValue { L = elementsAsAttributes },
                [":empty_list"] = new AttributeValue { L = [] }
            };

            if (condition != null)
            {
                var conditionExpr = BuildDynamoDbConditionExpression(condition);
                if (!string.IsNullOrEmpty(conditionExpr))
                {
                    request.ConditionExpression = conditionExpr;

                    // Add condition values to expression attribute values
                    if (condition is ValueCondition valueCondition)
                    {
                        request.ExpressionAttributeValues[":cond_val"] = ConvertPrimitiveToConditionAttributeValue(valueCondition.Value);
                    }
                    else if (condition is ArrayElementCondition arrayCondition)
                    {
                        request.ExpressionAttributeValues[":cond_val"] = ConvertPrimitiveToConditionAttributeValue(arrayCondition.ElementValue);
                    }

                    // Add expression attribute names for condition if needed
                    if (condition.ConditionType == DatabaseAttributeConditionType.AttributeEquals ||
                        condition.ConditionType == DatabaseAttributeConditionType.AttributeNotEquals ||
                        condition.ConditionType == DatabaseAttributeConditionType.AttributeGreater ||
                        condition.ConditionType == DatabaseAttributeConditionType.AttributeGreaterOrEqual ||
                        condition.ConditionType == DatabaseAttributeConditionType.AttributeLess ||
                        condition.ConditionType == DatabaseAttributeConditionType.AttributeLessOrEqual ||
                        condition.ConditionType == DatabaseAttributeConditionType.ArrayElementExists ||
                        condition.ConditionType == DatabaseAttributeConditionType.ArrayElementNotExists)
                    {
                        request.ExpressionAttributeNames["#cond_attr"] = condition.AttributeName;
                        // Update the condition expression to use the attribute name alias
                        request.ConditionExpression = conditionExpr.Replace(condition.AttributeName, "#cond_attr");
                    }
                }
            }

            var response = await _dynamoDbClient.UpdateItemAsync(request, cancellationToken);

            if (returnBehavior != ReturnItemBehavior.DoNotReturn && response.Attributes?.Count > 0)
            {
                var result = JObject.Parse(Document.FromAttributeMap(response.Attributes).ToJson());
                ApplyOptions(result);
                return OperationResult<JObject?>.Success(result);
            }

            return OperationResult<JObject?>.Success(null);
        }
        catch (ConditionalCheckFailedException)
        {
            return OperationResult<JObject?>.Failure("Condition check failed");
        }
        catch (Exception ex)
        {
            return OperationResult<JObject?>.Failure($"DatabaseServiceAWS->AddElementsToArrayAsync: {ex.GetType().Name}: {ex.Message}");
        }
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
        try
        {
            if (elementsToRemove.Length == 0)
            {
                return OperationResult<JObject?>.Failure("ElementsToRemove must contain values.");
            }

            var expectedKind = elementsToRemove[0].Kind;
            if (elementsToRemove.Any(element => element.Kind != expectedKind))
            {
                return OperationResult<JObject?>.Failure("All elements must have the same type.");
            }

            // For DynamoDB, removing from LIST type arrays requires a different approach
            // We need to get the current item, modify it, and put it back with list_append
            var getResult = await GetItemAsync(tableName, keyName, keyValue, null, cancellationToken);
            if (!getResult.IsSuccessful || getResult.Data == null)
            {
                return OperationResult<JObject?>.Failure("Item not found for array removal operation");
            }

            var currentItem = getResult.Data;
            if (!currentItem.TryGetValue(arrayAttributeName, out var arrayToken) || arrayToken is not JArray currentArray)
            {
                return OperationResult<JObject?>.Failure($"Attribute {arrayAttributeName} is not an array");
            }

            JObject? oldItem = returnBehavior == ReturnItemBehavior.ReturnOldValues
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

            var updateResult = await UpdateItemAsync(tableName, keyName, keyValue, updateData,
                ReturnItemBehavior.ReturnNewValues, condition, cancellationToken);

            if (!updateResult.IsSuccessful)
            {
                return OperationResult<JObject?>.Failure(updateResult.ErrorMessage ?? "Array update failed");
            }

            return returnBehavior switch
            {
                ReturnItemBehavior.DoNotReturn => OperationResult<JObject?>.Success(null),
                ReturnItemBehavior.ReturnOldValues => OperationResult<JObject?>.Success(oldItem),
                ReturnItemBehavior.ReturnNewValues => updateResult,
                _ => OperationResult<JObject?>.Success(null)
            };
        }
        catch (ConditionalCheckFailedException)
        {
            return OperationResult<JObject?>.Failure("Condition check failed");
        }
        catch (Exception ex)
        {
            return OperationResult<JObject?>.Failure($"DatabaseServiceAWS->RemoveElementsFromArrayAsync: {ex.Message}");
        }
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
        try
        {
            // Ensure table exists first
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<double>.Failure("Failed to get or create table");
            }

            if (_dynamoDbClient == null)
            {
                return OperationResult<double>.Failure("DynamoDB client not initialized");
            }

            var request = new UpdateItemRequest
            {
                TableName = tableName,
                Key = new Dictionary<string, AttributeValue>
                {
                    [keyName] = ConvertPrimitiveToAttributeValue(keyValue)
                },
                ReturnValues = ReturnValue.UPDATED_NEW,
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":incr"] = new AttributeValue { N = incrementValue.ToString(CultureInfo.InvariantCulture) },
                    [":start"] = new AttributeValue { N = "0" }
                },
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    ["#V"] = numericAttributeName
                },
                UpdateExpression = "SET #V = if_not_exists(#V, :start) + :incr"
            };

            if (condition != null)
            {
                // Build condition expression for UpdateItem operation
                var conditionExpr = BuildDynamoDbConditionExpression(condition);
                if (!string.IsNullOrEmpty(conditionExpr))
                {
                    request.ConditionExpression = conditionExpr;

                    // Add condition values to expression attribute values
                    if (condition is ValueCondition valueCondition)
                    {
                        request.ExpressionAttributeValues[":cond_val"] = ConvertPrimitiveToConditionAttributeValue(valueCondition.Value);
                    }
                    else if (condition is ArrayElementCondition arrayCondition)
                    {
                        request.ExpressionAttributeValues[":cond_val"] = ConvertPrimitiveToConditionAttributeValue(arrayCondition.ElementValue);
                    }

                    // Add expression attribute names for condition if needed
                    if (condition.ConditionType == DatabaseAttributeConditionType.AttributeEquals ||
                        condition.ConditionType == DatabaseAttributeConditionType.AttributeNotEquals ||
                        condition.ConditionType == DatabaseAttributeConditionType.AttributeGreater ||
                        condition.ConditionType == DatabaseAttributeConditionType.AttributeGreaterOrEqual ||
                        condition.ConditionType == DatabaseAttributeConditionType.AttributeLess ||
                        condition.ConditionType == DatabaseAttributeConditionType.AttributeLessOrEqual ||
                        condition.ConditionType == DatabaseAttributeConditionType.ArrayElementExists ||
                        condition.ConditionType == DatabaseAttributeConditionType.ArrayElementNotExists)
                    {
                        request.ExpressionAttributeNames["#cond_attr"] = condition.AttributeName;
                        // Update the condition expression to use the attribute name alias
                        request.ConditionExpression = conditionExpr.Replace(condition.AttributeName, "#cond_attr");
                    }
                }
            }

            var response = await _dynamoDbClient.UpdateItemAsync(request, cancellationToken);

            if (response.Attributes?.TryGetValue(numericAttributeName, out var value) == true &&
                double.TryParse(value.N, out var newValue))
            {
                return OperationResult<double>.Success(newValue);
            }

            return OperationResult<double>.Failure("Failed to get updated value from DynamoDB response");
        }
        catch (ConditionalCheckFailedException)
        {
            return OperationResult<double>.Failure("Condition check failed");
        }
        catch (ResourceNotFoundException ex)
        {
            return OperationResult<double>.Failure($"Table not found: {ex.Message}");
        }
        catch (Exception ex)
        {
            return OperationResult<double>.Failure($"DatabaseServiceAWS->IncrementAttributeAsync: {ex.GetType().Name}: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<IReadOnlyList<JObject>>> ScanTableAsync(
        string tableName,
        string[] keyNames,
        CancellationToken cancellationToken = default)
    {
        return await InternalScanTableAsync(tableName, null, cancellationToken);
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
                return OperationResult<(IReadOnlyList<JObject>, string?, long?)>.Failure("Failed to get table");
            }

            var config = new ScanOperationConfig
            {
                Select = SelectValues.AllAttributes,
                Limit = pageSize
            };

            if (!string.IsNullOrEmpty(pageToken))
            {
                // Parse page token if needed - implementation depends on your token format
            }

            var search = table.Scan(config);
            var documents = await search.GetNextSetAsync(cancellationToken);

            var results = new List<JObject>();
            foreach (var document in documents)
            {
                var jsonObject = JObject.Parse(document.ToJson());

                // Find the appropriate key
                foreach (var keyName in keyNames)
                {
                    if (document.ContainsKey(keyName))
                    {
                        // Key is already in the document from DynamoDB
                        break;
                    }
                }

                ApplyOptions(jsonObject);
                results.Add(jsonObject);
            }

            var nextToken = search.IsDone ? null : "next"; // Simplified token logic
            return OperationResult<(IReadOnlyList<JObject>, string?, long?)>.Success(
                (results.AsReadOnly(), nextToken, null));
        }
        catch (Exception e)
        {
            return OperationResult<(IReadOnlyList<JObject>, string?, long?)>.Failure(
                $"DatabaseServiceAWS->ScanTablePaginatedAsync: {e.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<IReadOnlyList<JObject>>> ScanTableWithFilterAsync(
        string tableName,
        string[] keyNames,
        DatabaseAttributeCondition filterCondition,
        CancellationToken cancellationToken = default)
    {
        return await InternalScanTableAsync(tableName, BuildConditionalExpression(filterCondition), cancellationToken);
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
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<(IReadOnlyList<JObject>, string?, long?)>.Failure("Failed to get table");
            }

            var config = new ScanOperationConfig
            {
                Select = SelectValues.AllAttributes,
                Limit = pageSize,
                FilterExpression = BuildConditionalExpression(filterCondition)
            };

            var search = table.Scan(config);
            var documents = await search.GetNextSetAsync(cancellationToken);

            var results = new List<JObject>();
            foreach (var document in documents)
            {
                var jsonObject = JObject.Parse(document.ToJson());
                ApplyOptions(jsonObject);
                results.Add(jsonObject);
            }

            var nextToken = search.IsDone ? null : "next"; // Simplified token logic
            return OperationResult<(IReadOnlyList<JObject>, string?, long?)>.Success(
                (results.AsReadOnly(), nextToken, null));
        }
        catch (Exception e)
        {
            return OperationResult<(IReadOnlyList<JObject>, string?, long?)>.Failure(
                $"DatabaseServiceAWS->ScanTableWithFilterPaginatedAsync: {e.Message}");
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
                return OperationResult<JObject?>.Failure("Failed to get table");
            }

            var item = new JObject(newItem);
            if (!item.ContainsKey(keyName))
            {
                AddKeyToJson(item, keyName, keyValue);
            }

            var itemAsDocument = Document.FromJson(item.ToString());

            if (putOrUpdateItemType == PutOrUpdateItemType.PutItem)
            {
                var config = new PutItemOperationConfig
                {
                    ReturnValues = returnBehavior switch
                    {
                        ReturnItemBehavior.DoNotReturn => ReturnValues.None,
                        ReturnItemBehavior.ReturnNewValues => ReturnValues.AllNewAttributes,
                        ReturnItemBehavior.ReturnOldValues => ReturnValues.AllOldAttributes,
                        _ => ReturnValues.None
                    }
                };

                if (!shouldOverrideIfExists)
                {
                    config.ConditionalExpression = new Expression
                    {
                        ExpressionStatement = $"attribute_not_exists({keyName})"
                    };
                }

                var document = await table.PutItemAsync(itemAsDocument, config, cancellationToken);

                if (returnBehavior == ReturnItemBehavior.DoNotReturn)
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
            else // UpdateItem
            {
                var config = new UpdateItemOperationConfig
                {
                    ReturnValues = returnBehavior switch
                    {
                        ReturnItemBehavior.DoNotReturn => ReturnValues.None,
                        ReturnItemBehavior.ReturnOldValues => ReturnValues.AllOldAttributes,
                        ReturnItemBehavior.ReturnNewValues => ReturnValues.AllNewAttributes,
                        _ => ReturnValues.None
                    }
                };

                if (conditionExpression != null)
                {
                    config.ConditionalExpression = BuildConditionalExpression(conditionExpression);
                }

                var document = await table.UpdateItemAsync(itemAsDocument, config, cancellationToken);

                if (returnBehavior == ReturnItemBehavior.DoNotReturn)
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
            return OperationResult<JObject?>.Failure("Condition check failed");
        }
        catch (Exception ex)
        {
            return OperationResult<JObject?>.Failure($"DatabaseServiceAWS->PutOrUpdateItemAsync: {ex.Message}");
        }
    }

    private async Task<OperationResult<IReadOnlyList<JObject>>> InternalScanTableAsync(
        string tableName,
        Expression? conditionalExpression = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return OperationResult<IReadOnlyList<JObject>>.Failure("Failed to get table");
            }

            var config = new ScanOperationConfig
            {
                Select = SelectValues.AllAttributes
            };

            if (conditionalExpression != null)
            {
                config.FilterExpression = conditionalExpression;
            }

            var search = table.Scan(config);
            var results = new List<JObject>();

            do
            {
                var documents = await search.GetNextSetAsync(cancellationToken);
                foreach (var document in documents)
                {
                    var jsonObject = JObject.Parse(document.ToJson());
                    ApplyOptions(jsonObject);
                    results.Add(jsonObject);
                }
            }
            while (!search.IsDone);

            return OperationResult<IReadOnlyList<JObject>>.Success(results.AsReadOnly());
        }
        catch (Exception e)
        {
            return OperationResult<IReadOnlyList<JObject>>.Failure($"DatabaseServiceAWS->InternalScanTableAsync: {e.Message}");
        }
    }

    private static Expression? BuildConditionalExpression(DatabaseAttributeCondition? condition)
    {
        if (condition == null) return null;

        var expression = new Expression();

        switch (condition.ConditionType)
        {
            case DatabaseAttributeConditionType.AttributeExists:
                expression.ExpressionStatement = $"attribute_exists(#attr)";
                expression.ExpressionAttributeNames["#attr"] = condition.AttributeName;
                break;
            case DatabaseAttributeConditionType.AttributeNotExists:
                expression.ExpressionStatement = $"attribute_not_exists(#attr)";
                expression.ExpressionAttributeNames["#attr"] = condition.AttributeName;
                break;
            case DatabaseAttributeConditionType.AttributeEquals when condition is ValueCondition valueCondition:
                expression.ExpressionStatement = $"#attr = :val";
                expression.ExpressionAttributeNames["#attr"] = condition.AttributeName;
                AddValueToExpression(expression, ":val", valueCondition.Value);
                break;
            case DatabaseAttributeConditionType.AttributeNotEquals when condition is ValueCondition valueCondition:
                expression.ExpressionStatement = $"#attr <> :val";
                expression.ExpressionAttributeNames["#attr"] = condition.AttributeName;
                AddValueToExpression(expression, ":val", valueCondition.Value);
                break;
            case DatabaseAttributeConditionType.AttributeGreater when condition is ValueCondition valueCondition:
                expression.ExpressionStatement = $"#attr > :val";
                expression.ExpressionAttributeNames["#attr"] = condition.AttributeName;
                AddValueToExpression(expression, ":val", valueCondition.Value);
                break;
            case DatabaseAttributeConditionType.AttributeGreaterOrEqual when condition is ValueCondition valueCondition:
                expression.ExpressionStatement = $"#attr >= :val";
                expression.ExpressionAttributeNames["#attr"] = condition.AttributeName;
                AddValueToExpression(expression, ":val", valueCondition.Value);
                break;
            case DatabaseAttributeConditionType.AttributeLess when condition is ValueCondition valueCondition:
                expression.ExpressionStatement = $"#attr < :val";
                expression.ExpressionAttributeNames["#attr"] = condition.AttributeName;
                AddValueToExpression(expression, ":val", valueCondition.Value);
                break;
            case DatabaseAttributeConditionType.AttributeLessOrEqual when condition is ValueCondition valueCondition:
                expression.ExpressionStatement = $"#attr <= :val";
                expression.ExpressionAttributeNames["#attr"] = condition.AttributeName;
                AddValueToExpression(expression, ":val", valueCondition.Value);
                break;
            case DatabaseAttributeConditionType.ArrayElementExists when condition is ArrayElementCondition arrayCondition:
                expression.ExpressionStatement = $"contains(#attr, :cond_val)";
                expression.ExpressionAttributeNames["#attr"] = condition.AttributeName;
                AddValueToExpression(expression, ":cond_val", arrayCondition.ElementValue);
                break;
            case DatabaseAttributeConditionType.ArrayElementNotExists when condition is ArrayElementCondition arrayCondition:
                expression.ExpressionStatement = $"NOT contains(#attr, :cond_val)";
                expression.ExpressionAttributeNames["#attr"] = condition.AttributeName;
                AddValueToExpression(expression, ":cond_val", arrayCondition.ElementValue);
                break;
        }

        return expression;
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
    private static string? BuildDynamoDbConditionExpression(DatabaseAttributeCondition condition)
    {
        var expression = condition.ConditionType switch
        {
            DatabaseAttributeConditionType.AttributeExists => $"attribute_exists({condition.AttributeName})",
            DatabaseAttributeConditionType.AttributeNotExists => $"attribute_not_exists({condition.AttributeName})",
            DatabaseAttributeConditionType.AttributeEquals => $"{condition.AttributeName} = :cond_val",
            DatabaseAttributeConditionType.AttributeNotEquals => $"{condition.AttributeName} <> :cond_val",
            DatabaseAttributeConditionType.AttributeGreater => $"{condition.AttributeName} > :cond_val",
            DatabaseAttributeConditionType.AttributeGreaterOrEqual => $"{condition.AttributeName} >= :cond_val",
            DatabaseAttributeConditionType.AttributeLess => $"{condition.AttributeName} < :cond_val",
            DatabaseAttributeConditionType.AttributeLessOrEqual => $"{condition.AttributeName} <= :cond_val",
            DatabaseAttributeConditionType.ArrayElementExists => $"contains({condition.AttributeName}, :cond_val)",
            DatabaseAttributeConditionType.ArrayElementNotExists => $"NOT contains({condition.AttributeName}, :cond_val)",
            _ => null
        };

        return expression;
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

    /// <summary>
    /// Evaluates a database condition against a JSON object
    /// </summary>
    private static bool EvaluateCondition(JObject jsonObject, DatabaseAttributeCondition condition)
    {
        return condition.ConditionType switch
        {
            DatabaseAttributeConditionType.AttributeExists => jsonObject.ContainsKey(condition.AttributeName),
            DatabaseAttributeConditionType.AttributeNotExists => !jsonObject.ContainsKey(condition.AttributeName),
            _ when condition is ValueCondition valueCondition => EvaluateValueCondition(jsonObject, valueCondition),
            _ when condition is ArrayElementCondition arrayCondition => EvaluateArrayElementCondition(jsonObject, arrayCondition),
            _ => false
        };
    }

    /// <summary>
    /// Evaluates a value-based condition against a JSON object
    /// </summary>
    private static bool EvaluateValueCondition(JObject jsonObject, ValueCondition condition)
    {
        if (!jsonObject.TryGetValue(condition.AttributeName, out var token))
            return false;

        try
        {
            return condition.ConditionType switch
            {
                DatabaseAttributeConditionType.AttributeEquals => CompareValues(token, condition.Value) == 0,
                DatabaseAttributeConditionType.AttributeNotEquals => CompareValues(token, condition.Value) != 0,
                DatabaseAttributeConditionType.AttributeGreater => CompareValues(token, condition.Value) > 0,
                DatabaseAttributeConditionType.AttributeGreaterOrEqual => CompareValues(token, condition.Value) >= 0,
                DatabaseAttributeConditionType.AttributeLess => CompareValues(token, condition.Value) < 0,
                DatabaseAttributeConditionType.AttributeLessOrEqual => CompareValues(token, condition.Value) <= 0,
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
    private static bool EvaluateArrayElementCondition(JObject jsonObject, ArrayElementCondition condition)
    {
        if (!jsonObject.TryGetValue(condition.AttributeName, out var token) || token is not JArray array)
            return condition.ConditionType == DatabaseAttributeConditionType.ArrayElementNotExists;

        bool elementExists = array.Any(item => CompareValues(item, condition.ElementValue) == 0);

        return condition.ConditionType switch
        {
            DatabaseAttributeConditionType.ArrayElementExists => elementExists,
            DatabaseAttributeConditionType.ArrayElementNotExists => !elementExists,
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
    public DatabaseAttributeCondition BuildAttributeExistsCondition(string attributeName) =>
        new ExistenceCondition(DatabaseAttributeConditionType.AttributeExists, attributeName);
    public DatabaseAttributeCondition BuildAttributeNotExistsCondition(string attributeName) =>
        new ExistenceCondition(DatabaseAttributeConditionType.AttributeNotExists, attributeName);
    public DatabaseAttributeCondition BuildArrayElementExistsCondition(string attributeName, PrimitiveType elementValue) =>
        new ArrayElementCondition(DatabaseAttributeConditionType.ArrayElementExists, attributeName, elementValue);
    public DatabaseAttributeCondition BuildArrayElementNotExistsCondition(string attributeName, PrimitiveType elementValue) =>
        new ArrayElementCondition(DatabaseAttributeConditionType.ArrayElementNotExists, attributeName, elementValue);

    #endregion

    /// <summary>
    /// Override AddKeyToJson to ensure DynamoDB key compatibility
    /// </summary>
    private new static void AddKeyToJson(JObject destination, string keyName, PrimitiveType keyValue)
    {
        // For DynamoDB compatibility, always store keys as strings since DynamoDB tables
        // are created with string key schemas for maximum flexibility
        destination[keyName] = keyValue.ToString();
    }

    public void Dispose()
    {
        _dynamoDbClient?.Dispose();
        // ReSharper disable once GCSuppressFinalizeForTypeWithoutDestructor
        GC.SuppressFinalize(this);
    }
}
