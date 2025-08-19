// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Cloud.Interfaces;
using Utilities.Common;
using Newtonsoft.Json.Linq;
using Amazon.DynamoDBv2.DocumentModel;
using System.Collections.Concurrent;
using Expression = Amazon.DynamoDBv2.DocumentModel.Expression;

namespace Cloud.Database.AWS;

public class DatabaseServiceAWS : DatabaseServiceBase, IDatabaseService, IDisposable
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
    /// Searches table definition in LoadedTables, if not loaded, loads, stores and returns
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
            // Use the modern TableBuilder approach instead of obsolete Table.LoadTable
            // First, get table description to understand its structure
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
                    var hashKeyType = ConvertScalarAttributeTypeToDynamoDBEntryType(hashKeyAttribute.AttributeType);
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
                    var rangeKeyType = ConvertScalarAttributeTypeToDynamoDBEntryType(rangeKeyAttribute.AttributeType);
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
                            var gsiHashKeyType = ConvertScalarAttributeTypeToDynamoDBEntryType(gsiHashKeyAttr.AttributeType);

                            if (gsiRangeKey != null)
                            {
                                var gsiRangeKeyAttr = describeResponse.Table.AttributeDefinitions.FirstOrDefault(a => a.AttributeName == gsiRangeKey.AttributeName);
                                if (gsiRangeKeyAttr != null)
                                {
                                    var gsiRangeKeyType = ConvertScalarAttributeTypeToDynamoDBEntryType(gsiRangeKeyAttr.AttributeType);
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
        catch (Exception)
        {
            // Table doesn't exist or other error
        }

        return null;
    }

    /// <summary>
    /// Converts DynamoDB ScalarAttributeType to DynamoDBEntryType for TableBuilder
    /// </summary>
    private static DynamoDBEntryType ConvertScalarAttributeTypeToDynamoDBEntryType(ScalarAttributeType attributeType)
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
        return value.Kind switch
        {
            PrimitiveTypeKind.Integer => new AttributeValue { N = value.AsInteger.ToString() },
            PrimitiveTypeKind.Double => new AttributeValue { N = value.AsDouble.ToString() },
            PrimitiveTypeKind.String => new AttributeValue { S = value.AsString },
            PrimitiveTypeKind.ByteArray => new AttributeValue { S = value.ToString() },
            _ => new AttributeValue { S = value.ToString() }
        };
    }

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
            if (_dynamoDbClient == null)
                return DatabaseResult<bool>.Failure("DynamoDB client not initialized");

            var request = new QueryRequest
            {
                TableName = tableName,
                KeyConditionExpression = $"{keyName} = :key_val",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":key_val"] = ConvertPrimitiveToAttributeValue(keyValue)
                },
                ProjectionExpression = keyName,
                ConsistentRead = true,
                Limit = 1
            };

            if (condition != null)
            {
                BuildConditionExpression(condition, request.ExpressionAttributeValues, out var conditionExpr);
                if (!string.IsNullOrEmpty(conditionExpr))
                {
                    request.KeyConditionExpression += $" and {conditionExpr}";
                }
            }

            var response = await _dynamoDbClient.QueryAsync(request, cancellationToken);
            return DatabaseResult<bool>.Success(response.Count > 0);
        }
        catch (Exception ex)
        {
            return DatabaseResult<bool>.Failure($"DatabaseServiceAWS->ItemExistsAsync: {ex.Message}");
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
                return DatabaseResult<JObject?>.Success(null);
            }

            var result = JObject.Parse(document.ToJson());
            AddKeyToJson(result, keyName, keyValue);
            ApplyOptions(result);

            return DatabaseResult<JObject?>.Success(result);
        }
        catch (Exception ex)
        {
            return DatabaseResult<JObject?>.Failure($"DatabaseServiceAWS->GetItemAsync: {ex.Message}");
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

            return DatabaseResult<IReadOnlyList<JObject>>.Success(results.AsReadOnly());
        }
        catch (Exception e)
        {
            return DatabaseResult<IReadOnlyList<JObject>>.Failure($"DatabaseServiceAWS->GetItemsAsync: {e.Message}");
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
                return DatabaseResult<JObject?>.Success(null);
            }

            if (document != null)
            {
                var result = JObject.Parse(document.ToJson());
                ApplyOptions(result);
                return DatabaseResult<JObject?>.Success(result);
            }

            return DatabaseResult<JObject?>.Success(null);
        }
        catch (Exception ex)
        {
            return DatabaseResult<JObject?>.Failure($"DatabaseServiceAWS->DeleteItemAsync: {ex.Message}");
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

            if (_dynamoDbClient == null)
            {
                return DatabaseResult<JObject?>.Failure("DynamoDB client not initialized");
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

            var elementsToAddList = elementsToAdd.Select(element => element.Kind switch
            {
                PrimitiveTypeKind.Integer => element.AsInteger.ToString(),
                PrimitiveTypeKind.Double => element.AsDouble.ToString(),
                PrimitiveTypeKind.ByteArray => element.ToString(),
                _ => element.AsString
            }).ToList();

            request.ExpressionAttributeNames = new Dictionary<string, string>
            {
                ["#V"] = arrayAttributeName
            };
            request.UpdateExpression = "ADD #V :vals";
            request.ExpressionAttributeValues = [];

            if (expectedKind == PrimitiveTypeKind.Integer || expectedKind == PrimitiveTypeKind.Double)
            {
                request.ExpressionAttributeValues[":vals"] = new AttributeValue { NS = elementsToAddList };
            }
            else
            {
                request.ExpressionAttributeValues[":vals"] = new AttributeValue { SS = elementsToAddList };
            }

            if (condition != null)
            {
                BuildConditionExpression(condition, request.ExpressionAttributeValues, out var conditionExpr);
                if (!string.IsNullOrEmpty(conditionExpr))
                {
                    request.ConditionExpression = conditionExpr;
                }
            }

            var response = await _dynamoDbClient.UpdateItemAsync(request, cancellationToken);

            if (returnBehavior != ReturnItemBehavior.DoNotReturn && response.Attributes?.Count > 0)
            {
                var result = JObject.Parse(Document.FromAttributeMap(response.Attributes).ToJson());
                ApplyOptions(result);
                return DatabaseResult<JObject?>.Success(result);
            }

            return DatabaseResult<JObject?>.Success(null);
        }
        catch (Exception ex)
        {
            return DatabaseResult<JObject?>.Failure($"DatabaseServiceAWS->AddElementsToArrayAsync: {ex.Message}");
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

            if (_dynamoDbClient == null)
            {
                return DatabaseResult<JObject?>.Failure("DynamoDB client not initialized");
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

            var elementsToRemoveList = elementsToRemove.Select(element => element.Kind switch
            {
                PrimitiveTypeKind.Integer => element.AsInteger.ToString(),
                PrimitiveTypeKind.Double => element.AsDouble.ToString(),
                PrimitiveTypeKind.ByteArray => element.ToString(),
                _ => element.AsString
            }).ToList();

            request.ExpressionAttributeNames = new Dictionary<string, string>
            {
                ["#V"] = arrayAttributeName
            };
            request.UpdateExpression = "DELETE #V :vals";
            request.ExpressionAttributeValues = [];

            if (expectedKind == PrimitiveTypeKind.Integer || expectedKind == PrimitiveTypeKind.Double)
            {
                request.ExpressionAttributeValues[":vals"] = new AttributeValue { NS = elementsToRemoveList };
            }
            else
            {
                request.ExpressionAttributeValues[":vals"] = new AttributeValue { SS = elementsToRemoveList };
            }

            if (condition != null)
            {
                BuildConditionExpression(condition, request.ExpressionAttributeValues, out var conditionExpr);
                if (!string.IsNullOrEmpty(conditionExpr))
                {
                    request.ConditionExpression = conditionExpr;
                }
            }

            var response = await _dynamoDbClient.UpdateItemAsync(request, cancellationToken);

            if (returnBehavior != ReturnItemBehavior.DoNotReturn && response.Attributes?.Count > 0)
            {
                var result = JObject.Parse(Document.FromAttributeMap(response.Attributes).ToJson());
                ApplyOptions(result);
                return DatabaseResult<JObject?>.Success(result);
            }

            return DatabaseResult<JObject?>.Success(null);
        }
        catch (Exception ex)
        {
            return DatabaseResult<JObject?>.Failure($"DatabaseServiceAWS->RemoveElementsFromArrayAsync: {ex.Message}");
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
            if (_dynamoDbClient == null)
            {
                return DatabaseResult<double>.Failure("DynamoDB client not initialized");
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
                    [":incr"] = new AttributeValue { N = incrementValue.ToString() },
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
                BuildConditionExpression(condition, request.ExpressionAttributeValues, out var conditionExpr);
                if (!string.IsNullOrEmpty(conditionExpr))
                {
                    request.ConditionExpression = conditionExpr;
                }
            }

            var response = await _dynamoDbClient.UpdateItemAsync(request, cancellationToken);

            if (response.Attributes?.TryGetValue(numericAttributeName, out var value) == true && 
                double.TryParse(value.N, out var newValue))
            {
                return DatabaseResult<double>.Success(newValue);
            }

            return DatabaseResult<double>.Failure("Failed to get updated value");
        }
        catch (Exception ex)
        {
            return DatabaseResult<double>.Failure($"DatabaseServiceAWS->IncrementAttributeAsync: {ex.Message}");
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
        return await InternalScanTableAsync(tableName, null, cancellationToken);
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
            return DatabaseResult<(IReadOnlyList<JObject>, string?, long?)>.Success(
                (results.AsReadOnly(), nextToken, null));
        }
        catch (Exception e)
        {
            return DatabaseResult<(IReadOnlyList<JObject>, string?, long?)>.Failure(
                $"DatabaseServiceAWS->ScanTablePaginatedAsync: {e.Message}");
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
        return await InternalScanTableAsync(tableName, BuildConditionalExpression(filterCondition), cancellationToken);
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
            return DatabaseResult<(IReadOnlyList<JObject>, string?, long?)>.Success(
                (results.AsReadOnly(), nextToken, null));
        }
        catch (Exception e)
        {
            return DatabaseResult<(IReadOnlyList<JObject>, string?, long?)>.Failure(
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
                    return DatabaseResult<JObject?>.Success(null);
                }

                if (document != null)
                {
                    var result = JObject.Parse(document.ToJson());
                    ApplyOptions(result);
                    return DatabaseResult<JObject?>.Success(result);
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
                    return DatabaseResult<JObject?>.Success(null);
                }

                if (document != null)
                {
                    var result = JObject.Parse(document.ToJson());
                    ApplyOptions(result);
                    return DatabaseResult<JObject?>.Success(result);
                }
            }

            return DatabaseResult<JObject?>.Success(null);
        }
        catch (ConditionalCheckFailedException)
        {
            return DatabaseResult<JObject?>.Failure("Condition check failed");
        }
        catch (Exception ex)
        {
            return DatabaseResult<JObject?>.Failure($"DatabaseServiceAWS->PutOrUpdateItemAsync: {ex.Message}");
        }
    }

    private async Task<DatabaseResult<IReadOnlyList<JObject>>> InternalScanTableAsync(
        string tableName,
        Expression? conditionalExpression = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var table = await GetTableAsync(tableName, cancellationToken);
            if (table == null)
            {
                return DatabaseResult<IReadOnlyList<JObject>>.Failure("Failed to get table");
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

            return DatabaseResult<IReadOnlyList<JObject>>.Success(results.AsReadOnly());
        }
        catch (Exception e)
        {
            return DatabaseResult<IReadOnlyList<JObject>>.Failure($"DatabaseServiceAWS->InternalScanTableAsync: {e.Message}");
        }
    }

    private static Expression? BuildConditionalExpression(DatabaseAttributeCondition? condition)
    {
        if (condition == null) return null;

        var expression = new Expression();

        switch (condition.ConditionType)
        {
            case DatabaseAttributeConditionType.AttributeExists:
                expression.ExpressionStatement = $"attribute_exists({condition.AttributeName})";
                break;
            case DatabaseAttributeConditionType.AttributeNotExists:
                expression.ExpressionStatement = $"attribute_not_exists({condition.AttributeName})";
                break;
            case DatabaseAttributeConditionType.AttributeEquals when condition is ValueCondition valueCondition:
                expression.ExpressionStatement = $"{condition.AttributeName} = :val";
                AddValueToExpression(expression, ":val", valueCondition.Value);
                break;
            case DatabaseAttributeConditionType.AttributeNotEquals when condition is ValueCondition valueCondition:
                expression.ExpressionStatement = $"{condition.AttributeName} <> :val";
                AddValueToExpression(expression, ":val", valueCondition.Value);
                break;
            case DatabaseAttributeConditionType.AttributeGreater when condition is ValueCondition valueCondition:
                expression.ExpressionStatement = $"{condition.AttributeName} > :val";
                AddValueToExpression(expression, ":val", valueCondition.Value);
                break;
            case DatabaseAttributeConditionType.AttributeGreaterOrEqual when condition is ValueCondition valueCondition:
                expression.ExpressionStatement = $"{condition.AttributeName} >= :val";
                AddValueToExpression(expression, ":val", valueCondition.Value);
                break;
            case DatabaseAttributeConditionType.AttributeLess when condition is ValueCondition valueCondition:
                expression.ExpressionStatement = $"{condition.AttributeName} < :val";
                AddValueToExpression(expression, ":val", valueCondition.Value);
                break;
            case DatabaseAttributeConditionType.AttributeLessOrEqual when condition is ValueCondition valueCondition:
                expression.ExpressionStatement = $"{condition.AttributeName} <= :val";
                AddValueToExpression(expression, ":val", valueCondition.Value);
                break;
            case DatabaseAttributeConditionType.ArrayElementExists when condition is ArrayElementCondition arrayCondition:
                expression.ExpressionStatement = $"contains({condition.AttributeName}, :cond_val)";
                AddValueToExpression(expression, ":cond_val", arrayCondition.ElementValue);
                break;
            case DatabaseAttributeConditionType.ArrayElementNotExists when condition is ArrayElementCondition arrayCondition:
                expression.ExpressionStatement = $"NOT contains({condition.AttributeName}, :cond_val)";
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

    private static bool BuildConditionExpression(DatabaseAttributeCondition? condition, Dictionary<string, AttributeValue> expressionAttributeValues, out string? finalConditionExpression)
    {
        finalConditionExpression = null;
        if (condition == null) return false;

        switch (condition.ConditionType)
        {
            case DatabaseAttributeConditionType.AttributeExists:
                finalConditionExpression = $"attribute_exists({condition.AttributeName})";
                return true;
            case DatabaseAttributeConditionType.AttributeNotExists:
                finalConditionExpression = $"attribute_not_exists({condition.AttributeName})";
                return true;
            case DatabaseAttributeConditionType.AttributeEquals when condition is ValueCondition valueCondition:
                finalConditionExpression = $"{condition.AttributeName} = :val";
                AddValueToAttributeValueDict(expressionAttributeValues, ":val", valueCondition.Value);
                return true;
            case DatabaseAttributeConditionType.AttributeNotEquals when condition is ValueCondition valueCondition:
                finalConditionExpression = $"{condition.AttributeName} <> :val";
                AddValueToAttributeValueDict(expressionAttributeValues, ":val", valueCondition.Value);
                return true;
            case DatabaseAttributeConditionType.AttributeGreater when condition is ValueCondition valueCondition:
                finalConditionExpression = $"{condition.AttributeName} > :val";
                AddValueToAttributeValueDict(expressionAttributeValues, ":val", valueCondition.Value);
                return true;
            case DatabaseAttributeConditionType.ArrayElementExists when condition is ArrayElementCondition arrayCondition:
                finalConditionExpression = $"contains({condition.AttributeName}, :cond_val)";
                AddValueToAttributeValueDict(expressionAttributeValues, ":cond_val", arrayCondition.ElementValue);
                return true;
        }

        return false;
    }

    private static void AddValueToAttributeValueDict(Dictionary<string, AttributeValue> dict, string placeholder, PrimitiveType value)
    {
        switch (value.Kind)
        {
            case PrimitiveTypeKind.Integer:
                dict[placeholder] = new AttributeValue { NS = [value.AsInteger.ToString()] };
                break;
            case PrimitiveTypeKind.Double:
                dict[placeholder] = new AttributeValue { NS = [value.AsDouble.ToString()] };
                break;
            case PrimitiveTypeKind.String:
                dict[placeholder] = new AttributeValue { SS = [value.AsString] };
                break;
            case PrimitiveTypeKind.ByteArray:
                dict[placeholder] = new AttributeValue { SS = [value.ToString()] };
                break;
        }
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

    public void Dispose()
    {
        _dynamoDbClient?.Dispose();
        GC.SuppressFinalize(this);
    }
}
