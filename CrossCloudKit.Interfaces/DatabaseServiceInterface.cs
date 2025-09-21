// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Interfaces.Enums;
using CrossCloudKit.Interfaces.Records;
using CrossCloudKit.Utilities.Common;
using Newtonsoft.Json.Linq;

namespace CrossCloudKit.Interfaces;

public abstract class DatabaseServiceBase
{
    private static JToken FromPrimitiveTypeToJToken(PrimitiveType primitive)
    {
        return primitive.Kind switch
        {
            PrimitiveTypeKind.Double => primitive.AsDouble,
            PrimitiveTypeKind.Integer => primitive.AsInteger,
            PrimitiveTypeKind.ByteArray => Convert.ToBase64String(primitive.AsByteArray),
            _ => primitive.AsString
        };
    }

    protected static void AddKeyToJson(JObject destination, string keyName, PrimitiveType keyValue)
    {
        destination[keyName] = FromPrimitiveTypeToJToken(keyValue);
    }

    public void SetOptions(DbOptions newOptions)
    {
        Options = newOptions ?? throw new ArgumentNullException(nameof(newOptions));
    }

    protected DbOptions Options { get; private set; } = new();
}

/// <summary>
/// Modern async interface for database services
/// </summary>
public interface IDatabaseService
{
    /// <summary>
    /// Gets a value indicating whether the database service has been successfully initialized
    /// </summary>
    bool IsInitialized { get; }

    /// <summary>
    /// Sets the database options for this service instance
    /// </summary>
    void SetOptions(DbOptions newOptions);

    // Condition builders
    DbAttributeCondition BuildAttributeExistsCondition(string attributeName);
    DbAttributeCondition BuildAttributeNotExistsCondition(string attributeName);
    DbAttributeCondition BuildAttributeEqualsCondition(string attributeName, PrimitiveType value);
    DbAttributeCondition BuildAttributeNotEqualsCondition(string attributeName, PrimitiveType value);
    DbAttributeCondition BuildAttributeGreaterCondition(string attributeName, PrimitiveType value);
    DbAttributeCondition BuildAttributeGreaterOrEqualCondition(string attributeName, PrimitiveType value);
    DbAttributeCondition BuildAttributeLessCondition(string attributeName, PrimitiveType value);
    DbAttributeCondition BuildAttributeLessOrEqualCondition(string attributeName, PrimitiveType value);
    DbAttributeCondition BuildArrayElementExistsCondition(string attributeName, PrimitiveType elementValue);
    DbAttributeCondition BuildArrayElementNotExistsCondition(string attributeName, PrimitiveType elementValue);

    /// <summary>
    /// Checks if an item exists and optionally satisfies a condition
    /// </summary>
    Task<OperationResult<bool>> ItemExistsAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        DbAttributeCondition? condition = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets an item from the database
    /// </summary>
    Task<OperationResult<JObject?>> GetItemAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        string[]? attributesToRetrieve = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets multiple items from the database
    /// </summary>
    Task<OperationResult<IReadOnlyList<JObject>>> GetItemsAsync(
        string tableName,
        string keyName,
        PrimitiveType[] keyValues,
        string[]? attributesToRetrieve = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Puts an item into the database
    /// </summary>
    Task<OperationResult<JObject?>> PutItemAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        JObject item,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        bool overwriteIfExists = false,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Updates an existing item in the database
    /// </summary>
    Task<OperationResult<JObject?>> UpdateItemAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        JObject updateData,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        DbAttributeCondition? condition = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes an item from the database
    /// </summary>
    Task<OperationResult<JObject?>> DeleteItemAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        DbAttributeCondition? condition = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds elements to an array attribute of an item
    /// </summary>
    Task<OperationResult<JObject?>> AddElementsToArrayAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        string arrayAttributeName,
        PrimitiveType[] elementsToAdd,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        DbAttributeCondition? condition = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes elements from an array attribute of an item
    /// </summary>
    Task<OperationResult<JObject?>> RemoveElementsFromArrayAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        string arrayAttributeName,
        PrimitiveType[] elementsToRemove,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        DbAttributeCondition? condition = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Atomically increments or decrements a numeric attribute of an item
    /// </summary>
    Task<OperationResult<double>> IncrementAttributeAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        string numericAttributeName,
        double incrementValue,
        DbAttributeCondition? condition = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Scans a table and returns all items
    /// </summary>
    Task<OperationResult<IReadOnlyList<JObject>>> ScanTableAsync(
        string tableName,
        string[] keyNames,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Scans a table with pagination support
    /// </summary>
    Task<OperationResult<(IReadOnlyList<JObject> Items, string? NextPageToken, long? TotalCount)>> ScanTablePaginatedAsync(
        string tableName,
        string[] keyNames,
        int pageSize,
        string? pageToken = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Scans a table and returns items that match the specified filter condition
    /// </summary>
    Task<OperationResult<IReadOnlyList<JObject>>> ScanTableWithFilterAsync(
        string tableName,
        string[] keyNames,
        DbAttributeCondition filterCondition,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Scans a table with filtering and pagination support
    /// </summary>
    Task<OperationResult<(IReadOnlyList<JObject> Items, string? NextPageToken, long? TotalCount)>> ScanTableWithFilterPaginatedAsync(
        string tableName,
        string[] keyNames,
        DbAttributeCondition filterCondition,
        int pageSize,
        string? pageToken = null,
        CancellationToken cancellationToken = default);
}
