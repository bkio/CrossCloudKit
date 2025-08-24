// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Utilities.Common;
using Newtonsoft.Json.Linq;

namespace Cloud.Interfaces;

/// <summary>
/// After performing an operation that causes a change in an item, defines what service shall return
/// </summary>
public enum ReturnItemBehavior
{
    DoNotReturn,
    ReturnOldValues,
    ReturnNewValues
}

public enum DatabaseAttributeConditionType
{
    AttributeEquals,
    AttributeNotEquals,
    AttributeGreater,
    AttributeGreaterOrEqual,
    AttributeLess,
    AttributeLessOrEqual,
    AttributeExists,
    AttributeNotExists,
    ArrayElementExists,
    ArrayElementNotExists
}

public enum AutoSortArrays
{
    No,
    Yes
}

public enum AutoConvertRoundableFloatToInt
{
    No,
    Yes
}

public class DatabaseOptions
{
    public AutoSortArrays AutoSortArrays { get; set; } = AutoSortArrays.No;
    public AutoConvertRoundableFloatToInt AutoConvertRoundableFloatToInt { get; set; } = AutoConvertRoundableFloatToInt.No;
}

/// <summary>
/// Represents the result of a database operation
/// </summary>
public class DatabaseResult<T>
{
    public bool IsSuccessful { get; }
    public T? Data { get; }
    public string? ErrorMessage { get; }

    private DatabaseResult(bool isSuccessful, T? data, string? errorMessage)
    {
        IsSuccessful = isSuccessful;
        Data = data;
        ErrorMessage = errorMessage;
    }

    public static DatabaseResult<T> Success(T data) => new(true, data, null);
    public static DatabaseResult<T> Failure(string errorMessage) => new(false, default, errorMessage);
}

public abstract class DatabaseAttributeCondition(DatabaseAttributeConditionType conditionType, string attributeName)
{
    public DatabaseAttributeConditionType ConditionType { get; } = conditionType;
    public string AttributeName { get; } = attributeName ?? throw new ArgumentNullException(nameof(attributeName));
}

public class ExistenceCondition : DatabaseAttributeCondition
{
    public ExistenceCondition(DatabaseAttributeConditionType conditionType, string attributeName)
        : base(conditionType, attributeName)
    {
        if (conditionType != DatabaseAttributeConditionType.AttributeExists &&
            conditionType != DatabaseAttributeConditionType.AttributeNotExists)
        {
            throw new ArgumentException("Invalid condition type for existence condition", nameof(conditionType));
        }
    }
}

public class ValueCondition(DatabaseAttributeConditionType conditionType, string attributeName, PrimitiveType value) : DatabaseAttributeCondition(conditionType, attributeName)
{
    public PrimitiveType Value { get; } = value ?? throw new ArgumentNullException(nameof(value));
}

public class ArrayElementCondition : DatabaseAttributeCondition
{
    public PrimitiveType ElementValue { get; }

    public ArrayElementCondition(DatabaseAttributeConditionType conditionType, string attributeName, PrimitiveType elementValue)
        : base(conditionType, attributeName)
    {
        if (conditionType != DatabaseAttributeConditionType.ArrayElementExists &&
            conditionType != DatabaseAttributeConditionType.ArrayElementNotExists)
        {
            throw new ArgumentException("Invalid condition type for array element condition", nameof(conditionType));
        }
        ElementValue = elementValue ?? throw new ArgumentNullException(nameof(elementValue));
    }
}

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
        ArgumentNullException.ThrowIfNull(destination);
        destination[keyName] = FromPrimitiveTypeToJToken(keyValue);
    }

    public void SetOptions(DatabaseOptions newOptions)
    {
        Options = newOptions ?? throw new ArgumentNullException(nameof(newOptions));
    }

    protected DatabaseOptions Options { get; private set; } = new();
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
    void SetOptions(DatabaseOptions newOptions);

    // Condition builders
    DatabaseAttributeCondition BuildAttributeExistsCondition(string attributeName);
    DatabaseAttributeCondition BuildAttributeNotExistsCondition(string attributeName);
    DatabaseAttributeCondition BuildAttributeEqualsCondition(string attributeName, PrimitiveType value);
    DatabaseAttributeCondition BuildAttributeNotEqualsCondition(string attributeName, PrimitiveType value);
    DatabaseAttributeCondition BuildAttributeGreaterCondition(string attributeName, PrimitiveType value);
    DatabaseAttributeCondition BuildAttributeGreaterOrEqualCondition(string attributeName, PrimitiveType value);
    DatabaseAttributeCondition BuildAttributeLessCondition(string attributeName, PrimitiveType value);
    DatabaseAttributeCondition BuildAttributeLessOrEqualCondition(string attributeName, PrimitiveType value);
    DatabaseAttributeCondition BuildArrayElementExistsCondition(string attributeName, PrimitiveType elementValue);
    DatabaseAttributeCondition BuildArrayElementNotExistsCondition(string attributeName, PrimitiveType elementValue);

    /// <summary>
    /// Checks if an item exists and optionally satisfies a condition
    /// </summary>
    Task<DatabaseResult<bool>> ItemExistsAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        DatabaseAttributeCondition? condition = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets an item from the database
    /// </summary>
    Task<DatabaseResult<JObject?>> GetItemAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        string[]? attributesToRetrieve = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets multiple items from the database
    /// </summary>
    Task<DatabaseResult<IReadOnlyList<JObject>>> GetItemsAsync(
        string tableName,
        string keyName,
        PrimitiveType[] keyValues,
        string[]? attributesToRetrieve = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Puts an item into the database
    /// </summary>
    Task<DatabaseResult<JObject?>> PutItemAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        JObject item,
        ReturnItemBehavior returnBehavior = ReturnItemBehavior.DoNotReturn,
        bool overwriteIfExists = false,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Updates an existing item in the database
    /// </summary>
    Task<DatabaseResult<JObject?>> UpdateItemAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        JObject updateData,
        ReturnItemBehavior returnBehavior = ReturnItemBehavior.DoNotReturn,
        DatabaseAttributeCondition? condition = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes an item from the database
    /// </summary>
    Task<DatabaseResult<JObject?>> DeleteItemAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        ReturnItemBehavior returnBehavior = ReturnItemBehavior.DoNotReturn,
        DatabaseAttributeCondition? condition = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds elements to an array attribute of an item
    /// </summary>
    Task<DatabaseResult<JObject?>> AddElementsToArrayAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        string arrayAttributeName,
        PrimitiveType[] elementsToAdd,
        ReturnItemBehavior returnBehavior = ReturnItemBehavior.DoNotReturn,
        DatabaseAttributeCondition? condition = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes elements from an array attribute of an item
    /// </summary>
    Task<DatabaseResult<JObject?>> RemoveElementsFromArrayAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        string arrayAttributeName,
        PrimitiveType[] elementsToRemove,
        ReturnItemBehavior returnBehavior = ReturnItemBehavior.DoNotReturn,
        DatabaseAttributeCondition? condition = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Atomically increments or decrements a numeric attribute of an item
    /// </summary>
    Task<DatabaseResult<double>> IncrementAttributeAsync(
        string tableName,
        string keyName,
        PrimitiveType keyValue,
        string numericAttributeName,
        double incrementValue,
        DatabaseAttributeCondition? condition = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Scans a table and returns all items
    /// </summary>
    Task<DatabaseResult<IReadOnlyList<JObject>>> ScanTableAsync(
        string tableName,
        string[] keyNames,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Scans a table with pagination support
    /// </summary>
    Task<DatabaseResult<(IReadOnlyList<JObject> Items, string? NextPageToken, long? TotalCount)>> ScanTablePaginatedAsync(
        string tableName,
        string[] keyNames,
        int pageSize,
        string? pageToken = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Scans a table and returns items that match the specified filter condition
    /// </summary>
    Task<DatabaseResult<IReadOnlyList<JObject>>> ScanTableWithFilterAsync(
        string tableName,
        string[] keyNames,
        DatabaseAttributeCondition filterCondition,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Scans a table with filtering and pagination support
    /// </summary>
    Task<DatabaseResult<(IReadOnlyList<JObject> Items, string? NextPageToken, long? TotalCount)>> ScanTableWithFilterPaginatedAsync(
        string tableName,
        string[] keyNames,
        DatabaseAttributeCondition filterCondition,
        int pageSize,
        string? pageToken = null,
        CancellationToken cancellationToken = default);
}
