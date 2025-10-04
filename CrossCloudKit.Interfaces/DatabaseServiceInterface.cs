// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;
using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Interfaces.Enums;
using CrossCloudKit.Interfaces.Records;
using CrossCloudKit.Utilities.Common;
using Newtonsoft.Json.Linq;

namespace CrossCloudKit.Interfaces;

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
    /// <param name="newOptions">The new database options to apply to this service instance. These options control behaviors such as automatic array sorting and float-to-integer conversion.</param>
    /// <remarks>
    /// This method allows runtime configuration of database service behavior. The options affect how data is processed and returned across all database operations.
    /// </remarks>
    void SetOptions(DbOptions newOptions);

    // Condition builders
    /// <summary>
    /// Creates a condition that checks if an attribute exists on an item
    /// </summary>
    /// <param name="attributeName">The name of the attribute to check for existence</param>
    /// <returns>A condition that evaluates to true if the specified attribute exists on the item</returns>
    /// <remarks>This condition is useful for ensuring an attribute is present before performing operations that depend on its existence.</remarks>
    Condition AttributeExists(string attributeName);

    /// <summary>
    /// Creates a condition that checks if an attribute does not exist on an item
    /// </summary>
    /// <param name="attributeName">The name of the attribute to check for non-existence</param>
    /// <returns>A condition that evaluates to true if the specified attribute does not exist on the item</returns>
    /// <remarks>This condition is useful for preventing overwrites or ensuring clean initial states.</remarks>
    Condition AttributeNotExists(string attributeName);

    /// <summary>
    /// Creates a condition that checks if an attribute equals a specific value
    /// </summary>
    /// <param name="attributeName">The name of the attribute to compare</param>
    /// <param name="value">The value to compare the attribute against</param>
    /// <returns>A condition that evaluates to true if the attribute equals the specified value</returns>
    /// <remarks>The comparison is type-sensitive and will handle different primitive types appropriately.</remarks>
    Condition AttributeEquals(string attributeName, Primitive value);

    /// <summary>
    /// Creates a condition that checks if an attribute does not equal a specific value
    /// </summary>
    /// <param name="attributeName">The name of the attribute to compare</param>
    /// <param name="value">The value to compare the attribute against</param>
    /// <returns>A condition that evaluates to true if the attribute does not equal the specified value</returns>
    /// <remarks>The comparison is type-sensitive and will handle different primitive types appropriately.</remarks>
    Condition AttributeNotEquals(string attributeName, Primitive value);

    /// <summary>
    /// Creates a condition that checks if an attribute is greater than a specific value
    /// </summary>
    /// <param name="attributeName">The name of the attribute to compare</param>
    /// <param name="value">The value to compare the attribute against</param>
    /// <returns>A condition that evaluates to true if the attribute is greater than the specified value</returns>
    /// <remarks>This condition works with numeric types and strings (lexicographical comparison).</remarks>
    Condition AttributeIsGreaterThan(string attributeName, Primitive value);

    /// <summary>
    /// Creates a condition that checks if an attribute is greater than or equal to a specific value
    /// </summary>
    /// <param name="attributeName">The name of the attribute to compare</param>
    /// <param name="value">The value to compare the attribute against</param>
    /// <returns>A condition that evaluates to true if the attribute is greater than or equal to the specified value</returns>
    /// <remarks>This condition works with numeric types and strings (lexicographical comparison).</remarks>
    Condition AttributeIsGreaterOrEqual(string attributeName, Primitive value);

    /// <summary>
    /// Creates a condition that checks if an attribute is less than a specific value
    /// </summary>
    /// <param name="attributeName">The name of the attribute to compare</param>
    /// <param name="value">The value to compare the attribute against</param>
    /// <returns>A condition that evaluates to true if the attribute is less than the specified value</returns>
    /// <remarks>This condition works with numeric types and strings (lexicographical comparison).</remarks>
    Condition AttributeIsLessThan(string attributeName, Primitive value);

    /// <summary>
    /// Creates a condition that checks if an attribute is less than or equal to a specific value
    /// </summary>
    /// <param name="attributeName">The name of the attribute to compare</param>
    /// <param name="value">The value to compare the attribute against</param>
    /// <returns>A condition that evaluates to true if the attribute is less than or equal to the specified value</returns>
    /// <remarks>This condition works with numeric types and strings (lexicographical comparison).</remarks>
    Condition AttributeIsLessOrEqual(string attributeName, Primitive value);

    /// <summary>
    /// Creates a condition that checks if an array attribute contains a specific element
    /// </summary>
    /// <param name="attributeName">The name of the array attribute to check</param>
    /// <param name="elementValue">The value to search for in the array</param>
    /// <returns>A condition that evaluates to true if the array contains the specified element</returns>
    /// <remarks>The element comparison is type-sensitive and uses exact matching.</remarks>
    Condition ArrayElementExists(string attributeName, Primitive elementValue);

    /// <summary>
    /// Creates a condition that checks if an array attribute does not contain a specific element
    /// </summary>
    /// <param name="attributeName">The name of the array attribute to check</param>
    /// <param name="elementValue">The value to search for in the array</param>
    /// <returns>A condition that evaluates to true if the array does not contain the specified element</returns>
    /// <remarks>The element comparison is type-sensitive and uses exact matching.</remarks>
    Condition ArrayElementNotExists(string attributeName, Primitive elementValue);

    /// <summary>
    /// Checks if an item exists in the database and optionally verifies that it satisfies specified conditions.
    /// </summary>
    /// <param name="tableName">The name of the table to check for the item. Table names are case-sensitive.</param>
    /// <param name="key">The name and value of the primary key attribute used to identify the item. Key->value supports string, integer, double, and byte array types.</param>
    /// <param name="conditions">Optional conditions that the item must satisfy. If provided, the method returns success only if the item exists AND meets the conditions.</param>
    /// <param name="cancellationToken">Token to cancel the asynchronous operation.</param>
    /// <returns>
    /// A task representing the asynchronous operation. The result contains:
    /// - Success with true if the item exists (and meets the conditions, if specified)
    /// - Failure with status code <see cref="HttpStatusCode.NotFound"/> if the item does not exist
    /// - Failure with status code <see cref="HttpStatusCode.PreconditionFailed"/> if the item exists but the conditions are not satisfied
    /// - Failure with status code <see cref="HttpStatusCode.InternalServerError"/> if the operation encounters an exception
    /// </returns>
    /// <remarks>
    /// This method first checks for the existence of the item using an efficient query.
    /// If conditions are provided, it evaluates them and only returns success if all conditions are satisfied.
    /// </remarks>
    /// <exception cref="ArgumentException">Thrown when tableName or keyName is null or whitespace.</exception>
    Task<OperationResult<bool>> ItemExistsAsync(
        string tableName,
        DbKey key,
        ConditionCoupling? conditions = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves a single item from the database by its primary key
    /// </summary>
    /// <param name="tableName">The name of the table from which to retrieve the item. Table names are case-sensitive.</param>
    /// <param name="key">The name and value of the primary key attribute used to identify the item. Key->value supports string, integer, double, and byte array types.</param>
    /// <param name="attributesToRetrieve">Optional array of attribute names to retrieve. If null or empty, all attributes are returned. This parameter may be ignored by some implementations.</param>
    /// <param name="cancellationToken">Token to cancel the asynchronous operation.</param>
    /// <returns>
    /// A task representing the asynchronous operation. The result contains:
    /// - Success with a JObject containing the item data if the item exists
    /// - Success with null if the item doesn't exist
    /// - Failure with error details if the operation encounters an exception
    /// </returns>
    /// <remarks>
    /// The returned JObject will include the primary key as an attribute. Database-specific formatting options
    /// (such as array sorting and float-to-integer conversion) are applied based on the current DbOptions.
    /// The operation uses consistent reads where supported by the underlying database implementation.
    /// </remarks>
    /// <exception cref="ArgumentException">Thrown when tableName or keyName is null or whitespace.</exception>
    Task<OperationResult<JObject?>> GetItemAsync(
        string tableName,
        DbKey key,
        string[]? attributesToRetrieve = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves multiple items from the database by their primary keys in a single batch operation
    /// </summary>
    /// <param name="tableName">The name of the table from which to retrieve the items. Table names are case-sensitive.</param>
    /// <param name="keys">The name and value of the primary keys attribute used to search the items.</param>
    /// <param name="attributesToRetrieve">Optional array of attribute names to retrieve for each item. If null or empty, all attributes are returned. This parameter may be ignored by some implementations.</param>
    /// <param name="cancellationToken">Token to cancel the asynchronous operation.</param>
    /// <returns>
    /// A task representing the asynchronous operation. The result contains:
    /// - Success with a read-only list of JObject items that were found (may be fewer than requested if some keys don't exist)
    /// - Success with an empty list if no items are found
    /// - Failure with error details if the operation encounters an exception
    /// </returns>
    /// <remarks>
    /// This method is optimized for batch retrieval and is more efficient than multiple individual GetItemAsync calls.
    /// The order of returned items may not match the order of input key values. Items that don't exist are simply omitted from the results.
    /// Each returned JObject will include the primary key as an attribute. Database-specific formatting options are applied based on current DbOptions.
    /// The operation uses consistent reads where supported by the underlying database implementation.
    /// </remarks>
    /// <exception cref="ArgumentException">Thrown when tableName or keyName is null or whitespace.</exception>
    /// <exception cref="ArgumentNullException">Thrown when keyValues is null.</exception>
    Task<OperationResult<IReadOnlyList<JObject>>> GetItemsAsync(
        string tableName,
        DbKey[] keys,
        string[]? attributesToRetrieve = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Inserts a new item into the database or optionally overwrites an existing item
    /// </summary>
    /// <param name="tableName">The name of the table in which to store the item. Table names are case-sensitive.</param>
    /// <param name="key">The name and value of the primary key attribute used to identify the item. Key->value supports string, integer, double, and byte array types.</param>
    /// <param name="item">The item data as a JObject. The primary key will be automatically added if not present.</param>
    /// <param name="returnBehavior">Specifies what data to return: DoNotReturn (default), ReturnOldValues (returns previous item if overwritten), or ReturnNewValues (returns the stored item).</param>
    /// <param name="overwriteIfExists">If false (default), the operation fails if an item with the same key exists. If true, overwrites existing items.</param>
    /// <param name="cancellationToken">Token to cancel the asynchronous operation.</param>
    /// <returns>
    /// A task representing the asynchronous operation. The result contains:
    /// - Success with the requested item data (based on returnBehavior) if the operation succeeds
    /// - Success with null if returnBehavior is DoNotReturn
    /// - Failure with Conflict status if overwriteIfExists is false and the item already exists
    /// - Failure with error details if the operation encounters an exception
    /// </returns>
    /// <remarks>
    /// This method is used for creating new items. Use UpdateItemAsync for modifying existing items with partial data.
    /// The primary key is automatically included in the stored item data. Database-specific formatting options are applied based on current DbOptions.
    /// If the table doesn't exist, it may be automatically created depending on the database implementation.
    /// </remarks>
    /// <exception cref="ArgumentException">Thrown when tableName or keyName is null or whitespace.</exception>
    /// <exception cref="ArgumentNullException">Thrown when item is null.</exception>
    Task<OperationResult<JObject?>> PutItemAsync(
        string tableName,
        DbKey key,
        JObject item,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        bool overwriteIfExists = false,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Updates an existing item in the database with partial data, merging new attributes with existing ones
    /// </summary>
    /// <param name="tableName">The name of the table containing the item to update. Table names are case-sensitive.</param>
    /// <param name="key">The name and value of the primary key attribute used to identify the item. Key->value supports string, integer, double, and byte array types.</param>
    /// <param name="updateData">The partial data to merge with the existing item. Only specified attributes are updated; others remain unchanged.</param>
    /// <param name="returnBehavior">Specifies what data to return: DoNotReturn (default), ReturnOldValues (returns item before update), or ReturnNewValues (returns item after update).</param>
    /// <param name="conditions">Optional conditions that must be satisfied for the update to proceed. If the conditions fails, the update is rejected.</param>
    /// <param name="cancellationToken">Token to cancel the asynchronous operation.</param>
    /// <returns>
    /// A task representing the asynchronous operation. The result contains:
    /// - Success with the requested item data (based on returnBehavior) if the update succeeds
    /// - Success with null if returnBehavior is DoNotReturn
    /// - Failure with PreconditionFailed status if the conditions are not satisfied
    /// - Failure with error details if the operation encounters an exception
    /// </returns>
    /// <remarks>
    /// This method performs a merge operation, combining the updateData with the existing item. Use PutItemAsync for complete item replacement.
    /// If the item doesn't exist, it will be created with the updateData (upsert behavior).
    /// The primary key is automatically included in the result data. Database-specific formatting options are applied based on current DbOptions.
    /// Conditional updates provide optimistic concurrency control and prevent race conditions.
    /// </remarks>
    /// <exception cref="ArgumentException">Thrown when tableName or keyName is null or whitespace.</exception>
    /// <exception cref="ArgumentNullException">Thrown when updateData is null.</exception>
    Task<OperationResult<JObject?>> UpdateItemAsync(
        string tableName,
        DbKey key,
        JObject updateData,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        ConditionCoupling? conditions = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes an item from the database by its primary key, optionally returning the deleted item data
    /// </summary>
    /// <param name="tableName">The name of the table from which to delete the item. Table names are case-sensitive.</param>
    /// <param name="key">The name and value of the primary key attribute used to identify the item. Key->value supports string, integer, double, and byte array types.</param>
    /// <param name="returnBehavior">Specifies what data to return: DoNotReturn (default), ReturnOldValues (returns the deleted item), or ReturnNewValues (not applicable for delete operations).</param>
    /// <param name="conditions">Optional conditions that must be satisfied for the deletion to proceed. If the conditions fail, the deletion is rejected.</param>
    /// <param name="cancellationToken">Token to cancel the asynchronous operation.</param>
    /// <returns>
    /// A task representing the asynchronous operation. The result contains:
    /// - Success with the deleted item data if returnBehavior is ReturnOldValues and the item existed
    /// - Success with null if returnBehavior is DoNotReturn or the item didn't exist
    /// - Failure with PreconditionFailed status if the conditions are not satisfied
    /// - Failure with error details if the operation encounters an exception
    /// </returns>
    /// <remarks>
    /// If the item doesn't exist, the operation succeeds without error (idempotent behavior).
    /// Conditional deletes provide optimistic concurrency control and prevent accidental deletions.
    /// The primary key is automatically included in the returned item data. Database-specific formatting options are applied based on current DbOptions.
    /// This operation is atomic and cannot be partially completed.
    /// </remarks>
    /// <exception cref="ArgumentException">Thrown when tableName or keyName is null or whitespace.</exception>
    Task<OperationResult<JObject?>> DeleteItemAsync(
        string tableName,
        DbKey key,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        ConditionCoupling? conditions = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Atomically adds elements to an array attribute of an existing item, creating the array if it doesn't exist
    /// </summary>
    /// <param name="tableName">The name of the table containing the item to modify. Table names are case-sensitive.</param>
    /// <param name="key">The name and value of the primary key attribute used to identify the item. Key->value supports string, integer, double, and byte array types.</param>
    /// <param name="arrayAttributeName">The name of the attribute that contains the array. If the attribute doesn't exist, a new array is created.</param>
    /// <param name="elementsToAdd">Array of elements to add to the target array. All elements must be of the same primitive type. Cannot be empty.</param>
    /// <param name="returnBehavior">Specifies what data to return: DoNotReturn (default), ReturnOldValues (returns item before modification), or ReturnNewValues (returns item after modification).</param>
    /// <param name="conditions">Optional conditions that must be satisfied for the operation to proceed. Evaluated against the current item state.</param>
    /// <param name="cancellationToken">Token to cancel the asynchronous operation.</param>
    /// <returns>
    /// A task representing the asynchronous operation. The result contains:
    /// - Success with the requested item data (based on returnBehavior) if the operation succeeds
    /// - Success with null if returnBehavior is DoNotReturn
    /// - Failure with BadRequest status if elementsToAdd is empty or contains mixed types
    /// - Failure with PreconditionFailed status if the conditions are not satisfied
    /// - Failure with error details if the operation encounters an exception
    /// </returns>
    /// <remarks>
    /// This operation is atomic and will either add all elements or none. Elements are appended to the end of the existing array.
    /// If the item doesn't exist, it will be created with the new array containing the specified elements.
    /// If the target attribute exists but is not an array, the behavior is implementation-dependent and may result in an error.
    /// All elements in elementsToAdd must have the same primitive type to ensure array consistency.
    /// The operation uses optimistic locking when conditions are specified to prevent race conditions.
    /// </remarks>
    /// <exception cref="ArgumentException">Thrown when tableName, keyName, or arrayAttributeName is null or whitespace, or when elementsToAdd contains mixed types.</exception>
    /// <exception cref="ArgumentNullException">Thrown when elementsToAdd is null.</exception>
    Task<OperationResult<JObject?>> AddElementsToArrayAsync(
        string tableName,
        DbKey key,
        string arrayAttributeName,
        Primitive[] elementsToAdd,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        ConditionCoupling? conditions = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Atomically removes specified elements from an array attribute of an existing item
    /// </summary>
    /// <param name="tableName">The name of the table containing the item to modify. Table names are case-sensitive.</param>
    /// <param name="key">The name and value of the primary key attribute used to identify the item. Key->value supports string, integer, double, and byte array types.</param>
    /// <param name="arrayAttributeName">The name of the attribute that contains the array to modify.</param>
    /// <param name="elementsToRemove">Array of elements to remove from the target array. All elements must be of the same primitive type. Cannot be empty. Elements that don't exist in the array are ignored.</param>
    /// <param name="returnBehavior">Specifies what data to return: DoNotReturn (default), ReturnOldValues (returns item before modification), or ReturnNewValues (returns item after modification).</param>
    /// <param name="conditions">Optional conditions that must be satisfied for the operation to proceed. Evaluated against the current item state.</param>
    /// <param name="cancellationToken">Token to cancel the asynchronous operation.</param>
    /// <returns>
    /// A task representing the asynchronous operation. The result contains:
    /// - Success with the requested item data (based on returnBehavior) if the operation succeeds
    /// - Success with null if returnBehavior is DoNotReturn or the item doesn't exist
    /// - Failure with BadRequest status if elementsToRemove is empty or contains mixed types
    /// - Failure with PreconditionFailed status if the conditions are not satisfied
    /// - Failure with error details if the operation encounters an exception
    /// </returns>
    /// <remarks>
    /// This operation is atomic and will either remove all matching elements or none. All instances of matching elements are removed from the array.
    /// If the item or array attribute doesn't exist, the operation succeeds without error (idempotent behavior).
    /// If the target attribute exists but is not an array, the behavior is implementation-dependent and may result in an error.
    /// Element matching uses exact value comparison. All elements in elementsToRemove must have the same primitive type.
    /// The operation uses optimistic locking when conditions are specified to prevent race conditions.
    /// </remarks>
    /// <exception cref="ArgumentException">Thrown when tableName, keyName, or arrayAttributeName is null or whitespace, or when elementsToRemove contains mixed types.</exception>
    /// <exception cref="ArgumentNullException">Thrown when elementsToRemove is null.</exception>
    Task<OperationResult<JObject?>> RemoveElementsFromArrayAsync(
        string tableName,
        DbKey key,
        string arrayAttributeName,
        Primitive[] elementsToRemove,
        DbReturnItemBehavior returnBehavior = DbReturnItemBehavior.DoNotReturn,
        ConditionCoupling? conditions = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Atomically increments or decrements a numeric attribute of an item, creating the item and attribute if they don't exist
    /// </summary>
    /// <param name="tableName">The name of the table containing the item to modify. Table names are case-sensitive.</param>
    /// <param name="key">The name and value of the primary key attribute used to identify the item. Key->value supports string, integer, double, and byte array types.</param>
    /// <param name="numericAttributeName">The name of the numeric attribute to increment. If the attribute doesn't exist, it's created with an initial value of 0 before applying the increment.</param>
    /// <param name="incrementValue">The value to add to the current attribute value. Use negative values for decrementing. Supports both integer and floating-point increments.</param>
    /// <param name="conditions">Optional conditions that must be satisfied for the operation to proceed. Evaluated against the current item state before the increment.</param>
    /// <param name="cancellationToken">Token to cancel the asynchronous operation.</param>
    /// <returns>
    /// A task representing the asynchronous operation. The result contains:
    /// - Success with the new value of the attribute after the increment operation
    /// - Failure with PreconditionFailed status if the conditions are not satisfied
    /// - Failure with error details if the operation encounters an exception
    /// </returns>
    /// <remarks>
    /// This operation is atomic and guarantees that concurrent increments will be properly serialized without data loss.
    /// If the item doesn't exist, it will be created with the numeric attribute set to the increment value.
    /// If the target attribute exists but is not numeric, the behavior is implementation-dependent and may result in an error.
    /// The operation always returns the final value after the increment, enabling atomic read-modify-write patterns.
    /// This is particularly useful for counters, sequence numbers, and other scenarios requiring atomic numeric operations.
    /// The operation uses optimistic locking when conditions are specified to prevent race conditions.
    /// </remarks>
    /// <exception cref="ArgumentException">Thrown when tableName, keyName, or numericAttributeName is null or whitespace.</exception>
    Task<OperationResult<double>> IncrementAttributeAsync(
        string tableName,
        DbKey key,
        string numericAttributeName,
        double incrementValue,
        ConditionCoupling? conditions = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Scans an entire table and returns all primary keys and items without filtering.
    /// </summary>
    /// <param name="tableName">The name of the table to scan. Table names are case-sensitive.</param>
    /// <param name="cancellationToken">Token to cancel the asynchronous operation.</param>
    /// <returns>
    /// A task representing the asynchronous operation. The result contains:
    /// - Success with a tuple consisting of:
    ///   - <c>Keys</c>: A read-only list of string representations of the primary keys for all items
    ///   - <c>Items</c>: A read-only list of <see cref="JObject"/> instances for all items in the table
    /// - Success with empty lists if the table is empty or does not exist
    /// - Failure with error details if the operation encounters an exception
    /// </returns>
    /// <remarks>
    /// This operation reads all items in the table, which may be expensive for large tables. Consider using paginated variants for better performance.
    /// Each returned <see cref="JObject"/> includes the primary key as an attribute when it can be determined.
    /// Database-specific formatting options are applied based on current <c>DbOptions</c> settings.
    /// The operation uses consistent reads where supported by the underlying database implementation.
    /// For large tables, this operation may take considerable time and consume significant resources.
    /// The order of returned items is not guaranteed and may vary between database implementations.
    /// The <c>Keys</c> and <c>Items</c> collections are index-aligned (i.e., <c>Keys[i]</c> corresponds to <c>Items[i]</c>).
    /// </remarks>
    /// <exception cref="ArgumentException">Thrown when <paramref name="tableName"/> is null or whitespace.</exception>
    Task<OperationResult<(IReadOnlyList<string> Keys, IReadOnlyList<JObject> Items)>> ScanTableAsync(
        string tableName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Scans a table with pagination support, returning a subset of primary keys, items, and continuation information.
    /// </summary>
    /// <param name="tableName">
    /// The name of the table to scan. Table names are case-sensitive.
    /// </param>
    /// <param name="pageSize">
    /// The maximum number of items to return in this page. Must be greater than 0.
    /// The actual number returned may be less due to filtering or reaching the end of the data.
    /// </param>
    /// <param name="pageToken">
    /// Optional pagination token from a previous scan operation.
    /// If null, scanning starts from the beginning of the table.
    /// </param>
    /// <param name="cancellationToken">Token to cancel the asynchronous operation.</param>
    /// <returns>
    /// A task representing the asynchronous operation. The result contains:
    /// - Success with a tuple consisting of:
    ///   - <c>Keys</c>: A read-only list of string representations of the primary keys for the items in this page (Only returned if <paramref name="pageToken"/> is null
    ///   - <c>Items</c>: A read-only list of <see cref="JObject"/> instances for the items in this page (up to <paramref name="pageSize"/>)
    ///   - <c>NextPageToken</c>: Token for the next page (null if no more pages are available)
    ///   - <c>TotalCount</c>: Total number of items in the table (supported by MongoDB and Basic implementations; null for AWS DynamoDB and Google Cloud Datastore)
    /// - Failure with error details if the operation encounters an exception
    /// </returns>
    /// <remarks>
    /// This method provides efficient scanning of large tables by processing data in smaller chunks.
    /// The <c>NextPageToken</c> should be passed to subsequent calls to continue scanning from where the previous call left off.
    /// Each returned <see cref="JObject"/> includes the primary key as an attribute when it can be determined.
    /// Database-specific formatting options are applied based on current <c>DbOptions</c> settings.
    /// <c>TotalCount</c> is null for AWS DynamoDB and Google Cloud Datastore due to their distributed nature, which makes efficient counting infeasible.
    /// MongoDB and Basic implementations return actual counts.
    /// Page tokens are implementation-specific and should be treated as opaque strings.
    /// The operation uses consistent reads where supported by the underlying database implementation.
    /// The <c>Keys</c> and <c>Items</c> collections are index-aligned (i.e., <c>Keys[i]</c> corresponds to <c>Items[i]</c>).
    /// </remarks>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="tableName"/> is null or whitespace, or <paramref name="pageSize"/> is less than or equal to 0.
    /// </exception>
    Task<OperationResult<(IReadOnlyList<string>? Keys, IReadOnlyList<JObject> Items, string? NextPageToken, long? TotalCount)>> ScanTablePaginatedAsync(
        string tableName,
        int pageSize,
        string? pageToken = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Scans an entire table and returns the primary keys and items that match the specified filter conditions.
    /// </summary>
    /// <param name="tableName">The name of the table to scan. Table names are case-sensitive.</param>
    /// <param name="filterConditions">
    /// The conditions that items must satisfy to be included in the results. The conditions are evaluated
    /// against each item's attributes.
    /// </param>
    /// <param name="cancellationToken">Token to cancel the asynchronous operation.</param>
    /// <returns>
    /// A task representing the asynchronous operation. The result contains:
    /// - Success with a tuple consisting of:
    ///   - <c>Keys</c>: a read-only list of string representations of the primary keys for the matching items
    ///   - <c>Items</c>: a read-only list of <see cref="JObject"/> instances for the matching items
    /// - Success with empty lists if no items match the conditions or the table is empty
    /// - Failure with error details if the operation encounters an exception
    /// </returns>
    /// <remarks>
    /// This operation scans all items in the table and applies the filter, which may be expensive for large tables.
    /// Consider using paginated variants for better performance and memory efficiency with large result sets.
    /// Filter conditions may be evaluated client-side or server-side depending on the implementation.
    /// Each returned <see cref="JObject"/> includes the primary key as an attribute when it can be determined.
    /// Database-specific formatting options are applied based on current <c>DbOptions</c> settings.
    /// The operation uses consistent reads where supported by the underlying database implementation.
    /// For large tables with selective filters, this operation may take a considerable time.
    /// The order of returned items is not guaranteed and may vary between database implementations.
    /// </remarks>
    /// <exception cref="ArgumentException">Thrown when <paramref name="tableName"/> is null or whitespace.</exception>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="filterConditions"/> is null.</exception>
    Task<OperationResult<(IReadOnlyList<string> Keys, IReadOnlyList<JObject> Items)>> ScanTableWithFilterAsync(
        string tableName,
        ConditionCoupling filterConditions,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Scans a table with filtering and pagination support, returning only items that match the conditions with continuation information
    /// </summary>
    /// <param name="tableName">The name of the table to scan. Table names are case-sensitive.</param>
    /// <param name="filterConditions">The conditions that items must satisfy to be included in the results. The conditions are evaluated against each item's attributes.</param>
    /// <param name="pageSize">The maximum number of matching items to return in this page. Must be greater than 0. Actual returned count may be less due to filtering or end of data.</param>
    /// <param name="pageToken">Optional pagination token from a previous scan operation. If null, starts from the beginning of the table.</param>
    /// <param name="cancellationToken">Token to cancel the asynchronous operation.</param>
    /// <returns>
    /// A task representing the asynchronous operation. The result contains:
    /// - Success with a tuple containing:
    ///   - Keys: A read-only list of keys used by items that match the filter conditions (Only returned if <paramref name="pageToken"/> is null.
    ///   - Items: A read-only list of matching items for this page (up to pageSize items)
    ///   - NextPageToken: Token for the next page (null if no more pages available)
    ///   - TotalCount: Total number of matching items in the table (supported by MongoDB and Basic implementations; null for AWS DynamoDB and Google Cloud Datastore due to their architectural limitations)
    /// - Failure with error details if the operation encounters an exception
    /// </returns>
    /// <remarks>
    /// This method provides efficient scanning of large tables with filtering by processing data in smaller chunks.
    /// The NextPageToken should be used for later calls to continue scanning from where the previous call left off.
    /// Note that the database may need to scan more than pageSize items internally to find pageSize matching items.
    /// Filter conditions are evaluated on the client side for some implementations or server-side for others.
    /// Each returned JObject includes the primary key as an attribute when it can be determined.
    /// Database-specific formatting options are applied based on current DbOptions settings.
    /// TotalCount is null for AWS DynamoDB and Google Cloud Datastore implementations due to their distributed nature, making efficient counting of filtered results difficult. MongoDB and Basic implementations provide actual counts.
    /// Page tokens are implementation-specific and should be treated as opaque strings.
    /// The operation uses consistent reads where supported by the underlying database implementation.
    /// </remarks>
    /// <exception cref="ArgumentException">Thrown when tableName is null or whitespace, or pageSize is less than or equal to 0.</exception>
    /// <exception cref="ArgumentNullException">Thrown when the filterConditions parameter is null.</exception>
    Task<OperationResult<(IReadOnlyList<string>? Keys, IReadOnlyList<JObject> Items, string? NextPageToken, long? TotalCount)>> ScanTableWithFilterPaginatedAsync(
        string tableName,
        ConditionCoupling filterConditions,
        int pageSize,
        string? pageToken = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the names of all tables available in the underlying database.
    /// </summary>
    /// <param name="cancellationToken">
    /// A token that can be used to cancel the asynchronous operation.
    /// </param>
    /// <returns>
    /// A task representing the asynchronous operation. The task result contains an <see cref="OperationResult{T}"/>
    /// wrapping a read-only list of strings, where each string represents a table name in the database.
    /// </returns>
    Task<OperationResult<IReadOnlyList<string>>> GetTableNamesAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Drops the specified table from the underlying database.
    /// </summary>
    /// <param name="tableName">
    /// The name of the table to be dropped. If the table does not exist, the operation is still considered successful.
    /// </param>
    /// <param name="cancellationToken">
    /// A token that can be used to cancel the asynchronous operation.
    /// </param>
    /// <returns>
    /// A task representing the asynchronous operation. The task result contains an <see cref="OperationResult{T}"/>
    /// wrapping a boolean value: <c>true</c> if the table was successfully dropped or did not exist; <c>false</c> if the operation failed.
    /// </returns>
    Task<OperationResult<bool>> DropTableAsync(string tableName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the list of primary key names for the specified table.
    /// </summary>
    /// <param name="tableName">The name of the table whose keys are being retrieved. Table names are case-sensitive.</param>
    /// <param name="cancellationToken">Token to cancel the asynchronous operation.</param>
    /// <returns>
    /// A task representing the asynchronous operation. The result contains:
    /// - Success with a read-only list of primary key names if the operation succeeds
    /// - Success with an empty list if the table exists but has no recorded keys
    /// - Failure with error details if the operation fails to retrieve the table keys
    /// </returns>
    /// <remarks>
    /// This method fetches the table metadata from the system table and extracts the key names.
    /// It does not scan table data; it only returns the names of the attributes used as primary keys.
    /// The operation depends on the <c>SystemTableKeyName</c> and <c>SystemTableKeysAttributeName</c> constants being defined correctly.
    /// </remarks>
    /// <exception cref="ArgumentException">Thrown when <paramref name="tableName"/> is null or whitespace.</exception>
    Task<OperationResult<IReadOnlyList<string>>> GetTableKeysAsync(
        string tableName,
        CancellationToken cancellationToken = default);
}
