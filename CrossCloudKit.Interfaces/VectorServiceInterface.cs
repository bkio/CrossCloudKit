// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Interfaces.Enums;
using CrossCloudKit.Interfaces.Records;
using CrossCloudKit.Utilities.Common;

namespace CrossCloudKit.Interfaces;

/// <summary>
/// Cross-cloud abstraction for vector database services.
/// Supports filtering on stored metadata using the same <see cref="Condition"/>
/// and <see cref="ConditionCoupling"/> model used by <see cref="IDatabaseService"/>.
/// </summary>
/// <remarks>
/// <para>Providers: Qdrant and Basic (in-memory). Use <see cref="EnsureCollectionExistsAsync"/> before upserting.</para>
/// <para>Pair with <see cref="ILLMService"/> for text-to-vector workflows via <see cref="LLMVectorExtensions"/>.</para>
/// <para>Condition builders (<c>FieldEquals</c>, etc.) and <c>.And()</c>/<c>.Or()</c> composition work identically to <see cref="IDatabaseService"/>.</para>
/// </remarks>
public interface IVectorService : IAsyncDisposable
{
    /// <summary>
    /// Gets a value indicating whether the service has been successfully initialised.
    /// </summary>
    /// <example>
    /// <code>
    /// if (!vectorService.IsInitialized)
    ///     throw new InvalidOperationException("Vector service is not initialized.");
    /// </code>
    /// </example>
    bool IsInitialized { get; }

    // ── Condition builders ──────────────────────────────────────────────────

    /// <summary>Creates a condition that checks whether a metadata field exists on a point.</summary>
    /// <param name="fieldName">The metadata field name.</param>
    /// <example>
    /// <code>
    /// var hasCategory = vectorService.FieldExists("category");
    /// </code>
    /// </example>
    Condition FieldExists(string fieldName);

    /// <summary>Creates a condition that checks whether a metadata field does not exist on a point.</summary>
    /// <param name="fieldName">The metadata field name.</param>
    /// <example>
    /// <code>
    /// var noTag = vectorService.FieldNotExists("deprecated");
    /// </code>
    /// </example>
    Condition FieldNotExists(string fieldName);

    /// <summary>Creates a condition that checks whether a metadata field equals <paramref name="value"/>.</summary>
    /// <param name="fieldName">The metadata field name.</param>
    /// <param name="value">The value to compare against.</param>
    /// <example>
    /// <code>
    /// var isEnglish = vectorService.FieldEquals("language", new Primitive("en"));
    /// var filter = isEnglish.And(vectorService.FieldExists("verified"));
    /// </code>
    /// </example>
    Condition FieldEquals(string fieldName, Primitive value);

    /// <summary>Creates a condition that checks whether a metadata field does not equal <paramref name="value"/>.</summary>
    /// <param name="fieldName">The metadata field name.</param>
    /// <param name="value">The value to compare against.</param>
    /// <example>
    /// <code>
    /// var notDraft = vectorService.FieldNotEquals("status", new Primitive("draft"));
    /// </code>
    /// </example>
    Condition FieldNotEquals(string fieldName, Primitive value);

    /// <summary>Creates a condition that checks whether a numeric metadata field is greater than <paramref name="value"/>.</summary>
    /// <param name="fieldName">The metadata field name.</param>
    /// <param name="value">The lower bound (exclusive).</param>
    /// <example>
    /// <code>
    /// var recent = vectorService.FieldGreaterThan("timestamp", new Primitive(1700000000L));
    /// </code>
    /// </example>
    Condition FieldGreaterThan(string fieldName, Primitive value);

    /// <summary>Creates a condition that checks whether a numeric metadata field is greater than or equal to <paramref name="value"/>.</summary>
    /// <param name="fieldName">The metadata field name.</param>
    /// <param name="value">The lower bound (inclusive).</param>
    /// <example>
    /// <code>
    /// var minScore = vectorService.FieldGreaterThanOrEqual("score", new Primitive(0.5));
    /// </code>
    /// </example>
    Condition FieldGreaterThanOrEqual(string fieldName, Primitive value);

    /// <summary>Creates a condition that checks whether a numeric metadata field is less than <paramref name="value"/>.</summary>
    /// <param name="fieldName">The metadata field name.</param>
    /// <param name="value">The upper bound (exclusive).</param>
    /// <example>
    /// <code>
    /// var lowCost = vectorService.FieldLessThan("cost", new Primitive(10.0));
    /// </code>
    /// </example>
    Condition FieldLessThan(string fieldName, Primitive value);

    /// <summary>Creates a condition that checks whether a numeric metadata field is less than or equal to <paramref name="value"/>.</summary>
    /// <param name="fieldName">The metadata field name.</param>
    /// <param name="value">The upper bound (inclusive).</param>
    /// <example>
    /// <code>
    /// var capScore = vectorService.FieldLessThanOrEqual("confidence", new Primitive(1.0));
    /// </code>
    /// </example>
    Condition FieldLessThanOrEqual(string fieldName, Primitive value);

    // ── Collection management ───────────────────────────────────────────────

    /// <summary>
    /// Ensures a named vector collection exists with the given configuration.
    /// Creates the collection if absent; is a no-op when it already exists.
    /// </summary>
    /// <param name="collectionName">The name of the collection.</param>
    /// <param name="vectorDimensions">The dimensionality of the embedding vectors stored in this collection.</param>
    /// <param name="distanceMetric">The distance metric used for similarity comparisons.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>
    /// An <see cref="OperationResult{T}"/> with <c>true</c> when the collection was created,
    /// <c>false</c> when it already existed, or a failure result on error.
    /// </returns>
    /// <example>
    /// <code>
    /// await vectorService.EnsureCollectionExistsAsync(
    ///     "documents", vectorDimensions: 1536, VectorDistanceMetric.Cosine);
    /// </code>
    /// </example>
    Task<OperationResult<bool>> EnsureCollectionExistsAsync(
        string collectionName,
        int vectorDimensions,
        VectorDistanceMetric distanceMetric,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a collection and all its points.
    /// </summary>
    /// <param name="collectionName">The name of the collection to delete.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>
    /// An <see cref="OperationResult{T}"/> with <c>true</c> on success,
    /// or a failure result if the collection does not exist or deletion fails.
    /// </returns>
    /// <example>
    /// <code>
    /// await vectorService.DeleteCollectionAsync("old-collection");
    /// </code>
    /// </example>
    Task<OperationResult<bool>> DeleteCollectionAsync(
        string collectionName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the names of all existing collections.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>An <see cref="OperationResult{T}"/> containing the collection names, or a failure result on error.</returns>
    /// <example>
    /// <code>
    /// var names = await vectorService.GetCollectionNamesAsync();
    /// </code>
    /// </example>
    Task<OperationResult<IReadOnlyList<string>>> GetCollectionNamesAsync(
        CancellationToken cancellationToken = default);

    // ── Point operations ────────────────────────────────────────────────────

    /// <summary>
    /// Upserts (inserts or updates) a single point in the specified collection.
    /// </summary>
    /// <param name="collectionName">The target collection.</param>
    /// <param name="point">The point to upsert, including its ID, vector, and optional metadata.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>An <see cref="OperationResult{T}"/> with <c>true</c> on success, or a failure result on error.</returns>
    /// <example>
    /// <code>
    /// var point = new VectorPoint
    /// {
    ///     Id = "doc-1",
    ///     Vector = embedding,
    ///     Metadata = new JObject { ["title"] = "Intro" }
    /// };
    /// await vectorService.UpsertAsync("documents", point);
    /// </code>
    /// </example>
    /// <seealso cref="LLMVectorExtensions.EmbedAndUpsertAsync"/>
    Task<OperationResult<bool>> UpsertAsync(
        string collectionName,
        VectorPoint point,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Upserts a batch of points into the specified collection in a single operation.
    /// </summary>
    /// <param name="collectionName">The target collection.</param>
    /// <param name="points">The points to upsert.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>An <see cref="OperationResult{T}"/> with <c>true</c> on success, or a failure result on error.</returns>
    /// <example>
    /// <code>
    /// await vectorService.UpsertBatchAsync("documents", points);
    /// </code>
    /// </example>
    /// <seealso cref="LLMVectorExtensions.EmbedAndUpsertBatchAsync"/>
    Task<OperationResult<bool>> UpsertBatchAsync(
        string collectionName,
        IReadOnlyList<VectorPoint> points,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a single point by ID from the specified collection.
    /// </summary>
    /// <param name="collectionName">The collection containing the point.</param>
    /// <param name="id">The string ID of the point to delete.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>
    /// An <see cref="OperationResult{T}"/> with <c>true</c> on success,
    /// or a failure result if the point was not found or deletion fails.
    /// </returns>
    /// <example>
    /// <code>
    /// await vectorService.DeleteAsync("documents", "doc-1");
    /// </code>
    /// </example>
    Task<OperationResult<bool>> DeleteAsync(
        string collectionName,
        string id,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Searches a collection for the <paramref name="topK"/> nearest neighbours of a query vector.
    /// </summary>
    /// <param name="collectionName">The collection to search.</param>
    /// <param name="queryVector">The query vector.</param>
    /// <param name="topK">The maximum number of results to return.</param>
    /// <param name="filter">
    /// Optional metadata filter. Use the condition builder methods on this interface
    /// combined with <see cref="ConditionCouplingUtilities.And(ConditionCoupling, ConditionCoupling)"/> and
    /// <see cref="ConditionCouplingUtilities.Or(ConditionCoupling, ConditionCoupling)"/> to compose complex filters.
    /// Pass <c>null</c> for an unfiltered search.
    /// </param>
    /// <param name="includeMetadata">When <c>true</c>, <see cref="VectorSearchResult.Metadata"/> is populated.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>
    /// An <see cref="OperationResult{T}"/> containing the search results ordered by descending
    /// similarity score, or a failure result on error.
    /// </returns>
    /// <example>
    /// <code>
    /// var filter = vectorService.FieldEquals("language", new Primitive("en"));
    /// var results = await vectorService.QueryAsync(
    ///     "documents", queryVector, topK: 5, filter: filter);
    /// if (results.IsSuccessful)
    ///     foreach (var r in results.Data)
    ///         Console.WriteLine($"{r.Id}: {r.Score}");
    /// </code>
    /// </example>
    /// <seealso cref="LLMVectorExtensions.SemanticSearchAsync"/>
    Task<OperationResult<IReadOnlyList<VectorSearchResult>>> QueryAsync(
        string collectionName,
        float[] queryVector,
        int topK,
        ConditionCoupling? filter = null,
        bool includeMetadata = true,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves a single point by ID from the specified collection.
    /// </summary>
    /// <param name="collectionName">The collection to search in.</param>
    /// <param name="id">The string ID of the point to retrieve.</param>
    /// <param name="includeVector">When <c>true</c>, <see cref="VectorPoint.Vector"/> is populated.</param>
    /// <param name="includeMetadata">When <c>true</c>, <see cref="VectorPoint.Metadata"/> is populated.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>
    /// An <see cref="OperationResult{T}"/> containing the point, or <c>null</c> when not found,
    /// or a failure result on error.
    /// </returns>
    /// <example>
    /// <code>
    /// var point = await vectorService.GetAsync("documents", "doc-1");
    /// if (point.IsSuccessful &amp;&amp; point.Data != null)
    ///     Console.WriteLine($"Vector length: {point.Data.Vector.Length}");
    /// </code>
    /// </example>
    Task<OperationResult<VectorPoint?>> GetAsync(
        string collectionName,
        string id,
        bool includeVector = true,
        bool includeMetadata = true,
        CancellationToken cancellationToken = default);
}
