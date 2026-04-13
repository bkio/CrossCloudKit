// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;
using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Interfaces.Records;
using CrossCloudKit.Utilities.Common;
using Newtonsoft.Json.Linq;

namespace CrossCloudKit.Interfaces;

/// <summary>
/// Extension methods that bridge <see cref="ILLMService"/> and <see cref="IVectorService"/>,
/// enabling common embed-then-store and embed-then-search workflows.
/// </summary>
public static class LLMVectorExtensions
{
    /// <summary>
    /// Embeds <paramref name="text"/> via the LLM service and upserts the resulting vector
    /// into the specified collection.
    /// </summary>
    /// <param name="vectorService">The vector service to upsert into.</param>
    /// <param name="llmService">The LLM service used to create the embedding.</param>
    /// <param name="collectionName">The target vector collection.</param>
    /// <param name="id">The point identifier.</param>
    /// <param name="text">The text to embed.</param>
    /// <param name="metadata">Optional metadata to attach to the point.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>
    /// An <see cref="OperationResult{T}"/> with <c>true</c> on success, or a failure result
    /// if the embedding or upsert step fails.
    /// </returns>
    /// <example>
    /// <code>
    /// await vectorService.EmbedAndUpsertAsync(
    ///     llmService, "documents", "doc-1", "CrossCloudKit is a cloud-agnostic framework.",
    ///     metadata: new JObject { ["source"] = "readme" });
    /// </code>
    /// </example>
    public static async Task<OperationResult<bool>> EmbedAndUpsertAsync(
        this IVectorService vectorService,
        ILLMService llmService,
        string collectionName,
        string id,
        string text,
        JObject? metadata = null,
        CancellationToken cancellationToken = default)
    {
        var embeddingResult = await llmService.CreateEmbeddingAsync(text, cancellationToken);
        if (!embeddingResult.IsSuccessful)
            return OperationResult<bool>.Failure(embeddingResult.ErrorMessage, embeddingResult.StatusCode);

        return await vectorService.UpsertAsync(collectionName, new VectorPoint
        {
            Id       = id,
            Vector   = embeddingResult.Data,
            Metadata = metadata
        }, cancellationToken);
    }

    /// <summary>
    /// Embeds a batch of texts via the LLM service and upserts the resulting vectors into
    /// the specified collection in a single batch operation.
    /// </summary>
    /// <param name="vectorService">The vector service to upsert into.</param>
    /// <param name="llmService">The LLM service used to create embeddings.</param>
    /// <param name="collectionName">The target vector collection.</param>
    /// <param name="items">
    /// Items to embed and upsert. Each element contains the point ID, the text to embed,
    /// and optional metadata.
    /// </param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>
    /// An <see cref="OperationResult{T}"/> with <c>true</c> on success, or a failure result
    /// if the embedding or upsert step fails.
    /// </returns>
    /// <example>
    /// <code>
    /// var items = new List&lt;(string Id, string Text, JObject? Metadata)&gt;
    /// {
    ///     ("doc-1", "First document text.", new JObject { ["page"] = 1 }),
    ///     ("doc-2", "Second document text.", null)
    /// };
    /// await vectorService.EmbedAndUpsertBatchAsync(llmService, "documents", items);
    /// </code>
    /// </example>
    public static async Task<OperationResult<bool>> EmbedAndUpsertBatchAsync(
        this IVectorService vectorService,
        ILLMService llmService,
        string collectionName,
        IReadOnlyList<(string Id, string Text, JObject? Metadata)> items,
        CancellationToken cancellationToken = default)
    {
        if (items.Count == 0)
            return OperationResult<bool>.Success(true);

        var texts = items.Select(i => i.Text).ToList();
        var embeddingsResult = await llmService.CreateEmbeddingsAsync(texts, cancellationToken);
        if (!embeddingsResult.IsSuccessful)
            return OperationResult<bool>.Failure(embeddingsResult.ErrorMessage, embeddingsResult.StatusCode);

        if (embeddingsResult.Data.Count != items.Count)
            return OperationResult<bool>.Failure(
                $"Embedding count mismatch: expected {items.Count} but the LLM service returned {embeddingsResult.Data.Count}.",
                HttpStatusCode.BadGateway);

        var points = new List<VectorPoint>(items.Count);
        for (int i = 0; i < items.Count; i++)
        {
            points.Add(new VectorPoint
            {
                Id       = items[i].Id,
                Vector   = embeddingsResult.Data[i],
                Metadata = items[i].Metadata
            });
        }

        return await vectorService.UpsertBatchAsync(collectionName, points, cancellationToken);
    }

    /// <summary>
    /// Embeds <paramref name="queryText"/> via the LLM service and performs a similarity
    /// search against the specified collection.
    /// </summary>
    /// <param name="vectorService">The vector service to search.</param>
    /// <param name="llmService">The LLM service used to create the query embedding.</param>
    /// <param name="collectionName">The collection to search.</param>
    /// <param name="queryText">The natural-language query to embed.</param>
    /// <param name="topK">The maximum number of results to return.</param>
    /// <param name="filter">Optional metadata filter.</param>
    /// <param name="includeMetadata">When <c>true</c>, result metadata is populated.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>
    /// An <see cref="OperationResult{T}"/> containing results ordered by descending similarity,
    /// or a failure result if the embedding or query step fails.
    /// </returns>
    /// <example>
    /// <code>
    /// var results = await vectorService.SemanticSearchAsync(
    ///     llmService, "documents", "What is CrossCloudKit?", topK: 5);
    /// if (results.IsSuccessful)
    ///     foreach (var r in results.Data)
    ///         Console.WriteLine($"{r.Id}: score={r.Score}");
    /// </code>
    /// </example>
    public static async Task<OperationResult<IReadOnlyList<VectorSearchResult>>> SemanticSearchAsync(
        this IVectorService vectorService,
        ILLMService llmService,
        string collectionName,
        string queryText,
        int topK,
        ConditionCoupling? filter = null,
        bool includeMetadata = true,
        CancellationToken cancellationToken = default)
    {
        var embeddingResult = await llmService.CreateEmbeddingAsync(queryText, cancellationToken);
        if (!embeddingResult.IsSuccessful)
            return OperationResult<IReadOnlyList<VectorSearchResult>>.Failure(
                embeddingResult.ErrorMessage, embeddingResult.StatusCode);

        return await vectorService.QueryAsync(
            collectionName,
            embeddingResult.Data,
            topK,
            filter,
            includeMetadata,
            cancellationToken);
    }
}
