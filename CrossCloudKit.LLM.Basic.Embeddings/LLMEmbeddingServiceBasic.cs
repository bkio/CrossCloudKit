// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;
using System.Runtime.CompilerServices;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Interfaces.Records;
using SmartComponents.LocalEmbeddings;

namespace CrossCloudKit.LLM.Basic.Embeddings;

/// <summary>
/// CPU-only, embedding-only <see cref="ILLMService"/> implementation.
/// Uses <c>all-MiniLM-L6-v2</c> via <c>SmartComponents.LocalEmbeddings</c>;
/// the model weights are bundled automatically by that package.
/// <para>
/// Completion methods (<see cref="CompleteAsync"/> and <see cref="CompleteStreamingAsync"/>)
/// always return a <see cref="System.Net.HttpStatusCode.ServiceUnavailable"/> failure.
/// For combined embedding <em>and</em> completion, use <c>LLMServiceBasic</c> from
/// <c>CrossCloudKit.LLM.Basic</c>.
/// </para>
/// </summary>
public sealed class LLMEmbeddingServiceBasic : ILLMService
{
    private readonly LocalEmbedder _embedder;
    private bool _disposed;

    /// <inheritdoc/>
    /// <remarks>Always <c>true</c> — embedding is always available.</remarks>
    public bool IsInitialized => true;

    /// <summary>Initialises a new <see cref="LLMEmbeddingServiceBasic"/>.</summary>
    public LLMEmbeddingServiceBasic()
    {
        _embedder = new LocalEmbedder();
    }

    // ── Embeddings ────────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public Task<OperationResult<float[]>> CreateEmbeddingAsync(
        string text,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var embedding = _embedder.Embed(text);
            return Task.FromResult(OperationResult<float[]>.Success(embedding.Values.ToArray()));
        }
        catch (Exception ex)
        {
            return Task.FromResult(
                OperationResult<float[]>.Failure(ex.Message, HttpStatusCode.InternalServerError));
        }
    }

    /// <inheritdoc/>
    public Task<OperationResult<IReadOnlyList<float[]>>> CreateEmbeddingsAsync(
        IReadOnlyList<string> texts,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var results = new List<float[]>(texts.Count);
            foreach (var text in texts)
            {
                var embedding = _embedder.Embed(text);
                results.Add(embedding.Values.ToArray());
            }

            return Task.FromResult(
                OperationResult<IReadOnlyList<float[]>>.Success(results));
        }
        catch (Exception ex)
        {
            return Task.FromResult(
                OperationResult<IReadOnlyList<float[]>>.Failure(
                    ex.Message, HttpStatusCode.InternalServerError));
        }
    }

    // ── Completions (not supported) ───────────────────────────────────────────

    /// <inheritdoc/>
    /// <remarks>Not supported by this sub-package. Returns <see cref="HttpStatusCode.ServiceUnavailable"/>.</remarks>
    public Task<OperationResult<LLMResponse>> CompleteAsync(
        LLMRequest request,
        CancellationToken cancellationToken = default)
        => Task.FromResult(OperationResult<LLMResponse>.Failure(
            "Completions are not supported by LLMEmbeddingServiceBasic. " +
            "Use LLMServiceBasic from CrossCloudKit.LLM.Basic for combined embedding + completion support.",
            HttpStatusCode.ServiceUnavailable));

    /// <inheritdoc/>
    /// <remarks>Not supported by this sub-package. Yields a single failure chunk.</remarks>
    public async IAsyncEnumerable<OperationResult<LLMStreamChunk>> CompleteStreamingAsync(
        LLMRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        yield return OperationResult<LLMStreamChunk>.Failure(
            "Completions are not supported by LLMEmbeddingServiceBasic. " +
            "Use LLMServiceBasic from CrossCloudKit.LLM.Basic for combined embedding + completion support.",
            HttpStatusCode.ServiceUnavailable);
        await Task.CompletedTask;
    }

    // ── IAsyncDisposable ──────────────────────────────────────────────────────

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            _embedder.Dispose();
        }
        return ValueTask.CompletedTask;
    }
}
