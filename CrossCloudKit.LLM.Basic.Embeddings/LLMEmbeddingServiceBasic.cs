// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;
using System.Runtime.CompilerServices;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Interfaces.Records;
using LLama;
using LLama.Common;
using LLama.Extensions;
using LLama.Native;

namespace CrossCloudKit.LLM.Basic.Embeddings;

/// <summary>
/// CPU-only, embedding-only <see cref="ILLMService"/> implementation.
/// Uses <c>snowflake-arctic-embed-m-long</c> (Q8_0) via <c>LLamaSharp</c>;
/// the model is bundled inside the NuGet package.
/// <para>
/// Completion methods (<see cref="CompleteAsync"/> and <see cref="CompleteStreamingAsync"/>)
/// always return a <see cref="System.Net.HttpStatusCode.ServiceUnavailable"/> failure.
/// For combined embedding <em>and</em> completion, use <c>LLMServiceBasic</c> from
/// <c>CrossCloudKit.LLM.Basic</c>.
/// </para>
/// </summary>
/// <example>
/// <code>
/// ILLMService embedder = new LLMEmbeddingServiceBasic();
/// var result = await embedder.CreateEmbeddingAsync("hello world");
/// </code>
/// </example>
public sealed class LLMEmbeddingServiceBasic : ILLMService
{
    /// <summary>Filename of the bundled default embedding GGUF model.</summary>
    public const string BundledModelFileName = "snowflake-arctic-embed-m-long-Q8_0.gguf";

    private readonly LLamaWeights? _weights;
    private readonly LLamaEmbedder? _embedder;
    private bool _disposed;

    /// <inheritdoc/>
    /// <remarks><c>true</c> when the embedding model was found and loaded successfully.</remarks>
    public bool IsInitialized => _embedder is not null;

    /// <summary>
    /// Initialises a new <see cref="LLMEmbeddingServiceBasic"/>.
    /// </summary>
    /// <param name="embeddingModelPath">
    /// Optional path to a GGUF embedding model file. When <c>null</c>, falls back to the
    /// <c>LLM_BASIC_EMBEDDING_MODEL_PATH</c> environment variable, then to
    /// <c>&lt;AppBaseDir&gt;/models/snowflake-arctic-embed-m-long-Q8_0.gguf</c>.
    /// </param>
    public LLMEmbeddingServiceBasic(string? embeddingModelPath = null)
    {
        var resolvedPath = ResolveEmbeddingModelPath(embeddingModelPath);

        if (resolvedPath is not null && File.Exists(resolvedPath))
        {
            try
            {
                var modelParams = new ModelParams(resolvedPath)
                {
                    Embeddings = true,
                    PoolingType = LLamaPoolingType.Mean
                };
                _weights = LLamaWeights.LoadFromFile(modelParams);
                _embedder = new LLamaEmbedder(_weights, modelParams);
            }
            catch
            {
                _embedder = null;
                _weights?.Dispose();
                _weights = null;
            }
        }
    }

    // ── Embeddings ────────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public async Task<OperationResult<float[]>> CreateEmbeddingAsync(
        string text,
        CancellationToken cancellationToken = default)
    {
        if (_embedder is null)
            return OperationResult<float[]>.Failure(
                "Embedding model is not available. Provide a GGUF model path via the constructor " +
                "or the LLM_BASIC_EMBEDDING_MODEL_PATH environment variable.",
                HttpStatusCode.ServiceUnavailable);

        try
        {
            var embeddings = await _embedder.GetEmbeddings(text, cancellationToken);
            var vector = embeddings[0];
            vector.EuclideanNormalization();
            return OperationResult<float[]>.Success(vector);
        }
        catch (Exception ex)
        {
            return OperationResult<float[]>.Failure(ex.Message, HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc/>
    public async Task<OperationResult<IReadOnlyList<float[]>>> CreateEmbeddingsAsync(
        IReadOnlyList<string> texts,
        CancellationToken cancellationToken = default)
    {
        if (_embedder is null)
            return OperationResult<IReadOnlyList<float[]>>.Failure(
                "Embedding model is not available. Provide a GGUF model path via the constructor " +
                "or the LLM_BASIC_EMBEDDING_MODEL_PATH environment variable.",
                HttpStatusCode.ServiceUnavailable);

        try
        {
            var results = new List<float[]>(texts.Count);
            foreach (var text in texts)
            {
                var embeddings = await _embedder.GetEmbeddings(text, cancellationToken);
                var vector = embeddings[0];
                vector.EuclideanNormalization();
                results.Add(vector);
            }

            return OperationResult<IReadOnlyList<float[]>>.Success(results);
        }
        catch (Exception ex)
        {
            return OperationResult<IReadOnlyList<float[]>>.Failure(
                ex.Message, HttpStatusCode.InternalServerError);
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
            _embedder?.Dispose();
            _weights?.Dispose();
        }
        return ValueTask.CompletedTask;
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private static string? ResolveEmbeddingModelPath(string? explicitPath)
    {
        if (!string.IsNullOrWhiteSpace(explicitPath)) return explicitPath;

        var env = Environment.GetEnvironmentVariable("LLM_BASIC_EMBEDDING_MODEL_PATH");
        if (!string.IsNullOrWhiteSpace(env)) return env;

        var bundled = Path.Combine(AppContext.BaseDirectory, "models", BundledModelFileName);
        if (File.Exists(bundled)) return bundled;

        return Path.Combine(AppContext.BaseDirectory, "models", "embedding-model.gguf");
    }
}
