// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Interfaces.Records;
using CrossCloudKit.LLM.Basic.Completion;
using CrossCloudKit.LLM.Basic.Embeddings;

namespace CrossCloudKit.LLM.Basic;

/// <summary>
/// Fully local (CPU-only) <see cref="ILLMService"/> implementation.
/// Combines <see cref="LLMEmbeddingServiceBasic"/> (all-MiniLM-L6-v2 via SmartComponents)
/// and <see cref="LLMCompletionServiceBasic"/> (GGUF models via LLamaSharp).
/// <list type="bullet">
///   <item><description>
///     <b>Embeddings</b> — uses <c>all-MiniLM-L6-v2</c> via
///     <c>SmartComponents.LocalEmbeddings</c>; the model is managed automatically by that package.
///   </description></item>
///   <item><description>
///     <b>Completions</b> — bundles SmolLM2-135M-Instruct (Q8_0, ~139 MB, Apache-2.0)
///     via <c>CrossCloudKit.LLM.Basic.Completion</c>. Completions work out of the box.
///     To use a different model, pass a GGUF path via the constructor’s
///     <c>completionModelPath</c> parameter, or set the <c>LLM_BASIC_MODEL_PATH</c>
///     environment variable.
///   </description></item>
/// </list>
/// <para>
/// If you only need embeddings, use <c>LLMEmbeddingServiceBasic</c> from
/// <c>CrossCloudKit.LLM.Basic.Embeddings</c>. If you only need completions, use
/// <c>LLMCompletionServiceBasic</c> from <c>CrossCloudKit.LLM.Basic.Completion</c>.
/// </para>
/// </summary>
public sealed class LLMServiceBasic : ILLMService
{
    private readonly LLMEmbeddingServiceBasic _embeddingService;
    private readonly LLMCompletionServiceBasic _completionService;
    private bool _disposed;

    /// <inheritdoc/>
    /// <remarks>Always <c>true</c> — embedding is always available. Use
    /// <see cref="IsCompletionAvailable"/> to check whether completions are usable.</remarks>
    public bool IsInitialized => true;

    /// <summary>
    /// <c>true</c> when a GGUF model file was found and loaded successfully,
    /// meaning <see cref="CompleteAsync"/> and <see cref="CompleteStreamingAsync"/>
    /// are usable.
    /// </summary>
    public bool IsCompletionAvailable => _completionService.IsCompletionAvailable;

    /// <summary>
    /// Initialises a new <see cref="LLMServiceBasic"/>.
    /// </summary>
    /// <param name="completionModelPath">
    /// Optional path to a GGUF model file for text completion.
    /// When <c>null</c>, falls back to the <c>LLM_BASIC_MODEL_PATH</c> environment variable,
    /// then to <c>&lt;AppBaseDir&gt;/models/completion-model.gguf</c>.
    /// Completions are disabled if the file cannot be found.
    /// </param>
    /// <param name="contextSize">The LLM context window size in tokens (default: 2048).</param>
    /// <param name="gpuLayerCount">
    /// Number of model layers to offload to GPU. Defaults to 0 (CPU-only).
    /// </param>
    public LLMServiceBasic(
        string? completionModelPath = null,
        int contextSize = 2048,
        int gpuLayerCount = 0)
    {
        _embeddingService  = new LLMEmbeddingServiceBasic();
        _completionService = new LLMCompletionServiceBasic(completionModelPath, contextSize, gpuLayerCount);
    }

    // ── Embeddings ────────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public Task<OperationResult<float[]>> CreateEmbeddingAsync(
        string text,
        CancellationToken cancellationToken = default)
        => _embeddingService.CreateEmbeddingAsync(text, cancellationToken);

    /// <inheritdoc/>
    public Task<OperationResult<IReadOnlyList<float[]>>> CreateEmbeddingsAsync(
        IReadOnlyList<string> texts,
        CancellationToken cancellationToken = default)
        => _embeddingService.CreateEmbeddingsAsync(texts, cancellationToken);

    // ── Completions ───────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public Task<OperationResult<LLMResponse>> CompleteAsync(
        LLMRequest request,
        CancellationToken cancellationToken = default)
        => _completionService.CompleteAsync(request, cancellationToken);

    /// <inheritdoc/>
    public async IAsyncEnumerable<OperationResult<LLMStreamChunk>> CompleteStreamingAsync(
        LLMRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var chunk in _completionService.CompleteStreamingAsync(request, cancellationToken))
            yield return chunk;
    }

    // ── IAsyncDisposable ──────────────────────────────────────────────────────

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            await _embeddingService.DisposeAsync();
            await _completionService.DisposeAsync();
        }
    }
}
