// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using CrossCloudKit.Basic.DebugPanel;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Interfaces.Enums;
using CrossCloudKit.Interfaces.Records;
using CrossCloudKit.LLM.Basic.Completion;
using CrossCloudKit.LLM.Basic.Embeddings;

namespace CrossCloudKit.LLM.Basic;

/// <summary>
/// Fully local (CPU-only) <see cref="ILLMService"/> implementation.
/// Combines <see cref="LLMEmbeddingServiceBasic"/> (snowflake-arctic-embed-m-long via LLamaSharp)
/// and <see cref="LLMCompletionServiceBasic"/> (GGUF models via LLamaSharp).
/// <list type="bullet">
///   <item><description>
///     <b>Embeddings</b> — uses <c>snowflake-arctic-embed-m-long</c> (Q8_0) via
///     <c>LLamaSharp</c>; the model is bundled inside the NuGet package.
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
    private readonly LLMRequestLog _requestLog = new();
    private DebugTracker? _debugTracker;
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
    /// <param name="embeddingModelPath">
    /// Optional path to a GGUF embedding model file.
    /// When <c>null</c>, falls back to the <c>LLM_BASIC_EMBEDDING_MODEL_PATH</c> environment variable,
    /// then to <c>&lt;AppBaseDir&gt;/models/snowflake-arctic-embed-m-long-Q8_0.gguf</c>.
    /// </param>
    /// <param name="contextSize">The LLM context window size in tokens (default: 2048).</param>
    /// <param name="gpuLayerCount">
    /// Number of model layers to offload to GPU. Defaults to 0 (CPU-only).
    /// </param>
    public LLMServiceBasic(
        string? completionModelPath = null,
        string? embeddingModelPath = null,
        int contextSize = 2048,
        int gpuLayerCount = 0)
    {
        _embeddingService  = new LLMEmbeddingServiceBasic(embeddingModelPath);
        _completionService = new LLMCompletionServiceBasic(completionModelPath, contextSize, gpuLayerCount);

        var resolvedCompletionPath = ResolveCompletionModelPath(completionModelPath);
        var resolvedEmbeddingPath = ResolveEmbeddingModelPath(embeddingModelPath);

        var completionModel = new LLMModelInfo
        {
            Available = _completionService.IsCompletionAvailable,
            Path = resolvedCompletionPath,
            FileSizeBytes = GetFileSize(resolvedCompletionPath),
            ContextSize = contextSize
        };
        var embeddingModel = new LLMModelInfo
        {
            Available = _embeddingService.IsInitialized,
            Path = resolvedEmbeddingPath,
            FileSizeBytes = GetFileSize(resolvedEmbeddingPath)
        };

        var provider = new LLMDebugDataProvider(_requestLog, completionModel, embeddingModel);
        var displayPath = resolvedCompletionPath ?? resolvedEmbeddingPath ?? "LLM.Basic";
        try { _debugTracker = DebugPanelCoordinator.RegisterAsync("LLM", displayPath, provider).GetAwaiter().GetResult(); }
        catch { /* Debug panel is non-critical */ }
    }

    // ── Embeddings ────────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public async Task<OperationResult<float[]>> CreateEmbeddingAsync(
        string text,
        CancellationToken cancellationToken = default)
    {
        var sw = Stopwatch.StartNew();
        var result = await _embeddingService.CreateEmbeddingAsync(text, cancellationToken);
        sw.Stop();

        _requestLog.Add(new LLMRequestLogEntry
        {
            RequestType = LLMRequestType.Embedding,
            TimestampUtc = DateTime.UtcNow,
            Duration = sw.Elapsed,
            Success = result.IsSuccessful,
            ErrorMessage = result.IsSuccessful ? null : result.ErrorMessage,
            TextCount = 1,
            Dimensions = result.IsSuccessful ? result.Data?.Length : null,
            FirstTextPreview = Truncate(text, 500)
        });
        _debugTracker?.BeginOperation("CreateEmbedding", $"len={text.Length}, ok={result.IsSuccessful}")?.Dispose();

        return result;
    }

    /// <inheritdoc/>
    public async Task<OperationResult<IReadOnlyList<float[]>>> CreateEmbeddingsAsync(
        IReadOnlyList<string> texts,
        CancellationToken cancellationToken = default)
    {
        var sw = Stopwatch.StartNew();
        var result = await _embeddingService.CreateEmbeddingsAsync(texts, cancellationToken);
        sw.Stop();

        _requestLog.Add(new LLMRequestLogEntry
        {
            RequestType = LLMRequestType.EmbeddingBatch,
            TimestampUtc = DateTime.UtcNow,
            Duration = sw.Elapsed,
            Success = result.IsSuccessful,
            ErrorMessage = result.IsSuccessful ? null : result.ErrorMessage,
            TextCount = texts.Count,
            Dimensions = result.IsSuccessful && result.Data?.Count > 0 ? result.Data[0].Length : null,
            FirstTextPreview = texts.Count > 0 ? Truncate(texts[0], 500) : null
        });
        _debugTracker?.BeginOperation("CreateEmbeddings", $"count={texts.Count}, ok={result.IsSuccessful}")?.Dispose();

        return result;
    }

    // ── Completions ───────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public async Task<OperationResult<LLMResponse>> CompleteAsync(
        LLMRequest request,
        CancellationToken cancellationToken = default)
    {
        var sw = Stopwatch.StartNew();
        var result = await _completionService.CompleteAsync(request, cancellationToken);
        sw.Stop();

        var lastUserMsg = request.Messages.LastOrDefault(m => m.Role == LLMRole.User)?.Content;

        _requestLog.Add(new LLMRequestLogEntry
        {
            RequestType = LLMRequestType.Completion,
            TimestampUtc = DateTime.UtcNow,
            Duration = sw.Elapsed,
            Success = result.IsSuccessful,
            ErrorMessage = result.IsSuccessful ? null : result.ErrorMessage,
            PromptPreview = Truncate(lastUserMsg, 500),
            ResponsePreview = result.IsSuccessful ? Truncate(result.Data?.Content, 500) : null,
            FinishReason = result.IsSuccessful ? result.Data?.FinishReason.ToString() : null,
            PromptTokenEstimate = EstimateTokens(request.Messages),
            CompletionTokenEstimate = result.IsSuccessful ? EstimateTokens(result.Data?.Content) : null
        });
        _debugTracker?.BeginOperation("Complete", $"ok={result.IsSuccessful}")?.Dispose();

        return result;
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<OperationResult<LLMStreamChunk>> CompleteStreamingAsync(
        LLMRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var sw = Stopwatch.StartNew();
        var lastUserMsg = request.Messages.LastOrDefault(m => m.Role == LLMRole.User)?.Content;
        var responseBuilder = new System.Text.StringBuilder();
        var success = true;
        string? errorMessage = null;
        string? finishReason = null;

        await foreach (var chunk in _completionService.CompleteStreamingAsync(request, cancellationToken))
        {
            if (chunk.IsSuccessful && chunk.Data is not null)
            {
                if (!string.IsNullOrEmpty(chunk.Data.ContentDelta))
                    responseBuilder.Append(chunk.Data.ContentDelta);
                if (chunk.Data.IsFinal)
                    finishReason = chunk.Data.FinishReason.ToString();
            }
            else if (!chunk.IsSuccessful)
            {
                success = false;
                errorMessage = chunk.ErrorMessage;
            }

            yield return chunk;
        }

        sw.Stop();

        var response = responseBuilder.ToString();
        _requestLog.Add(new LLMRequestLogEntry
        {
            RequestType = LLMRequestType.CompletionStreaming,
            TimestampUtc = DateTime.UtcNow,
            Duration = sw.Elapsed,
            Success = success,
            ErrorMessage = errorMessage,
            PromptPreview = Truncate(lastUserMsg, 500),
            ResponsePreview = Truncate(response, 500),
            FinishReason = finishReason,
            PromptTokenEstimate = EstimateTokens(request.Messages),
            CompletionTokenEstimate = EstimateTokens(response)
        });
        _debugTracker?.BeginOperation("CompleteStreaming", $"ok={success}")?.Dispose();
    }

    // ── IAsyncDisposable ──────────────────────────────────────────────────────

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;

            if (_debugTracker is not null)
            {
                try { await DebugPanelCoordinator.DeregisterAsync(_debugTracker.InstanceId); }
                catch { /* non-critical */ }
            }

            await _embeddingService.DisposeAsync();
            await _completionService.DisposeAsync();
        }
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private static string? Truncate(string? value, int maxLen)
    {
        if (string.IsNullOrEmpty(value)) return value;
        return value.Length <= maxLen ? value : value[..maxLen] + "…";
    }

    /// <summary>Rough token estimate: ~4 chars per token for English text.</summary>
    private static int EstimateTokens(IEnumerable<LLMMessage>? messages)
    {
        if (messages is null) return 0;
        return messages.Sum(m => (m.Content?.Length ?? 0)) / 4;
    }

    private static int EstimateTokens(string? text)
    {
        return (text?.Length ?? 0) / 4;
    }

    private static long GetFileSize(string? path)
    {
        if (string.IsNullOrEmpty(path)) return -1;
        try { return File.Exists(path) ? new FileInfo(path).Length : -1; }
        catch { return -1; }
    }

    private static string? ResolveCompletionModelPath(string? explicitPath)
    {
        if (!string.IsNullOrWhiteSpace(explicitPath)) return explicitPath;
        var env = Environment.GetEnvironmentVariable("LLM_BASIC_MODEL_PATH");
        if (!string.IsNullOrWhiteSpace(env)) return env;
        var bundled = Path.Combine(AppContext.BaseDirectory, "models", LLMCompletionServiceBasic.BundledModelFileName);
        return File.Exists(bundled) ? bundled : Path.Combine(AppContext.BaseDirectory, "models", "completion-model.gguf");
    }

    private static string? ResolveEmbeddingModelPath(string? explicitPath)
    {
        if (!string.IsNullOrWhiteSpace(explicitPath)) return explicitPath;
        var env = Environment.GetEnvironmentVariable("LLM_BASIC_EMBEDDING_MODEL_PATH");
        if (!string.IsNullOrWhiteSpace(env)) return env;
        var bundled = Path.Combine(AppContext.BaseDirectory, "models", LLMEmbeddingServiceBasic.BundledModelFileName);
        return File.Exists(bundled) ? bundled : Path.Combine(AppContext.BaseDirectory, "models", "embedding-model.gguf");
    }
}
