// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Interfaces.Enums;
using CrossCloudKit.Interfaces.Records;
using LLama;
using LLama.Common;
using LLama.Sampling;

namespace CrossCloudKit.LLM.Basic.Completion;

/// <summary>
/// CPU-only, completion-only <see cref="ILLMService"/> implementation.
/// <para>
/// <b>Bundled model</b> — ships with SmolLM2-135M-Instruct (Q8_0, ~139 MB, Apache-2.0).
/// Completions work out of the box with zero configuration. To use a different model,
/// pass a GGUF path via the constructor’s <c>completionModelPath</c> parameter or the
/// <c>LLM_BASIC_MODEL_PATH</c> environment variable.
/// </para>
/// <para>
/// Embedding methods (<see cref="CreateEmbeddingAsync"/> and <see cref="CreateEmbeddingsAsync"/>)
/// always return a <see cref="System.Net.HttpStatusCode.ServiceUnavailable"/> failure.
/// For combined embedding <em>and</em> completion, use <c>LLMServiceBasic</c> from
/// <c>CrossCloudKit.LLM.Basic</c>.
/// </para>
/// </summary>
/// <example>
/// <code>
/// // Zero-config (uses bundled SmolLM2-135M model)
/// ILLMService llm = new LLMCompletionServiceBasic();
///
/// // Custom GGUF model
/// ILLMService llm = new LLMCompletionServiceBasic(
///     completionModelPath: "/models/my-model.gguf",
///     contextSize: 4096);
/// </code>
/// </example>
public sealed class LLMCompletionServiceBasic : ILLMService
{
    /// <summary>Filename of the bundled default GGUF model.</summary>
    public const string BundledModelFileName = "SmolLM2-135M-Instruct-Q8_0.gguf";

    private readonly LLamaWeights? _completionModel;
    private readonly ModelParams? _completionModelParams;
    private readonly int _contextSize;
    private bool _disposed;

    /// <inheritdoc/>
    /// <remarks><c>true</c> when a GGUF model was found and loaded successfully.</remarks>
    public bool IsInitialized => _completionModel is not null;

    /// <summary>
    /// <c>true</c> when a GGUF model file was found and loaded successfully,
    /// meaning <see cref="CompleteAsync"/> and <see cref="CompleteStreamingAsync"/> are usable.
    /// </summary>
    public bool IsCompletionAvailable => _completionModel is not null;

    /// <summary>
    /// Initialises a new <see cref="LLMCompletionServiceBasic"/>.
    /// </summary>
    /// <param name="completionModelPath">
    /// Optional path to a GGUF model file. When <c>null</c>, falls back to the
    /// <c>LLM_BASIC_MODEL_PATH</c> environment variable, then to
    /// <c>&lt;AppBaseDir&gt;/models/completion-model.gguf</c>.
    /// Completions are disabled if the file cannot be found.
    /// </param>
    /// <param name="contextSize">The LLM context window size in tokens (default: 2048).</param>
    /// <param name="gpuLayerCount">Number of model layers to offload to GPU (default: 0 = CPU-only).</param>
    public LLMCompletionServiceBasic(
        string? completionModelPath = null,
        int contextSize = 2048,
        int gpuLayerCount = 0)
    {
        _contextSize = contextSize;

        var resolvedPath = ResolveCompletionModelPath(completionModelPath);

        if (resolvedPath is not null && File.Exists(resolvedPath))
        {
            try
            {
                _completionModelParams = new ModelParams(resolvedPath)
                {
                    ContextSize = (uint)contextSize,
                    GpuLayerCount = gpuLayerCount
                };
                _completionModel = LLamaWeights.LoadFromFile(_completionModelParams);
            }
            catch
            {
                _completionModel = null;
                _completionModelParams = null;
            }
        }
    }

    // ── Completions ───────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public async Task<OperationResult<LLMResponse>> CompleteAsync(
        LLMRequest request,
        CancellationToken cancellationToken = default)
    {
        if (_completionModel is null || _completionModelParams is null)
            return OperationResult<LLMResponse>.Failure(
                "Completion model is not available. Provide a GGUF model path via the constructor " +
                "or the LLM_BASIC_MODEL_PATH environment variable.",
                HttpStatusCode.ServiceUnavailable);

        try
        {
            var prompt = FormatChatMlPrompt(request);
            var builder = new StringBuilder();

            var executor = new StatelessExecutor(_completionModel, _completionModelParams);
            var inferParams = BuildInferenceParams(request);

            await foreach (var token in executor.InferAsync(prompt, inferParams, cancellationToken))
                builder.Append(token);

            return OperationResult<LLMResponse>.Success(new LLMResponse
            {
                Content      = builder.ToString().Trim(),
                FinishReason = LLMFinishReason.Stop
            });
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return OperationResult<LLMResponse>.Failure("Request was cancelled.", HttpStatusCode.RequestTimeout);
        }
        catch (Exception ex)
        {
            return OperationResult<LLMResponse>.Failure(ex.Message, HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<OperationResult<LLMStreamChunk>> CompleteStreamingAsync(
        LLMRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (_completionModel is null || _completionModelParams is null)
        {
            yield return OperationResult<LLMStreamChunk>.Failure(
                "Completion model is not available. Provide a GGUF model path via the constructor " +
                "or the LLM_BASIC_MODEL_PATH environment variable.",
                HttpStatusCode.ServiceUnavailable);
            yield break;
        }

        // ── Phase 1: Setup (no yield → try-catch is allowed) ──────────────
        IAsyncEnumerator<string>? enumerator = null;
        OperationResult<LLMStreamChunk>? setupError = null;
        try
        {
            var prompt = FormatChatMlPrompt(request);
            var executor = new StatelessExecutor(_completionModel, _completionModelParams);
            var inferParams = BuildInferenceParams(request);
            enumerator = executor.InferAsync(prompt, inferParams, cancellationToken)
                .GetAsyncEnumerator(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            setupError = OperationResult<LLMStreamChunk>.Failure(
                "Request was cancelled.", HttpStatusCode.RequestTimeout);
        }
        catch (Exception ex)
        {
            setupError = OperationResult<LLMStreamChunk>.Failure(
                ex.Message, HttpStatusCode.InternalServerError);
        }

        if (setupError is not null)
        {
            yield return setupError;
            yield break;
        }

        // ── Phase 2: Stream reading (yield inside try-finally) ────────────
        var activeEnumerator = enumerator!;
        try
        {
            while (true)
            {
                bool hasNext = false;
                string? token = null;
                Exception? stepEx = null;

                try
                {
                    hasNext = await activeEnumerator.MoveNextAsync();
                    if (hasNext) token = activeEnumerator.Current;
                }
                catch (OperationCanceledException oce) when (cancellationToken.IsCancellationRequested)
                {
                    stepEx = oce;
                }
                catch (Exception ex)
                {
                    stepEx = ex;
                }

                if (stepEx is OperationCanceledException)
                {
                    yield return OperationResult<LLMStreamChunk>.Failure(
                        "Request was cancelled.", HttpStatusCode.RequestTimeout);
                    yield break;
                }

                if (stepEx is not null)
                {
                    yield return OperationResult<LLMStreamChunk>.Failure(
                        stepEx.Message, HttpStatusCode.InternalServerError);
                    yield break;
                }

                if (!hasNext) break;

                yield return OperationResult<LLMStreamChunk>.Success(
                    new LLMStreamChunk { ContentDelta = token! });
            }
        }
        finally
        {
            await activeEnumerator.DisposeAsync();
        }

        yield return OperationResult<LLMStreamChunk>.Success(
            new LLMStreamChunk { IsFinal = true, FinishReason = LLMFinishReason.Stop });
    }

    // ── Embeddings (not supported) ────────────────────────────────────────────

    /// <inheritdoc/>
    /// <remarks>Not supported by this sub-package. Returns <see cref="HttpStatusCode.ServiceUnavailable"/>.</remarks>
    public Task<OperationResult<float[]>> CreateEmbeddingAsync(
        string text,
        CancellationToken cancellationToken = default)
        => Task.FromResult(OperationResult<float[]>.Failure(
            "Embeddings are not supported by LLMCompletionServiceBasic. " +
            "Use LLMServiceBasic from CrossCloudKit.LLM.Basic for combined embedding + completion support.",
            HttpStatusCode.ServiceUnavailable));

    /// <inheritdoc/>
    /// <remarks>Not supported by this sub-package. Returns <see cref="HttpStatusCode.ServiceUnavailable"/>.</remarks>
    public Task<OperationResult<IReadOnlyList<float[]>>> CreateEmbeddingsAsync(
        IReadOnlyList<string> texts,
        CancellationToken cancellationToken = default)
        => Task.FromResult(OperationResult<IReadOnlyList<float[]>>.Failure(
            "Embeddings are not supported by LLMCompletionServiceBasic. " +
            "Use LLMServiceBasic from CrossCloudKit.LLM.Basic for combined embedding + completion support.",
            HttpStatusCode.ServiceUnavailable));

    // ── IAsyncDisposable ──────────────────────────────────────────────────────

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            _completionModel?.Dispose();
        }
        return ValueTask.CompletedTask;
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private static string? ResolveCompletionModelPath(string? explicitPath)
    {
        if (!string.IsNullOrWhiteSpace(explicitPath)) return explicitPath;

        var env = Environment.GetEnvironmentVariable("LLM_BASIC_MODEL_PATH");
        if (!string.IsNullOrWhiteSpace(env)) return env;

        // Check for the bundled default model (shipped inside the NuGet package)
        var bundled = Path.Combine(AppContext.BaseDirectory, "models", BundledModelFileName);
        if (File.Exists(bundled)) return bundled;

        // Legacy fallback path
        return Path.Combine(AppContext.BaseDirectory, "models", "completion-model.gguf");
    }

    private static string FormatChatMlPrompt(LLMRequest request)
    {
        var sb = new StringBuilder();
        foreach (var msg in request.Messages)
        {
            var role = msg.Role switch
            {
                LLMRole.System    => "system",
                LLMRole.User      => "user",
                LLMRole.Assistant => "assistant",
                LLMRole.Tool      => "tool",
                _                 => "user"
            };
            sb.Append("<|im_start|>").Append(role).Append('\n')
              .Append(msg.Content).Append("<|im_end|>\n");
        }
        sb.Append("<|im_start|>assistant\n");
        return sb.ToString();
    }

    private static InferenceParams BuildInferenceParams(LLMRequest request)
    {
        return new InferenceParams
        {
            MaxTokens        = request.MaxTokens ?? 512,
            AntiPrompts      = ["<|im_end|>", "<|im_start|>"],
            SamplingPipeline = new DefaultSamplingPipeline
            {
                Temperature = (float)(request.Temperature ?? 0.7)
            }
        };
    }
}
