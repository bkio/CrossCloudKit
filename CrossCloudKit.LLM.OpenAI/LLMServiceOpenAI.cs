// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Text;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Interfaces.Enums;
using CrossCloudKit.Interfaces.Records;
using CrossCloudKit.Utilities.Common;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CrossCloudKit.LLM.OpenAI;

/// <summary>
/// <see cref="ILLMService"/> implementation that targets any OpenAI-compatible REST endpoint.
/// Compatible with OpenAI, Azure OpenAI, Google Gemini (via compatibility layer),
/// Groq, Amazon Bedrock, Ollama, and LM Studio.
/// </summary>
/// <example>
/// <code>
/// // OpenAI
/// ILLMService llm = new LLMServiceOpenAI(
///     baseUrl: "https://api.openai.com/v1",
///     apiKey: "sk-...",
///     defaultModel: "gpt-4o-mini",
///     embeddingModel: "text-embedding-3-small");
///
/// // Local Ollama
/// ILLMService llm = new LLMServiceOpenAI(
///     baseUrl: "http://localhost:11434/v1",
///     defaultModel: "llama3");
/// </code>
/// </example>
public sealed class LLMServiceOpenAI : ILLMService
{
    private readonly HttpClient _httpClient;
    private readonly string _defaultModel;
    private readonly string _embeddingModel;
    private readonly bool _ownsHttpClient;
    private bool _disposed;

    /// <inheritdoc/>
    public bool IsInitialized { get; }

    /// <summary>
    /// Initialises a new <see cref="LLMServiceOpenAI"/> connected to an OpenAI-compatible endpoint.
    /// </summary>
    /// <param name="baseUrl">
    /// The base URL of the API endpoint, e.g. <c>https://api.openai.com/v1</c> or
    /// <c>http://localhost:11434/v1</c> for Ollama. Must not be <c>null</c>.
    /// </param>
    /// <param name="apiKey">
    /// The API key. Pass an empty string for endpoints that do not require authentication
    /// (e.g. locally-running Ollama).
    /// </param>
    /// <param name="defaultModel">
    /// The model identifier used when <see cref="LLMRequest.Model"/> is <c>null</c>.
    /// </param>
    /// <param name="embeddingModel">
    /// The model identifier used for embedding requests.
    /// When <c>null</c>, falls back to <paramref name="defaultModel"/>.
    /// Set this to a dedicated embedding model (e.g. <c>nomic-embed-text:v1.5</c> for Ollama,
    /// <c>text-embedding-3-small</c> for OpenAI) when your completion and embedding
    /// models differ — which is the common case on self-hosted endpoints.
    /// </param>
    public LLMServiceOpenAI(
        string baseUrl,
        string apiKey = "",
        string defaultModel = "gpt-4o-mini",
        string? embeddingModel = null)
    {
        _defaultModel  = defaultModel;
        _embeddingModel = embeddingModel ?? defaultModel;

        var normalised = baseUrl.TrimEnd('/');
        _httpClient = new HttpClient { BaseAddress = new Uri(normalised + "/") };
        _httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

        if (!string.IsNullOrWhiteSpace(apiKey))
        {
            _httpClient.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", apiKey);
        }

        _ownsHttpClient = true;
        IsInitialized = true;
    }

    /// <summary>
    /// Initialises a new <see cref="LLMServiceOpenAI"/> using a pre-configured <see cref="HttpClient"/>.
    /// The caller retains ownership of the client; it will not be disposed by this service.
    /// </summary>
    /// <param name="httpClient">A pre-configured <see cref="HttpClient"/> whose <c>BaseAddress</c> is set.</param>
    /// <param name="defaultModel">The default model identifier for completions.</param>
    /// <param name="embeddingModel">
    /// The model identifier for embedding requests. When <c>null</c>, falls back to <paramref name="defaultModel"/>.
    /// </param>
    public LLMServiceOpenAI(
        HttpClient httpClient,
        string defaultModel = "gpt-4o-mini",
        string? embeddingModel = null)
    {
        _httpClient     = httpClient;
        _defaultModel   = defaultModel;
        _embeddingModel = embeddingModel ?? defaultModel;
        _ownsHttpClient = false;
        IsInitialized   = true;
    }

    // ── Chat Completions ─────────────────────────────────────────────────────

    /// <inheritdoc/>
    public async Task<OperationResult<LLMResponse>> CompleteAsync(
        LLMRequest request,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<LLMResponse>.Failure("Service is not initialised.", HttpStatusCode.ServiceUnavailable);

        try
        {
            var body = BuildCompletionBody(request, stream: false);
            using var content = new StringContent(body, Encoding.UTF8, "application/json");
            using var response = await _httpClient.PostAsync("chat/completions", content, cancellationToken);

            var responseText = await response.Content.ReadAsStringAsync(cancellationToken);

            if (!response.IsSuccessStatusCode)
                return OperationResult<LLMResponse>.Failure(
                    $"API error {(int)response.StatusCode}: {responseText}",
                    response.StatusCode);

            var json = JObject.Parse(responseText);
            var result = ParseCompletionResponse(json);
            return OperationResult<LLMResponse>.Success(result);
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
        if (!IsInitialized)
        {
            yield return OperationResult<LLMStreamChunk>.Failure(
                "Service is not initialised.", HttpStatusCode.ServiceUnavailable);
            yield break;
        }

        HttpResponseMessage? response = null;
        Stream? stream = null;
        StreamReader? reader = null;
        OperationResult<LLMStreamChunk>? setupError = null;

        // ── Phase 1: Setup (no yield → try-catch is allowed) ──────────────
        try
        {
            var body = BuildCompletionBody(request, stream: true);
            using var httpRequest = new HttpRequestMessage(HttpMethod.Post, "chat/completions")
            {
                Content = new StringContent(body, Encoding.UTF8, "application/json")
            };

            response = await _httpClient.SendAsync(
                httpRequest,
                HttpCompletionOption.ResponseHeadersRead,
                cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                var errText = await response.Content.ReadAsStringAsync(cancellationToken);
                setupError = OperationResult<LLMStreamChunk>.Failure(
                    $"API error {(int)response.StatusCode}: {errText}",
                    response.StatusCode);
            }
            else
            {
                stream = await response.Content.ReadAsStreamAsync(cancellationToken);
                reader = new StreamReader(stream);
            }
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
            response?.Dispose();
            yield break;
        }

        // ── Phase 2: Stream reading (yield inside try-finally) ────────────
        try
        {
            while (!reader!.EndOfStream && !cancellationToken.IsCancellationRequested)
            {
                string? line;
                bool wasCancelled = false;
                Exception? readError = null;
                try
                {
                    line = await reader.ReadLineAsync(cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    line = null;
                    wasCancelled = true;
                }
                catch (Exception ex)
                {
                    line = null;
                    readError = ex;
                }

                if (wasCancelled)
                {
                    yield return OperationResult<LLMStreamChunk>.Failure(
                        "Request was cancelled.", HttpStatusCode.RequestTimeout);
                    yield break;
                }

                if (readError is not null)
                {
                    yield return OperationResult<LLMStreamChunk>.Failure(
                        readError.Message, HttpStatusCode.BadGateway);
                    yield break;
                }

                if (line is null) continue;
                if (!line.StartsWith("data: ", StringComparison.Ordinal)) continue;

                var data = line[6..];
                if (data == "[DONE]")
                {
                    yield return OperationResult<LLMStreamChunk>.Success(
                        new LLMStreamChunk { IsFinal = true, FinishReason = LLMFinishReason.Stop });
                    yield break;
                }

                OperationResult<LLMStreamChunk> chunk;
                try
                {
                    chunk = ParseStreamChunk(JObject.Parse(data));
                }
                catch (Exception parseEx)
                {
                    // Catch ALL exceptions from chunk parsing (not just JsonException) to prevent
                    // a null chunk being yielded to the consumer, which would cause a
                    // NullReferenceException. Examples of non-JSON errors: ArgumentOutOfRangeException
                    // from an API returning an empty choices array.
                    chunk = OperationResult<LLMStreamChunk>.Failure(
                        $"Failed to parse stream chunk: {parseEx.Message}",
                        HttpStatusCode.BadGateway);
                }

                yield return chunk;

                // On ANY failure the stream must end immediately per the interface contract:
                // "On error, a failure result is yielded and the stream ends."
                if (!chunk.IsSuccessful)
                    yield break;

                if (chunk.Data.IsFinal)
                    yield break;
            }
        }
        finally
        {
            reader?.Dispose();
            stream?.Dispose();
            response?.Dispose();
        }
    }

    // ── Embeddings ────────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public async Task<OperationResult<float[]>> CreateEmbeddingAsync(
        string text,
        CancellationToken cancellationToken = default)
    {
        var result = await CreateEmbeddingsAsync([text], cancellationToken);
        if (!result.IsSuccessful)
            return OperationResult<float[]>.Failure(result.ErrorMessage, result.StatusCode);

        if (result.Data.Count == 0)
            return OperationResult<float[]>.Failure(
                "API returned an empty embeddings array.", HttpStatusCode.BadGateway);

        return OperationResult<float[]>.Success(result.Data[0]);
    }

    /// <inheritdoc/>
    public async Task<OperationResult<IReadOnlyList<float[]>>> CreateEmbeddingsAsync(
        IReadOnlyList<string> texts,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<IReadOnlyList<float[]>>.Failure(
                "Service is not initialised.", HttpStatusCode.ServiceUnavailable);

        if (texts.Count == 0)
            return OperationResult<IReadOnlyList<float[]>>.Success(Array.Empty<float[]>());

        try
        {
            var body = JsonConvert.SerializeObject(new
            {
                model = _embeddingModel,
                input = texts
            });

            using var content = new StringContent(body, Encoding.UTF8, "application/json");
            using var response = await _httpClient.PostAsync("embeddings", content, cancellationToken);

            var responseText = await response.Content.ReadAsStringAsync(cancellationToken);

            if (!response.IsSuccessStatusCode)
                return OperationResult<IReadOnlyList<float[]>>.Failure(
                    $"API error {(int)response.StatusCode}: {responseText}",
                    response.StatusCode);

            var json = JObject.Parse(responseText);
            var dataArray = json["data"] as JArray
                ?? throw new InvalidOperationException("Unexpected embeddings response format.");

            var embeddings = dataArray
                .OrderBy(item => item["index"]?.Value<int>() ?? 0)
                .Select(item => (item["embedding"] as JArray
                    ?? throw new InvalidOperationException("Missing embedding array."))
                    .Values<float>()
                    .ToArray())
                .ToList();

            return OperationResult<IReadOnlyList<float[]>>.Success(embeddings);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return OperationResult<IReadOnlyList<float[]>>.Failure("Request was cancelled.", HttpStatusCode.RequestTimeout);
        }
        catch (Exception ex)
        {
            return OperationResult<IReadOnlyList<float[]>>.Failure(ex.Message, HttpStatusCode.InternalServerError);
        }
    }

    // ── IAsyncDisposable ──────────────────────────────────────────────────────

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            if (_ownsHttpClient)
                _httpClient.Dispose();
        }
        return ValueTask.CompletedTask;
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private string BuildCompletionBody(LLMRequest request, bool stream)
    {
        var obj = new JObject
        {
            ["model"] = request.Model ?? _defaultModel,
            ["stream"] = stream,
            ["messages"] = new JArray(request.Messages.Select(MessageToJson))
        };

        if (request.Temperature.HasValue)
            obj["temperature"] = request.Temperature.Value;

        if (request.MaxTokens.HasValue)
            obj["max_tokens"] = request.MaxTokens.Value;

        if (request.StopSequences?.Count > 0)
            obj["stop"] = new JArray(request.StopSequences);

        if (request.Tools?.Count > 0)
        {
            obj["tools"] = new JArray(request.Tools.Select(t => new JObject
            {
                ["type"] = "function",
                ["function"] = new JObject
                {
                    ["name"] = t.Name,
                    ["description"] = t.Description,
                    ["parameters"] = t.Parameters
                }
            }));
        }

        return obj.ToString(Formatting.None);
    }

    private static JObject MessageToJson(LLMMessage msg)
    {
        var role = msg.Role switch
        {
            LLMRole.System    => "system",
            LLMRole.User      => "user",
            LLMRole.Assistant => "assistant",
            LLMRole.Tool      => "tool",
            _                 => "user"
        };

        var obj = new JObject
        {
            ["role"]    = role,
            ["content"] = msg.Content
        };

        if (msg.ToolCallId is not null)
            obj["tool_call_id"] = msg.ToolCallId;

        return obj;
    }

    private static LLMResponse ParseCompletionResponse(JObject json)
    {
        var choice = (json["choices"] as JArray)?[0]
            ?? throw new InvalidOperationException("No choices in response.");

        var message  = choice["message"]  ?? new JObject();
        var content  = message["content"]?.Value<string>() ?? string.Empty;
        var finishRaw = choice["finish_reason"]?.Value<string>();

        var finishReason = finishRaw switch
        {
            "stop"          => LLMFinishReason.Stop,
            "length"        => LLMFinishReason.Length,
            "tool_calls"    => LLMFinishReason.ToolCall,
            "content_filter"=> LLMFinishReason.ContentFilter,
            _               => LLMFinishReason.Stop
        };

        LLMUsage? usage = null;
        if (json["usage"] is JObject usageObj)
        {
            usage = new LLMUsage
            {
                PromptTokens     = usageObj["prompt_tokens"]?.Value<int>()     ?? 0,
                CompletionTokens = usageObj["completion_tokens"]?.Value<int>() ?? 0,
                TotalTokens      = usageObj["total_tokens"]?.Value<int>()      ?? 0
            };
        }

        List<LLMToolCall>? toolCalls = null;
        if (message["tool_calls"] is JArray tcArray && tcArray.Count > 0)
        {
            toolCalls = tcArray.Select(tc => new LLMToolCall
            {
                Id        = tc["id"]?.Value<string>()                             ?? string.Empty,
                Name      = tc["function"]?["name"]?.Value<string>()              ?? string.Empty,
                Arguments = tc["function"]?["arguments"]?.Value<string>()         ?? string.Empty
            }).ToList();
        }

        return new LLMResponse
        {
            Content      = content,
            FinishReason = finishReason,
            Usage        = usage,
            ToolCalls    = toolCalls
        };
    }

    private static OperationResult<LLMStreamChunk> ParseStreamChunk(JObject json)
    {
        // Guard: some providers send a final usage-only chunk with an empty choices array.
        // Accessing [0] on an empty JArray throws ArgumentOutOfRangeException, so we
        // treat an empty (or missing) choices array the same as a null choice.
        var choicesArray = json["choices"] as JArray;
        var choice = (choicesArray is { Count: > 0 }) ? choicesArray[0] : null;

        string contentDelta = string.Empty;
        bool isFinal = false;
        LLMFinishReason? finishReason = null;
        LLMUsage? usage = null;

        if (choice is not null)
        {
            contentDelta = choice["delta"]?["content"]?.Value<string>() ?? string.Empty;

            var finishRaw = choice["finish_reason"]?.Value<string>();
            if (!string.IsNullOrEmpty(finishRaw))
            {
                isFinal = true;
                finishReason = finishRaw switch
                {
                    "stop"           => LLMFinishReason.Stop,
                    "length"         => LLMFinishReason.Length,
                    "tool_calls"     => LLMFinishReason.ToolCall,
                    "content_filter" => LLMFinishReason.ContentFilter,
                    _                => LLMFinishReason.Stop
                };
            }
        }

        if (json["usage"] is JObject usageObj)
        {
            usage = new LLMUsage
            {
                PromptTokens     = usageObj["prompt_tokens"]?.Value<int>()     ?? 0,
                CompletionTokens = usageObj["completion_tokens"]?.Value<int>() ?? 0,
                TotalTokens      = usageObj["total_tokens"]?.Value<int>()      ?? 0
            };
        }

        return OperationResult<LLMStreamChunk>.Success(new LLMStreamChunk
        {
            ContentDelta = contentDelta,
            IsFinal      = isFinal,
            FinishReason = finishReason,
            Usage        = usage
        });
    }
}
