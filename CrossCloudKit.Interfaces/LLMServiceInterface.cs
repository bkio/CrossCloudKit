// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Interfaces.Records;

namespace CrossCloudKit.Interfaces;

/// <summary>
/// Cross-cloud abstraction for Large Language Model services.
/// Supports both hosted endpoints (OpenAI, Azure OpenAI, Gemini, Groq, Bedrock)
/// and locally-running models (Ollama, LM Studio, LLamaSharp).
/// </summary>
public interface ILLMService : IAsyncDisposable
{
    /// <summary>
    /// Gets a value indicating whether the service has been successfully initialised.
    /// </summary>
    bool IsInitialized { get; }

    /// <summary>
    /// Sends a chat completion request and returns the full response once generation is complete.
    /// </summary>
    /// <param name="request">The completion request including messages, model, and generation parameters.</param>
    /// <param name="cancellationToken">Token to cancel the request.</param>
    /// <returns>
    /// An <see cref="OperationResult{T}"/> containing the completed <see cref="LLMResponse"/>,
    /// or a failure result with an error description if the request could not be fulfilled.
    /// </returns>
    Task<OperationResult<LLMResponse>> CompleteAsync(
        LLMRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends a chat completion request and streams token chunks as they are generated.
    /// </summary>
    /// <param name="request">The completion request including messages, model, and generation parameters.</param>
    /// <param name="cancellationToken">Token to cancel the stream.</param>
    /// <returns>
    /// An async enumerable of <see cref="OperationResult{T}"/> wrapping <see cref="LLMStreamChunk"/>.
    /// Each successful item contains an incremental content delta; the final item has
    /// <see cref="LLMStreamChunk.IsFinal"/> set to <c>true</c>.
    /// On error, a failure result is yielded and the stream ends.
    /// </returns>
    IAsyncEnumerable<OperationResult<LLMStreamChunk>> CompleteStreamingAsync(
        LLMRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a dense embedding vector for a single piece of text.
    /// </summary>
    /// <param name="text">The text to embed.</param>
    /// <param name="cancellationToken">Token to cancel the request.</param>
    /// <returns>
    /// An <see cref="OperationResult{T}"/> containing the embedding as a <c>float[]</c>,
    /// or a failure result on error.
    /// </returns>
    Task<OperationResult<float[]>> CreateEmbeddingAsync(
        string text,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates dense embedding vectors for a batch of texts in a single request.
    /// </summary>
    /// <param name="texts">The list of texts to embed.</param>
    /// <param name="cancellationToken">Token to cancel the request.</param>
    /// <returns>
    /// An <see cref="OperationResult{T}"/> containing one <c>float[]</c> per input text
    /// in the same order as <paramref name="texts"/>, or a failure result on error.
    /// </returns>
    Task<OperationResult<IReadOnlyList<float[]>>> CreateEmbeddingsAsync(
        IReadOnlyList<string> texts,
        CancellationToken cancellationToken = default);
}
