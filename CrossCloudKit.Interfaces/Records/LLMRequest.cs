// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Interfaces.Records;

/// <summary>
/// Parameters for an LLM chat completion request.
/// </summary>
public sealed record LLMRequest
{
    /// <summary>
    /// The model identifier to use for completion.
    /// When <c>null</c> the service uses its configured default model.
    /// </summary>
    public string? Model { get; init; }

    /// <summary>
    /// The conversation history, including system, user, assistant,
    /// and tool messages, in chronological order.
    /// </summary>
    public IReadOnlyList<LLMMessage> Messages { get; init; } = [];

    /// <summary>
    /// Sampling temperature in the range [0, 2].
    /// Higher values make output more random; lower values make it more deterministic.
    /// When <c>null</c> the service uses its default.
    /// </summary>
    public double? Temperature { get; init; }

    /// <summary>
    /// Maximum number of tokens to generate.
    /// When <c>null</c> the service uses its default.
    /// </summary>
    public int? MaxTokens { get; init; }

    /// <summary>
    /// Optional sequences at which to stop token generation.
    /// </summary>
    public IReadOnlyList<string>? StopSequences { get; init; }

    /// <summary>
    /// Tools (functions) that the model may choose to invoke.
    /// When empty or <c>null</c>, tool calling is disabled.
    /// </summary>
    public IReadOnlyList<LLMToolDefinition>? Tools { get; init; }
}
