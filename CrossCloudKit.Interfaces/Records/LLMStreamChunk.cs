// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces.Enums;

namespace CrossCloudKit.Interfaces.Records;

/// <summary>
/// A single chunk delivered during a streaming LLM completion.
/// </summary>
/// <example>
/// <code>
/// await foreach (var chunk in llmService.CompleteStreamingAsync(request))
/// {
///     if (chunk.IsSuccessful)
///     {
///         Console.Write(chunk.Data.ContentDelta);
///         if (chunk.Data.IsFinal)
///             Console.WriteLine($"\nDone: {chunk.Data.FinishReason}");
///     }
/// }
/// </code>
/// </example>
public sealed record LLMStreamChunk
{
    /// <summary>
    /// The incremental text fragment produced in this chunk.
    /// Empty string for the final chunk.
    /// </summary>
    public string ContentDelta { get; init; } = string.Empty;

    /// <summary><c>true</c> if this is the last chunk in the stream.</summary>
    public bool IsFinal { get; init; }

    /// <summary>
    /// The reason generation stopped.
    /// Only set when <see cref="IsFinal"/> is <c>true</c>.
    /// </summary>
    public LLMFinishReason? FinishReason { get; init; }

    /// <summary>
    /// Token usage statistics for the full completion.
    /// Only set in the final chunk, and only if the backend supports it.
    /// </summary>
    public LLMUsage? Usage { get; init; }
}
