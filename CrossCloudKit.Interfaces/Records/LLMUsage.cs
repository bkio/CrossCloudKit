// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Interfaces.Records;

/// <summary>
/// Token usage statistics returned by an LLM completion call.
/// </summary>
public sealed record LLMUsage
{
    /// <summary>Number of tokens consumed by the input (prompt) messages.</summary>
    public int PromptTokens { get; init; }

    /// <summary>Number of tokens generated in the completion response.</summary>
    public int CompletionTokens { get; init; }

    /// <summary>Total tokens used (<see cref="PromptTokens"/> + <see cref="CompletionTokens"/>).</summary>
    public int TotalTokens { get; init; }
}
