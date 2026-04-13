// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces.Enums;

namespace CrossCloudKit.Interfaces.Records;

/// <summary>
/// A single message in an LLM conversation.
/// </summary>
/// <example>
/// <code>
/// var system = new LLMMessage { Role = LLMRole.System, Content = "You are a helpful assistant." };
/// var user   = new LLMMessage { Role = LLMRole.User, Content = "Hello!" };
/// var toolResult = new LLMMessage
/// {
///     Role = LLMRole.Tool,
///     ToolCallId = "call_abc",
///     Content = "{\"result\":42}"
/// };
/// </code>
/// </example>
public sealed record LLMMessage
{
    /// <summary>The role of the message author.</summary>
    public LLMRole Role { get; init; }

    /// <summary>The text content of the message.</summary>
    public string Content { get; init; } = string.Empty;

    /// <summary>
    /// Optional tool call ID when <see cref="Role"/> is <see cref="LLMRole.Tool"/>.
    /// Associates the tool result with the originating tool call.
    /// </summary>
    public string? ToolCallId { get; init; }
}
