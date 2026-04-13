// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Interfaces.Records;

/// <summary>
/// Represents a tool invocation requested by the LLM.
/// </summary>
public sealed record LLMToolCall
{
    /// <summary>
    /// Unique identifier for this tool call, used to associate the
    /// tool result with the original request.
    /// </summary>
    public string Id { get; init; } = string.Empty;

    /// <summary>The name of the tool to invoke.</summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>
    /// JSON string containing the arguments for the tool call,
    /// matching the schema defined in <see cref="LLMToolDefinition.Parameters"/>.
    /// </summary>
    public string Arguments { get; init; } = string.Empty;
}
