// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Newtonsoft.Json.Linq;

namespace CrossCloudKit.Interfaces.Records;

/// <summary>
/// Describes a tool (function) that the LLM may invoke.
/// </summary>
/// <example>
/// <code>
/// var weatherTool = new LLMToolDefinition
/// {
///     Name = "get_weather",
///     Description = "Gets the current weather for a city.",
///     Parameters = JObject.Parse(@"{
///         ""type"": ""object"",
///         ""properties"": {
///             ""city"": { ""type"": ""string"" }
///         },
///         ""required"": [""city""]
///     }")
/// };
/// var request = new LLMRequest
/// {
///     Messages = new[] { new LLMMessage { Role = LLMRole.User, Content = "Weather in Berlin?" } },
///     Tools = new[] { weatherTool }
/// };
/// </code>
/// </example>
public sealed record LLMToolDefinition
{
    /// <summary>The unique name of the tool.</summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>Human-readable description that helps the model decide when to call this tool.</summary>
    public string Description { get; init; } = string.Empty;

    /// <summary>
    /// JSON Schema object describing the tool parameters.
    /// Follows the OpenAI function-calling schema format.
    /// </summary>
    public JObject Parameters { get; init; } = new();
}
