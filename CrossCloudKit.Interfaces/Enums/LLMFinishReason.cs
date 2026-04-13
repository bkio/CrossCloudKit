// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Interfaces.Enums;

/// <summary>
/// The reason why an LLM generation stopped.
/// </summary>
public enum LLMFinishReason
{
    /// <summary>The model reached a natural stopping point.</summary>
    Stop,

    /// <summary>The maximum token limit was reached.</summary>
    Length,

    /// <summary>The model decided to invoke a tool or function.</summary>
    ToolCall,

    /// <summary>Content was filtered by the safety system.</summary>
    ContentFilter
}
