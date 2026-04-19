// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Basic.DebugPanel;

/// <summary>
/// Represents a single logged operation from a Basic service instance.
/// </summary>
public sealed record OperationEvent
{
    /// <summary>Instance that performed the operation.</summary>
    public required string InstanceId { get; init; }

    /// <summary>Type of service: "Memory", "Database", "File", "Vector", "PubSub".</summary>
    public required string ServiceType { get; init; }

    /// <summary>Operation name, e.g. "PutItem", "GetKeyValue".</summary>
    public required string OperationName { get; init; }

    /// <summary>Human-readable details, e.g. "table=Users, key=user-123".</summary>
    public string Details { get; init; } = "";

    /// <summary>Duration in milliseconds.</summary>
    public long DurationMs { get; init; }

    /// <summary>UTC timestamp when the operation completed.</summary>
    public DateTime TimestampUtc { get; init; }

    /// <summary>Whether the operation succeeded.</summary>
    public bool Success { get; init; }
}
