// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Basic.DebugPanel;

/// <summary>
/// Represents a registered Basic service instance in the debug panel.
/// </summary>
public sealed record ServiceRegistration
{
    /// <summary>Unique identifier for this service instance (GUID).</summary>
    public required string InstanceId { get; init; }

    /// <summary>Type of service: "Memory", "Database", "File", "Vector", "PubSub".</summary>
    public required string ServiceType { get; init; }

    /// <summary>Resolved base directory path — used as the unique identifier for multiple instances of the same type.</summary>
    public required string Path { get; init; }

    /// <summary>Process ID of the process that owns this service instance.</summary>
    public int ProcessId { get; init; }

    /// <summary>Machine name of the host running the service.</summary>
    public string MachineName { get; init; } = "";

    /// <summary>UTC timestamp when the service was initialized.</summary>
    public DateTime StartedAtUtc { get; init; }
}
