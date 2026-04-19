// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Collections.Concurrent;

namespace CrossCloudKit.Basic.DebugPanel;

/// <summary>
/// Thread-safe in-memory state backing the debug panel server.
/// Holds registered services and a capped ring buffer of recent operations.
/// </summary>
public sealed class DebugPanelState
{
    /// <summary>Maximum number of operation events kept in the ring buffer.</summary>
    public const int MaxOperationEvents = 1000;

    /// <summary>Active service registrations keyed by InstanceId.</summary>
    public ConcurrentDictionary<string, ServiceRegistration> Services { get; } = new();

    /// <summary>Ring buffer of recent operation events (capped at <see cref="MaxOperationEvents"/>).</summary>
    public ConcurrentQueue<OperationEvent> Operations { get; } = new();

    /// <summary>Per-instance total operation count.</summary>
    public ConcurrentDictionary<string, long> OperationCountByInstance { get; } = new();

    /// <summary>Per-instance data providers for browsing service content.</summary>
    private ConcurrentDictionary<string, IDebugDataProvider> DataProviders { get; } = new();

    /// <summary>UTC timestamp when the server started.</summary>
    public DateTime ServerStartedAtUtc { get; set; }

    /// <summary>
    /// Registers a service instance. Idempotent — re-registering the same InstanceId overwrites.
    /// </summary>
    public void Register(ServiceRegistration registration)
    {
        Services[registration.InstanceId] = registration;
        OperationCountByInstance.TryAdd(registration.InstanceId, 0);
    }

    /// <summary>
    /// Deregisters a service instance by InstanceId. Returns true if found and removed.
    /// </summary>
    public bool Deregister(string instanceId)
    {
        var removed = Services.TryRemove(instanceId, out _);
        OperationCountByInstance.TryRemove(instanceId, out _);
        DataProviders.TryRemove(instanceId, out _);
        return removed;
    }

    /// <summary>
    /// Stores a data provider for the given service instance.
    /// </summary>
    public void SetDataProvider(string instanceId, IDebugDataProvider provider)
    {
        DataProviders[instanceId] = provider;
    }

    /// <summary>
    /// Retrieves the data provider for the given service instance, or null if none registered.
    /// </summary>
    public IDebugDataProvider? GetDataProvider(string instanceId)
    {
        DataProviders.TryGetValue(instanceId, out var provider);
        return provider;
    }

    /// <summary>
    /// Returns true if the given service instance has a data provider.
    /// </summary>
    public bool HasDataProvider(string instanceId)
    {
        return DataProviders.ContainsKey(instanceId);
    }

    /// <summary>
    /// Adds an operation event to the ring buffer, evicting the oldest if at capacity.
    /// </summary>
    public void AddOperation(OperationEvent operation)
    {
        Operations.Enqueue(operation);
        OperationCountByInstance.AddOrUpdate(operation.InstanceId, 1, (_, count) => count + 1);

        // Evict oldest entries beyond the cap
        while (Operations.Count > MaxOperationEvents)
            Operations.TryDequeue(out _);
    }

    /// <summary>
    /// Removes all service registrations whose <see cref="ServiceRegistration.ProcessId"/> refers to a
    /// process that is no longer running. Returns the InstanceIds that were purged.
    /// </summary>
    public IReadOnlyList<string> PurgeDeadProcesses()
    {
        var deadInstanceIds = new List<string>();

        // Group by ProcessId to avoid redundant process checks
        var pidGroups = Services.Values
            .GroupBy(s => s.ProcessId)
            .ToList();

        foreach (var group in pidGroups)
        {
            if (!IsProcessAlive(group.Key))
            {
                foreach (var svc in group)
                {
                    Deregister(svc.InstanceId);
                    deadInstanceIds.Add(svc.InstanceId);
                }
            }
        }

        return deadInstanceIds;
    }

    private static bool IsProcessAlive(int pid)
    {
        try
        {
            using var process = System.Diagnostics.Process.GetProcessById(pid);
            return !process.HasExited;
        }
        catch (ArgumentException)
        {
            // Process does not exist
            return false;
        }
        catch (InvalidOperationException)
        {
            // Process has exited between GetProcessById and HasExited
            return false;
        }
    }

    /// <summary>
    /// Returns the total number of registered service instances.
    /// </summary>
    public int ServiceCount => Services.Count;
}
