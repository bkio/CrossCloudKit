// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Net.Http.Headers;
using System.Text;
using Newtonsoft.Json;

namespace CrossCloudKit.Basic.DebugPanel;

/// <summary>
/// Per-service-instance operation tracker that logs operations to the debug panel server.
/// All HTTP calls are fire-and-forget and never throw.
/// </summary>
public sealed class DebugTracker
{
    private readonly HttpClient _httpClient;
    private readonly string _baseUrl;
    private volatile bool _disposed;

    /// <summary>Unique identifier for this service instance.</summary>
    public string InstanceId { get; }

    /// <summary>Service type ("Memory", "Database", etc.).</summary>
    public string ServiceType { get; }

    internal DebugTracker(string instanceId, string serviceType, HttpClient httpClient, string baseUrl)
    {
        InstanceId = instanceId;
        ServiceType = serviceType;
        _httpClient = httpClient;
        _baseUrl = baseUrl;
    }

    /// <summary>
    /// Begins tracking an operation. Dispose the returned scope when the operation completes.
    /// Returns null if the tracker has been disposed.
    /// </summary>
    public OperationScope? BeginOperation(string operationName, string details = "")
    {
        if (_disposed) return null;
        return new OperationScope(this, operationName, details);
    }

    /// <summary>
    /// Marks this tracker as disposed. Further operations are silently ignored.
    /// </summary>
    public void MarkDisposed() => _disposed = true;

    internal void PostOperation(OperationEvent operation)
    {
        if (_disposed) return;

        // Fire-and-forget — never block the caller
        _ = Task.Run(async () =>
        {
            try
            {
                var json = JsonConvert.SerializeObject(operation);
                using var content = new StringContent(json, Encoding.UTF8, "application/json");
                await _httpClient.PostAsync($"{_baseUrl}/api/operation", content);
            }
            catch
            {
                // Silently ignore — this is a debug tool, not critical path
            }
        });
    }

    /// <summary>
    /// Disposable scope that measures operation duration and posts the result.
    /// </summary>
    public sealed class OperationScope : IDisposable
    {
        private readonly DebugTracker _tracker;
        private readonly string _operationName;
        private readonly string _details;
        private readonly Stopwatch _sw;
        private bool _disposed;
        private bool _success = true;

        internal OperationScope(DebugTracker tracker, string operationName, string details)
        {
            _tracker = tracker;
            _operationName = operationName;
            _details = details;
            _sw = Stopwatch.StartNew();
        }

        /// <summary>Mark the operation as failed.</summary>
        public void MarkFailed() => _success = false;

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            _sw.Stop();

            _tracker.PostOperation(new OperationEvent
            {
                InstanceId = _tracker.InstanceId,
                ServiceType = _tracker.ServiceType,
                OperationName = _operationName,
                Details = _details,
                DurationMs = _sw.ElapsedMilliseconds,
                TimestampUtc = DateTime.UtcNow,
                Success = _success
            });
        }
    }
}
