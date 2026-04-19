// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace CrossCloudKit.Basic.DebugPanel;

/// <summary>
/// Embedded Kestrel-based HTTP server that hosts the debug panel dashboard
/// and accepts registration/operation events from Basic service instances.
/// </summary>
public sealed class DebugPanelServer : IAsyncDisposable
{
    private WebApplication? _app;
    private CancellationTokenSource? _cts;
    private Task? _runTask;
    private bool _disposed;

    private readonly ConcurrentDictionary<string, SseClient> _sseClients = new();

    /// <summary>In-memory state backing all endpoints.</summary>
    public DebugPanelState State { get; } = new();

    /// <summary>The port the server is listening on. 0 if not started.</summary>
    public int Port { get; private set; }

    /// <summary>Whether the server is currently running.</summary>
    public bool IsRunning => _app is not null && !_disposed;

    private Timer? _shutdownTimer;
    private readonly object _shutdownLock = new();
    private Timer? _livenessTimer;

    /// <summary>Grace period before auto-shutdown after last deregistration.</summary>
    public TimeSpan ShutdownGracePeriod { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Interval between process liveness checks. Dead-process entries are purged automatically.</summary>
    public TimeSpan LivenessCheckInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>Fired when the server has been stopped (auto-shutdown or explicit).</summary>
    public event Action? OnStopped;

    /// <summary>
    /// Starts the server on the given port.
    /// </summary>
    public async Task StartAsync(int port)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(DebugPanelServer));
        if (_app is not null) throw new InvalidOperationException("Server is already running.");

        _cts = new CancellationTokenSource();
        State.ServerStartedAtUtc = DateTime.UtcNow;
        Port = port;

        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseKestrelCore().ConfigureKestrel(opts =>
        {
            opts.Listen(IPAddress.Loopback, port);
        });
        builder.Logging.ClearProviders();
        _app = builder.Build();

        MapEndpoints(_app);

        _runTask = _app.StartAsync(_cts.Token);

        // Wait a bit for Kestrel to start accepting connections
        await Task.Delay(200, CancellationToken.None);

        // Start periodic liveness checks to purge dead-process entries
        _livenessTimer = new Timer(_ => _ = PurgeDeadProcessesAsync(), null, LivenessCheckInterval, LivenessCheckInterval);
    }

    /// <summary>
    /// Stops the server gracefully.
    /// </summary>
    public async Task StopAsync()
    {
        if (_app is null) return;

        CancelScheduledShutdown();

        try
        {
            await _app.StopAsync();
            if (_runTask is not null)
            {
                try { await _runTask; }
                catch (OperationCanceledException) { }
            }

            await _app.DisposeAsync();
        }
        catch (Exception)
        {
            // Ignore errors during shutdown
        }
        finally
        {
            _livenessTimer?.Dispose();
            _livenessTimer = null;
            _app = null;
            _runTask = null;
            _cts?.Dispose();
            _cts = null;
            Port = 0;
            OnStopped?.Invoke();
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        await StopAsync();
        CancelScheduledShutdown();
        GC.SuppressFinalize(this);
    }

    // ── Auto-shutdown logic ───────────────────────────────────────────────

    internal void ScheduleShutdownIfEmpty()
    {
        lock (_shutdownLock)
        {
            if (State.ServiceCount > 0) return;
            _shutdownTimer?.Dispose();
            _shutdownTimer = new Timer(_ =>
            {
                lock (_shutdownLock)
                {
                    if (State.ServiceCount > 0) return;
                }
                _ = Task.Run(StopAsync);
            }, null, ShutdownGracePeriod, Timeout.InfiniteTimeSpan);
        }
    }

    internal void CancelScheduledShutdown()
    {
        lock (_shutdownLock)
        {
            _shutdownTimer?.Dispose();
            _shutdownTimer = null;
        }
    }

    // ── Process liveness checking ─────────────────────────────────────────

    private async Task PurgeDeadProcessesAsync()
    {
        try
        {
            var purgedIds = State.PurgeDeadProcesses();
            if (purgedIds.Count == 0) return;

            foreach (var instanceId in purgedIds)
            {
                await BroadcastSseAsync("service-deregistered",
                    JsonConvert.SerializeObject(new { InstanceId = instanceId }));
            }

            ScheduleShutdownIfEmpty();
        }
        catch
        {
            // Non-critical — swallow errors from liveness checks
        }
    }

    // ── Endpoint mapping ──────────────────────────────────────────────────

    private void MapEndpoints(WebApplication app)
    {
        app.MapGet("/", () =>
        {
            var html = DebugPanelHtmlRenderer.Render(State);
            return Results.Content(html, "text/html");
        });

        app.MapGet("/api/services", () =>
        {
            var json = JsonConvert.SerializeObject(State.Services.Values);
            return Results.Content(json, "application/json");
        });

        app.MapPost("/api/register", async (HttpRequest request) =>
        {
            var body = await new StreamReader(request.Body).ReadToEndAsync();
            var reg = JsonConvert.DeserializeObject<ServiceRegistration>(body);
            if (reg is null || string.IsNullOrEmpty(reg.InstanceId))
                return Results.BadRequest("Invalid registration payload.");

            // Purge any stale entries from dead processes before registering
            await PurgeDeadProcessesAsync();

            State.Register(reg);
            CancelScheduledShutdown();
            await BroadcastSseAsync("service-registered", JsonConvert.SerializeObject(reg));
            return Results.Ok();
        });

        app.MapPost("/api/deregister", async (HttpRequest request) =>
        {
            var body = await new StreamReader(request.Body).ReadToEndAsync();
            var payload = JsonConvert.DeserializeAnonymousType(body, new { InstanceId = "" });
            if (payload is null || string.IsNullOrEmpty(payload.InstanceId))
                return Results.BadRequest("Missing InstanceId.");

            var removed = State.Deregister(payload.InstanceId);
            if (removed)
            {
                await BroadcastSseAsync("service-deregistered", JsonConvert.SerializeObject(new { payload.InstanceId }));
                ScheduleShutdownIfEmpty();
            }
            return Results.Ok();
        });

        app.MapPost("/api/operation", async (HttpRequest request) =>
        {
            var body = await new StreamReader(request.Body).ReadToEndAsync();
            var op = JsonConvert.DeserializeObject<OperationEvent>(body);
            if (op is null || string.IsNullOrEmpty(op.InstanceId))
                return Results.BadRequest("Invalid operation payload.");

            State.AddOperation(op);
            await BroadcastSseAsync("operation", JsonConvert.SerializeObject(op));
            return Results.Ok();
        });

        app.MapGet("/api/events", async (HttpContext context) =>
        {
            context.Response.ContentType = "text/event-stream";
            context.Response.Headers.CacheControl = "no-cache";
            context.Response.Headers.Connection = "keep-alive";

            var clientId = Guid.NewGuid().ToString("N");
            var client = new SseClient(context.Response);
            _sseClients[clientId] = client;

            try
            {
                // Send initial heartbeat
                await client.WriteAsync("heartbeat", "connected");

                // Keep the connection open until the client disconnects or server stops
                var tcs = new TaskCompletionSource();
                context.RequestAborted.Register(() => tcs.TrySetResult());
                _cts?.Token.Register(() => tcs.TrySetResult());
                await tcs.Task;
            }
            finally
            {
                _sseClients.TryRemove(clientId, out _);
            }
        });

        // ── Browse endpoints ──────────────────────────────────────────────

        app.MapGet("/api/browse/{instanceId}/containers", async (string instanceId) =>
        {
            var provider = State.GetDataProvider(instanceId);
            if (provider is null)
                return Results.NotFound("No data provider for this instance.");

            try
            {
                var containers = await provider.ListContainersAsync();
                return Results.Content(JsonConvert.SerializeObject(containers), "application/json");
            }
            catch (Exception ex)
            {
                return Results.Problem($"Error listing containers: {ex.Message}");
            }
        });

        app.MapGet("/api/browse/{instanceId}/items", async (string instanceId, HttpRequest request) =>
        {
            var provider = State.GetDataProvider(instanceId);
            if (provider is null)
                return Results.NotFound("No data provider for this instance.");

            var container = request.Query["container"].ToString();
            if (string.IsNullOrEmpty(container))
                return Results.BadRequest("Missing 'container' query parameter.");

            var maxItems = 200;
            if (int.TryParse(request.Query["max"], out var parsed) && parsed > 0)
                maxItems = Math.Min(parsed, 1000);

            try
            {
                var items = await provider.ListItemsAsync(container, maxItems);
                return Results.Content(JsonConvert.SerializeObject(items), "application/json");
            }
            catch (Exception ex)
            {
                return Results.Problem($"Error listing items: {ex.Message}");
            }
        });

        app.MapGet("/api/browse/{instanceId}/detail", async (string instanceId, HttpRequest request) =>
        {
            var provider = State.GetDataProvider(instanceId);
            if (provider is null)
                return Results.NotFound("No data provider for this instance.");

            var container = request.Query["container"].ToString();
            var itemId = request.Query["id"].ToString();
            if (string.IsNullOrEmpty(container) || string.IsNullOrEmpty(itemId))
                return Results.BadRequest("Missing 'container' or 'id' query parameter.");

            try
            {
                var detail = await provider.GetItemDetailAsync(container, itemId);
                if (detail is null)
                    return Results.NotFound("Item not found.");

                return Results.Content(JsonConvert.SerializeObject(detail), "application/json");
            }
            catch (Exception ex)
            {
                return Results.Problem($"Error getting item detail: {ex.Message}");
            }
        });

        app.MapGet("/api/browse/{instanceId}/browsable", (string instanceId) =>
        {
            var has = State.HasDataProvider(instanceId);
            return Results.Content(JsonConvert.SerializeObject(new { Browsable = has }), "application/json");
        });
    }

    // ── SSE broadcasting ──────────────────────────────────────────────────

    private async Task BroadcastSseAsync(string eventType, string data)
    {
        var deadClients = new List<string>();

        foreach (var (clientId, client) in _sseClients)
        {
            try
            {
                await client.WriteAsync(eventType, data);
            }
            catch
            {
                deadClients.Add(clientId);
            }
        }

        foreach (var id in deadClients)
            _sseClients.TryRemove(id, out _);
    }

    // ── SSE client wrapper ────────────────────────────────────────────────

    private sealed class SseClient(HttpResponse response)
    {
        private readonly SemaphoreSlim _writeLock = new(1, 1);

        public async Task WriteAsync(string eventType, string data)
        {
            await _writeLock.WaitAsync();
            try
            {
                await response.WriteAsync($"event: {eventType}\ndata: {data}\n\n");
                await response.Body.FlushAsync();
            }
            finally
            {
                _writeLock.Release();
            }
        }
    }
}
