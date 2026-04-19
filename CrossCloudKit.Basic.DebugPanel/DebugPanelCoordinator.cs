// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;
using System.Net.Sockets;
using System.Text;
using CrossCloudKit.Utilities.Common;
using Newtonsoft.Json;

namespace CrossCloudKit.Basic.DebugPanel;

/// <summary>
/// Process-wide singleton that coordinates debug panel server startup and
/// provides <see cref="DebugTracker"/> instances to Basic service constructors.
/// <para>
/// The first Basic service to register within a process (or across processes on the same machine)
/// starts the embedded Kestrel server. Subsequent services reuse the running server via HTTP POSTs.
/// </para>
/// <para>
/// Disabled via environment variable <c>CROSSCLOUDKIT_DEBUG_PANEL_DISABLED=true</c>.
/// </para>
/// </summary>
public static class DebugPanelCoordinator
{
    /// <summary>Default port for the debug panel.</summary>
    public const int DefaultPort = 57765;

    private static readonly SemaphoreSlim InitLock = new(1, 1);
    private static DebugPanelServer? _server;
    private static HttpClient? _httpClient;
    private static int _localRegistrationCount;
    private static int _port = DefaultPort;

    /// <summary>Whether the coordinator is disabled via environment variable.</summary>
    public static bool IsDisabled =>
        string.Equals(Environment.GetEnvironmentVariable("CROSSCLOUDKIT_DEBUG_PANEL_DISABLED"), "true", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Resolves the effective port. If <c>CROSSCLOUDKIT_DEBUG_PANEL_PORT</c> is set to a valid integer,
    /// that value is used; otherwise <paramref name="requestedPort"/> is returned.
    /// </summary>
    private static int ResolvePort(int requestedPort)
    {
        var envPort = Environment.GetEnvironmentVariable("CROSSCLOUDKIT_DEBUG_PANEL_PORT");
        return int.TryParse(envPort, out var parsed) ? parsed : requestedPort;
    }

    /// <summary>Whether this process owns the running server.</summary>
    public static bool IsServerOwner => _server is not null;

    /// <summary>
    /// Registers a Basic service instance with the debug panel.
    /// Starts the server if no server is running on the configured port.
    /// Returns a <see cref="DebugTracker"/> for the service to log operations.
    /// </summary>
    /// <param name="serviceType">Service type: "Memory", "Database", "File", "Vector", "PubSub".</param>
    /// <param name="path">Resolved base directory path of the service instance.</param>
    /// <param name="dataProvider">Optional data provider for browsing service data in the panel.</param>
    /// <param name="port">Port to use (defaults to <see cref="DefaultPort"/>).</param>
    /// <returns>A tracker, or null if the panel is disabled.</returns>
    public static async Task<DebugTracker?> RegisterAsync(string serviceType, string path, IDebugDataProvider? dataProvider = null, int port = DefaultPort)
    {
        if (IsDisabled) return null;

        await InitLock.WaitAsync();
        try
        {
            _port = ResolvePort(port);
            EnsureHttpClient();

            if (_localRegistrationCount == 0)
                await EnsureServerRunningAsync(_port);

            var instanceId = Guid.NewGuid().ToString("N");
            var registration = new ServiceRegistration
            {
                InstanceId = instanceId,
                ServiceType = serviceType,
                Path = path,
                ProcessId = Environment.ProcessId,
                MachineName = Environment.MachineName,
                StartedAtUtc = DateTime.UtcNow
            };

            await PostRegistrationAsync(registration);

            // If we own the server, store the data provider directly for in-process browsing
            if (_server is not null && dataProvider is not null)
                _server.State.SetDataProvider(instanceId, dataProvider);

            Interlocked.Increment(ref _localRegistrationCount);

            return new DebugTracker(instanceId, serviceType, _httpClient!, $"http://localhost:{_port}");
        }
        catch
        {
            // Debug panel is non-critical — swallow all errors
            return null;
        }
        finally
        {
            InitLock.Release();
        }
    }

    /// <summary>
    /// Deregisters a service instance. If this was the last local registration
    /// and this process owns the server, triggers auto-shutdown.
    /// </summary>
    public static async Task DeregisterAsync(string instanceId)
    {
        if (IsDisabled || string.IsNullOrEmpty(instanceId)) return;

        await InitLock.WaitAsync();
        try
        {
            try
            {
                EnsureHttpClient();
                var json = JsonConvert.SerializeObject(new { InstanceId = instanceId });
                using var content = new StringContent(json, Encoding.UTF8, "application/json");
                await _httpClient!.PostAsync($"http://localhost:{_port}/api/deregister", content);
            }
            catch
            {
                // Ignore — server may already be down
            }

            var remaining = Interlocked.Decrement(ref _localRegistrationCount);
            if (remaining <= 0 && _server is not null)
            {
                _localRegistrationCount = 0;
                // Let the server auto-shutdown via its grace period logic
                _server.ScheduleShutdownIfEmpty();
                _server.OnStopped += OnServerStopped;
            }
        }
        finally
        {
            InitLock.Release();
        }
    }

    // ── Internal: for testing ─────────────────────────────────────────────

    /// <summary>
    /// Resets all static state. Only for use in tests.
    /// </summary>
    internal static async Task ResetAsync()
    {
        await InitLock.WaitAsync();
        try
        {
            if (_server is not null)
            {
                await _server.DisposeAsync();
                _server = null;
            }
            _httpClient?.Dispose();
            _httpClient = null;
            _localRegistrationCount = 0;
            _port = DefaultPort;
        }
        finally
        {
            InitLock.Release();
        }
    }

    /// <summary>
    /// Returns the server instance if this process owns it. For testing only.
    /// </summary>
    internal static DebugPanelServer? Server => _server;

    // ── Private helpers ───────────────────────────────────────────────────

    private static void EnsureHttpClient()
    {
        _httpClient ??= new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
    }

    private static async Task EnsureServerRunningAsync(int port)
    {
        // Use an OS-level named mutex to coordinate across processes
        try
        {
            using var mutex = new AutoMutex($"CrossCloudKit.DebugPanel.{port}", TimeSpan.FromSeconds(3));

            if (!IsPortInUse(port))
            {
                _server = new DebugPanelServer();
                await _server.StartAsync(port);
            }
        }
        catch (TimeoutException)
        {
            // Another process is starting the server — that's fine, we'll be a client
        }
    }

    private static bool IsPortInUse(int port)
    {
        try
        {
            using var listener = new TcpListener(IPAddress.Loopback, port);
            listener.Start();
            listener.Stop();
            return false;
        }
        catch (SocketException)
        {
            return true;
        }
    }

    private static async Task PostRegistrationAsync(ServiceRegistration reg)
    {
        var json = JsonConvert.SerializeObject(reg);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        var response = await _httpClient!.PostAsync($"http://localhost:{_port}/api/register", content);
        response.EnsureSuccessStatusCode();
    }

    private static void OnServerStopped()
    {
        _server = null;
    }
}
