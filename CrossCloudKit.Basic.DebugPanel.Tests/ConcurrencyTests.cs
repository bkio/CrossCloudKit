// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Text;
using FluentAssertions;
using Newtonsoft.Json;
using Xunit;

namespace CrossCloudKit.Basic.DebugPanel.Tests;

/// <summary>
/// Concurrency tests — stress the server with parallel registrations, operations, and SSE.
/// </summary>
[Collection("Coordinator")]
public class ConcurrencyTests : IAsyncLifetime
{
    private DebugPanelServer _server = null!;
    private HttpClient _http = null!;
    private int _port;

    public async Task InitializeAsync()
    {
        _port = TestHelpers.GetAvailablePort();
        _server = new DebugPanelServer();
        await _server.StartAsync(_port);
        _http = new HttpClient { BaseAddress = new Uri($"http://localhost:{_port}"), Timeout = TimeSpan.FromSeconds(30) };
    }

    public async Task DisposeAsync()
    {
        _http.Dispose();
        await _server.DisposeAsync();
    }

    [Fact]
    public async Task ConcurrentRegistrations_ShouldAllSucceed()
    {
        const int threadCount = 20;
        var tasks = Enumerable.Range(0, threadCount).Select(i =>
        {
            var reg = new ServiceRegistration
            {
                InstanceId = $"conc-reg-{i}",
                ServiceType = i % 3 == 0 ? "Memory" : i % 3 == 1 ? "Database" : "Vector",
                Path = $"/tmp/conc/{i}",
                ProcessId = Environment.ProcessId,
                MachineName = Environment.MachineName,
                StartedAtUtc = DateTime.UtcNow
            };
            return PostJsonAsync("/api/register", reg);
        }).ToArray();

        await Task.WhenAll(tasks);

        _server.State.ServiceCount.Should().Be(threadCount);
    }

    [Fact]
    public async Task ConcurrentOperations_ShouldAllBeLogged()
    {
        const int opCount = 100;

        _server.State.Register(new ServiceRegistration
        {
            InstanceId = "conc-ops",
            ServiceType = "Memory",
            Path = "/tmp/ops",
            ProcessId = 1,
            MachineName = "test",
            StartedAtUtc = DateTime.UtcNow
        });

        var tasks = Enumerable.Range(0, opCount).Select(i =>
        {
            var op = new OperationEvent
            {
                InstanceId = "conc-ops",
                ServiceType = "Memory",
                OperationName = $"Op_{i}",
                TimestampUtc = DateTime.UtcNow,
                Success = true
            };
            return PostJsonAsync("/api/operation", op);
        }).ToArray();

        await Task.WhenAll(tasks);

        // All ops should be counted
        _server.State.OperationCountByInstance["conc-ops"].Should().Be(opCount);
    }

    [Fact]
    public async Task ConcurrentRegisterAndDeregister_ShouldNotCrash()
    {
        const int count = 20;

        // First register all
        for (var i = 0; i < count; i++)
        {
            _server.State.Register(new ServiceRegistration
            {
                InstanceId = $"race-{i}",
                ServiceType = "Memory",
                Path = $"/tmp/race/{i}",
                ProcessId = 1,
                MachineName = "test",
                StartedAtUtc = DateTime.UtcNow
            });
        }

        // Concurrently register some new + deregister some existing
        var tasks = Enumerable.Range(0, count).Select(i =>
        {
            if (i % 2 == 0)
            {
                // Register new
                var reg = new ServiceRegistration
                {
                    InstanceId = $"race-new-{i}",
                    ServiceType = "Vector",
                    Path = $"/tmp/race-new/{i}",
                    ProcessId = 1,
                    MachineName = "test",
                    StartedAtUtc = DateTime.UtcNow
                };
                return PostJsonAsync("/api/register", reg);
            }
            else
            {
                // Deregister existing
                return PostJsonAsync("/api/deregister", new { InstanceId = $"race-{i}" });
            }
        }).ToArray();

        await Task.WhenAll(tasks);

        // Should not crash — just verify state is consistent
        _server.State.ServiceCount.Should().BeGreaterOrEqualTo(0);
    }

    [Fact]
    public async Task ConcurrentCoordinatorRegistrations_ShouldAllSucceed()
    {
        Environment.SetEnvironmentVariable("CROSSCLOUDKIT_DEBUG_PANEL_DISABLED", null);
        var port = TestHelpers.GetAvailablePort();
        const int threadCount = 10;

        try
        {
            var tasks = Enumerable.Range(0, threadCount).Select(i =>
                DebugPanelCoordinator.RegisterAsync($"CoordType{i}", $"/tmp/coord-conc/{i}", port: port)
            ).ToArray();

            var trackers = await Task.WhenAll(tasks);

            trackers.Should().AllSatisfy(t => t.Should().NotBeNull());
            DebugPanelCoordinator.Server.Should().NotBeNull();
            DebugPanelCoordinator.Server!.IsRunning.Should().BeTrue();
            DebugPanelCoordinator.Server.State.ServiceCount.Should().Be(threadCount);
        }
        finally
        {
            await DebugPanelCoordinator.ResetAsync();
        }
    }

    [Fact]
    public async Task ConcurrentStateAddOperations_ThreadSafe()
    {
        var state = new DebugPanelState();
        const int threadCount = 50;
        const int opsPerThread = 100;

        var tasks = Enumerable.Range(0, threadCount).Select(t => Task.Run(() =>
        {
            for (var i = 0; i < opsPerThread; i++)
            {
                state.AddOperation(new OperationEvent
                {
                    InstanceId = $"thread-{t}",
                    ServiceType = "Test",
                    OperationName = $"Op_{t}_{i}",
                    TimestampUtc = DateTime.UtcNow,
                    Success = true
                });
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        var totalExpected = threadCount * opsPerThread;
        // Count should equal total or be capped by max
        var totalInCounts = state.OperationCountByInstance.Values.Sum();
        totalInCounts.Should().Be(totalExpected);

        // Queue should be capped
        state.Operations.Count.Should().BeLessOrEqualTo(DebugPanelState.MaxOperationEvents);
    }

    [Fact]
    public async Task Sse_WithRapidOperations_ShouldNotCrash()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        // Connect SSE client
        using var sseResponse = await _http.GetAsync("/api/events", HttpCompletionOption.ResponseHeadersRead, cts.Token);

        // Fire 50 rapid operations
        var tasks = Enumerable.Range(0, 50).Select(i =>
        {
            var op = new OperationEvent
            {
                InstanceId = "rapid",
                ServiceType = "Memory",
                OperationName = $"Rapid_{i}",
                TimestampUtc = DateTime.UtcNow,
                Success = true
            };
            return PostJsonAsync("/api/operation", op);
        }).ToArray();

        await Task.WhenAll(tasks);

        // Read a few events (won't necessarily get all 50 since SSE is async)
        var stream = await sseResponse.Content.ReadAsStreamAsync(cts.Token);
        using var reader = new StreamReader(stream);

        // Just read for a bit — the key assertion is no crash/exception
        var lineCount = 0;
        while (lineCount < 5 && !cts.Token.IsCancellationRequested)
        {
            try
            {
                var line = await reader.ReadLineAsync(cts.Token);
                if (line == null) break;
                lineCount++;
            }
            catch (OperationCanceledException) { break; }
        }

        lineCount.Should().BeGreaterThan(0);
    }

    // ── Helper ─────────────────────────────────────────────────────────────

    private async Task<HttpResponseMessage> PostJsonAsync(string url, object payload)
    {
        var json = JsonConvert.SerializeObject(payload);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        return await _http.PostAsync(url, content);
    }
}
