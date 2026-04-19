// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Text;
using FluentAssertions;
using Newtonsoft.Json;
using Xunit;

namespace CrossCloudKit.Basic.DebugPanel.Tests;

/// <summary>
/// Tests for Server-Sent Events endpoint (/api/events).
/// </summary>
public class SseTests : IAsyncLifetime
{
    private DebugPanelServer _server = null!;
    private HttpClient _http = null!;
    private int _port;

    public async Task InitializeAsync()
    {
        _port = TestHelpers.GetAvailablePort();
        _server = new DebugPanelServer();
        await _server.StartAsync(_port);
        _http = new HttpClient { BaseAddress = new Uri($"http://localhost:{_port}"), Timeout = TimeSpan.FromSeconds(10) };
    }

    public async Task DisposeAsync()
    {
        _http.Dispose();
        await _server.DisposeAsync();
    }

    [Fact]
    public async Task Sse_ShouldReceiveHeartbeatOnConnect()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        using var response = await _http.GetAsync("/api/events", HttpCompletionOption.ResponseHeadersRead, cts.Token);

        response.Content.Headers.ContentType!.MediaType.Should().Be("text/event-stream");

        var stream = await response.Content.ReadAsStreamAsync(cts.Token);
        using var reader = new StreamReader(stream);

        var events = await ReadSseEventsAsync(reader, 1, cts.Token);
        events.Should().Contain(e => e.EventType == "heartbeat" && e.Data == "connected");
    }

    [Fact]
    public async Task Sse_ShouldReceiveServiceRegisteredEvent()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        // Connect SSE first
        using var response = await _http.GetAsync("/api/events", HttpCompletionOption.ResponseHeadersRead, cts.Token);
        var stream = await response.Content.ReadAsStreamAsync(cts.Token);
        using var reader = new StreamReader(stream);

        // Read past heartbeat
        await ReadSseEventsAsync(reader, 1, cts.Token);

        // Register a service
        var reg = new ServiceRegistration
        {
            InstanceId = "sse-reg-1",
            ServiceType = "Memory",
            Path = "/tmp/sse",
            ProcessId = 1,
            MachineName = "test",
            StartedAtUtc = DateTime.UtcNow
        };
        using var content = new StringContent(JsonConvert.SerializeObject(reg), Encoding.UTF8, "application/json");
        await _http.PostAsync("/api/register", content, cts.Token);

        var events = await ReadSseEventsAsync(reader, 1, cts.Token);
        events.Should().Contain(e => e.EventType == "service-registered");
    }

    [Fact]
    public async Task Sse_ShouldReceiveOperationEvent()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        using var response = await _http.GetAsync("/api/events", HttpCompletionOption.ResponseHeadersRead, cts.Token);
        var stream = await response.Content.ReadAsStreamAsync(cts.Token);
        using var reader = new StreamReader(stream);

        await ReadSseEventsAsync(reader, 1, cts.Token); // heartbeat

        var op = new OperationEvent
        {
            InstanceId = "sse-op-1",
            ServiceType = "Vector",
            OperationName = "Upsert",
            TimestampUtc = DateTime.UtcNow,
            Success = true
        };
        using var content = new StringContent(JsonConvert.SerializeObject(op), Encoding.UTF8, "application/json");
        await _http.PostAsync("/api/operation", content, cts.Token);

        var events = await ReadSseEventsAsync(reader, 1, cts.Token);
        events.Should().Contain(e => e.EventType == "operation");
    }

    [Fact]
    public async Task Sse_ShouldReceiveDeregistrationEvent()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        // Register first
        var reg = new ServiceRegistration
        {
            InstanceId = "sse-dereg-1",
            ServiceType = "Database",
            Path = "/tmp/sse-dereg",
            ProcessId = 1,
            MachineName = "test",
            StartedAtUtc = DateTime.UtcNow
        };
        using var regContent = new StringContent(JsonConvert.SerializeObject(reg), Encoding.UTF8, "application/json");
        await _http.PostAsync("/api/register", regContent, cts.Token);

        // Connect SSE
        using var response = await _http.GetAsync("/api/events", HttpCompletionOption.ResponseHeadersRead, cts.Token);
        var stream = await response.Content.ReadAsStreamAsync(cts.Token);
        using var reader = new StreamReader(stream);

        await ReadSseEventsAsync(reader, 1, cts.Token); // heartbeat

        // Deregister
        var deregPayload = JsonConvert.SerializeObject(new { InstanceId = "sse-dereg-1" });
        using var deregContent = new StringContent(deregPayload, Encoding.UTF8, "application/json");
        await _http.PostAsync("/api/deregister", deregContent, cts.Token);

        var events = await ReadSseEventsAsync(reader, 1, cts.Token);
        events.Should().Contain(e => e.EventType == "service-deregistered");
    }

    [Fact]
    public async Task Sse_MultipleConcurrentClients_ShouldAllReceiveEvents()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        // Connect two SSE clients
        using var response1 = await _http.GetAsync("/api/events", HttpCompletionOption.ResponseHeadersRead, cts.Token);
        using var response2 = await _http.GetAsync("/api/events", HttpCompletionOption.ResponseHeadersRead, cts.Token);

        var stream1 = await response1.Content.ReadAsStreamAsync(cts.Token);
        var stream2 = await response2.Content.ReadAsStreamAsync(cts.Token);
        using var reader1 = new StreamReader(stream1);
        using var reader2 = new StreamReader(stream2);

        // Read past heartbeats
        await ReadSseEventsAsync(reader1, 1, cts.Token);
        await ReadSseEventsAsync(reader2, 1, cts.Token);

        // Register a service
        var reg = new ServiceRegistration
        {
            InstanceId = "sse-multi-1",
            ServiceType = "File",
            Path = "/tmp/multi",
            ProcessId = 1,
            MachineName = "test",
            StartedAtUtc = DateTime.UtcNow
        };
        using var content = new StringContent(JsonConvert.SerializeObject(reg), Encoding.UTF8, "application/json");
        await _http.PostAsync("/api/register", content, cts.Token);

        // Both clients should receive the event
        var events1 = await ReadSseEventsAsync(reader1, 1, cts.Token);
        var events2 = await ReadSseEventsAsync(reader2, 1, cts.Token);

        events1.Should().Contain(e => e.EventType == "service-registered");
        events2.Should().Contain(e => e.EventType == "service-registered");
    }

    // ── SSE parser ────────────────────────────────────────────────────────

    private record SseEvent(string EventType, string Data);

    private static async Task<List<SseEvent>> ReadSseEventsAsync(StreamReader reader, int count, CancellationToken ct)
    {
        var events = new List<SseEvent>();
        string? currentEvent = null;
        string? currentData = null;

        while (events.Count < count)
        {
            var line = await reader.ReadLineAsync(ct);
            if (line == null) break;

            if (line.StartsWith("event: "))
                currentEvent = line["event: ".Length..];
            else if (line.StartsWith("data: "))
                currentData = line["data: ".Length..];
            else if (line == "" && currentEvent != null && currentData != null)
            {
                events.Add(new SseEvent(currentEvent, currentData));
                currentEvent = null;
                currentData = null;
            }
        }

        return events;
    }
}
