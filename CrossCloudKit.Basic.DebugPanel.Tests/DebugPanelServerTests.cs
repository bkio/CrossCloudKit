// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;
using System.Text;
using FluentAssertions;
using Newtonsoft.Json;
using Xunit;

namespace CrossCloudKit.Basic.DebugPanel.Tests;

/// <summary>
/// Tests for <see cref="DebugPanelServer"/> lifecycle, endpoints, and shutdown behavior.
/// </summary>
public class DebugPanelServerTests : IAsyncLifetime
{
    private DebugPanelServer _server = null!;
    private HttpClient _http = null!;
    private int _port;

    public async Task InitializeAsync()
    {
        _port = TestHelpers.GetAvailablePort();
        _server = new DebugPanelServer();
        await _server.StartAsync(_port);
        _http = new HttpClient { BaseAddress = new Uri($"http://localhost:{_port}") };
    }

    public async Task DisposeAsync()
    {
        _http.Dispose();
        await _server.DisposeAsync();
    }

    // ── Server lifecycle ──────────────────────────────────────────────────

    [Fact]
    public void Server_ShouldBeRunning_AfterStart()
    {
        _server.IsRunning.Should().BeTrue();
        _server.Port.Should().Be(_port);
    }

    [Fact]
    public async Task Server_ShouldRespondToGetRoot_WithHtml()
    {
        var response = await _http.GetAsync("/");
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Content.Headers.ContentType!.MediaType.Should().Be("text/html");
        var html = await response.Content.ReadAsStringAsync();
        html.Should().Contain("CrossCloudKit Debug Panel");
    }

    [Fact]
    public async Task Server_StopAsync_ShouldStopAcceptingRequests()
    {
        await _server.StopAsync();
        _server.IsRunning.Should().BeFalse();
        _server.Port.Should().Be(0);
    }

    [Fact]
    public async Task Server_MultipleStartStop_ShouldWork()
    {
        await _server.StopAsync();
        _server.IsRunning.Should().BeFalse();

        var newPort = TestHelpers.GetAvailablePort();
        await _server.StartAsync(newPort);
        _server.IsRunning.Should().BeTrue();
        _server.Port.Should().Be(newPort);

        using var http2 = new HttpClient { BaseAddress = new Uri($"http://localhost:{newPort}") };
        var response = await http2.GetAsync("/");
        response.StatusCode.Should().Be(HttpStatusCode.OK);

        await _server.StopAsync();
    }

    [Fact]
    public async Task Server_DoubleDispose_ShouldNotThrow()
    {
        await _server.DisposeAsync();
        await _server.DisposeAsync();
    }

    [Fact]
    public async Task Server_StartWhenAlreadyRunning_ShouldThrow()
    {
        var act = () => _server.StartAsync(_port);
        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    // ── Registration endpoints ────────────────────────────────────────────

    [Fact]
    public async Task Register_ShouldAddServiceToState()
    {
        var reg = CreateRegistration("inst-1", "Memory", "/tmp/mem");
        await PostJsonAsync("/api/register", reg);

        var response = await _http.GetStringAsync("/api/services");
        var services = JsonConvert.DeserializeObject<List<ServiceRegistration>>(response);
        services.Should().ContainSingle(s => s.InstanceId == "inst-1");
    }

    [Fact]
    public async Task Register_MultipleServices_ShouldAllAppear()
    {
        await PostJsonAsync("/api/register", CreateRegistration("a", "Memory", "/tmp/a"));
        await PostJsonAsync("/api/register", CreateRegistration("b", "Database", "/tmp/b"));
        await PostJsonAsync("/api/register", CreateRegistration("c", "Vector", "/tmp/c"));

        var response = await _http.GetStringAsync("/api/services");
        var services = JsonConvert.DeserializeObject<List<ServiceRegistration>>(response);
        services.Should().HaveCount(3);
    }

    [Fact]
    public async Task Register_SameInstanceIdTwice_ShouldBeIdempotent()
    {
        await PostJsonAsync("/api/register", CreateRegistration("dup", "Memory", "/tmp/dup"));
        await PostJsonAsync("/api/register", CreateRegistration("dup", "Memory", "/tmp/dup-v2"));

        var response = await _http.GetStringAsync("/api/services");
        var services = JsonConvert.DeserializeObject<List<ServiceRegistration>>(response);
        services.Should().ContainSingle(s => s.InstanceId == "dup");
        services!.First().Path.Should().Be("/tmp/dup-v2"); // overwritten
    }

    [Fact]
    public async Task Deregister_ShouldRemoveService()
    {
        await PostJsonAsync("/api/register", CreateRegistration("del-1", "Memory", "/tmp/del"));
        await PostJsonAsync("/api/deregister", new { InstanceId = "del-1" });

        var response = await _http.GetStringAsync("/api/services");
        var services = JsonConvert.DeserializeObject<List<ServiceRegistration>>(response);
        services.Should().BeEmpty();
    }

    [Fact]
    public async Task Deregister_UnknownInstanceId_ShouldNotFail()
    {
        var response = await PostJsonAsync("/api/deregister", new { InstanceId = "nonexistent" });
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task Register_InvalidPayload_ShouldReturnBadRequest()
    {
        var response = await PostJsonAsync("/api/register", new { Foo = "bar" });
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    // ── Operation logging ─────────────────────────────────────────────────

    [Fact]
    public async Task Operation_ShouldAppearInState()
    {
        await PostJsonAsync("/api/register", CreateRegistration("op-inst", "Memory", "/tmp/mem"));

        var op = new OperationEvent
        {
            InstanceId = "op-inst",
            ServiceType = "Memory",
            OperationName = "SetKeyValues",
            Details = "scope=test",
            DurationMs = 42,
            TimestampUtc = DateTime.UtcNow,
            Success = true
        };
        await PostJsonAsync("/api/operation", op);

        _server.State.Operations.Should().ContainSingle(o => o.OperationName == "SetKeyValues");
        _server.State.OperationCountByInstance["op-inst"].Should().Be(1);
    }

    [Fact]
    public async Task Operation_RingBuffer_ShouldCapAtMaxSize()
    {
        await PostJsonAsync("/api/register", CreateRegistration("cap-inst", "Vector", "/tmp/vec"));

        for (var i = 0; i < DebugPanelState.MaxOperationEvents + 100; i++)
        {
            var op = new OperationEvent
            {
                InstanceId = "cap-inst",
                ServiceType = "Vector",
                OperationName = $"Op_{i}",
                TimestampUtc = DateTime.UtcNow,
                Success = true
            };
            _server.State.AddOperation(op);
        }

        _server.State.Operations.Count.Should().BeLessOrEqualTo(DebugPanelState.MaxOperationEvents);
    }

    [Fact]
    public async Task Operation_ForUnknownInstance_ShouldStillBeAccepted()
    {
        var op = new OperationEvent
        {
            InstanceId = "ghost",
            ServiceType = "Memory",
            OperationName = "GetKeyValue",
            TimestampUtc = DateTime.UtcNow,
            Success = true
        };
        var response = await PostJsonAsync("/api/operation", op);
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    // ── Auto-shutdown ─────────────────────────────────────────────────────

    [Fact]
    public async Task AutoShutdown_ShouldTrigger_AfterGracePeriod_WhenNoServices()
    {
        _server.ShutdownGracePeriod = TimeSpan.FromMilliseconds(300);

        await PostJsonAsync("/api/register", CreateRegistration("auto-1", "Memory", "/tmp/auto"));
        await PostJsonAsync("/api/deregister", new { InstanceId = "auto-1" });

        // Server should still be running during grace period
        await Task.Delay(100);
        _server.IsRunning.Should().BeTrue();

        // Wait for grace period to elapse
        await Task.Delay(500);
        _server.IsRunning.Should().BeFalse();
    }

    [Fact]
    public async Task AutoShutdown_ShouldBeCancelled_ByNewRegistration()
    {
        _server.ShutdownGracePeriod = TimeSpan.FromMilliseconds(500);

        await PostJsonAsync("/api/register", CreateRegistration("cancel-1", "Memory", "/tmp/c1"));
        await PostJsonAsync("/api/deregister", new { InstanceId = "cancel-1" });

        await Task.Delay(100);

        // Register a new service during grace period
        await PostJsonAsync("/api/register", CreateRegistration("cancel-2", "Vector", "/tmp/c2"));

        await Task.Delay(600);
        // Server should still be running because new registration cancelled shutdown
        _server.IsRunning.Should().BeTrue();
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private ServiceRegistration CreateRegistration(string instanceId, string serviceType, string path) =>
        new()
        {
            InstanceId = instanceId,
            ServiceType = serviceType,
            Path = path,
            ProcessId = Environment.ProcessId,
            MachineName = Environment.MachineName,
            StartedAtUtc = DateTime.UtcNow
        };

    private async Task<HttpResponseMessage> PostJsonAsync(string url, object payload)
    {
        var json = JsonConvert.SerializeObject(payload);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        return await _http.PostAsync(url, content);
    }
}
