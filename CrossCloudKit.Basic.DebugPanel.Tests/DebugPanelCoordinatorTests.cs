// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;
using System.Text;
using FluentAssertions;
using Newtonsoft.Json;
using Xunit;

namespace CrossCloudKit.Basic.DebugPanel.Tests;

/// <summary>
/// Tests for <see cref="DebugPanelCoordinator"/> — singleton server management, first-starts/second-reuses, process mutex.
/// </summary>
[Collection("Coordinator")]
public class DebugPanelCoordinatorTests : IAsyncLifetime
{
    private int _port;

    public Task InitializeAsync()
    {
        _port = TestHelpers.GetAvailablePort();
        Environment.SetEnvironmentVariable("CROSSCLOUDKIT_DEBUG_PANEL_DISABLED", null);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await DebugPanelCoordinator.ResetAsync();
        Environment.SetEnvironmentVariable("CROSSCLOUDKIT_DEBUG_PANEL_DISABLED", "true");
    }

    [Fact]
    public async Task RegisterAsync_ShouldStartServer_WhenFirstService()
    {
        var tracker = await DebugPanelCoordinator.RegisterAsync("Memory", "/tmp/coord1", port: _port);
        tracker.Should().NotBeNull();
        DebugPanelCoordinator.Server.Should().NotBeNull();
        DebugPanelCoordinator.Server!.IsRunning.Should().BeTrue();
        DebugPanelCoordinator.Server.Port.Should().Be(_port);
    }

    [Fact]
    public async Task RegisterAsync_SecondService_ShouldReuseSameServer()
    {
        var t1 = await DebugPanelCoordinator.RegisterAsync("Memory", "/tmp/coord2a", port: _port);
        var serverBefore = DebugPanelCoordinator.Server;

        var t2 = await DebugPanelCoordinator.RegisterAsync("Database", "/tmp/coord2b", port: _port);
        var serverAfter = DebugPanelCoordinator.Server;

        serverBefore.Should().BeSameAs(serverAfter);
        t1.Should().NotBeNull();
        t2.Should().NotBeNull();
    }

    [Fact]
    public async Task DeregisterAsync_ShouldRemoveService()
    {
        var tracker = await DebugPanelCoordinator.RegisterAsync("Memory", "/tmp/coord3", port: _port);
        tracker.Should().NotBeNull();

        using var http = new HttpClient { BaseAddress = new Uri($"http://localhost:{_port}") };
        var servicesJson = await http.GetStringAsync("/api/services");
        var services = JsonConvert.DeserializeObject<List<ServiceRegistration>>(servicesJson);
        services.Should().ContainSingle();

        // Need to know the instance ID - read it from state
        var instanceId = services!.First().InstanceId;
        await DebugPanelCoordinator.DeregisterAsync(instanceId);

        servicesJson = await http.GetStringAsync("/api/services");
        services = JsonConvert.DeserializeObject<List<ServiceRegistration>>(servicesJson);
        services.Should().BeEmpty();
    }

    [Fact]
    public async Task DeregisterAsync_LastService_ShouldTriggerAutoShutdown()
    {
        var tracker = await DebugPanelCoordinator.RegisterAsync("Memory", "/tmp/coord4", port: _port);
        var server = DebugPanelCoordinator.Server!;
        server.ShutdownGracePeriod = TimeSpan.FromMilliseconds(300);

        // Get instance ID from state
        var instanceId = server.State.Services.Keys.First();
        await DebugPanelCoordinator.DeregisterAsync(instanceId);

        // Wait for auto-shutdown
        await Task.Delay(600);
        server.IsRunning.Should().BeFalse();
    }

    [Fact]
    public async Task RegisterAsync_AfterShutdown_ShouldStartNewServer()
    {
        var t1 = await DebugPanelCoordinator.RegisterAsync("Memory", "/tmp/coord5a", port: _port);
        var server1 = DebugPanelCoordinator.Server!;
        server1.ShutdownGracePeriod = TimeSpan.FromMilliseconds(100);

        var instanceId = server1.State.Services.Keys.First();
        await DebugPanelCoordinator.DeregisterAsync(instanceId);

        // Wait for auto-shutdown
        await Task.Delay(400);
        server1.IsRunning.Should().BeFalse();

        // Register again — should start a new server
        var newPort = TestHelpers.GetAvailablePort();
        var t2 = await DebugPanelCoordinator.RegisterAsync("Vector", "/tmp/coord5b", port: newPort);
        t2.Should().NotBeNull();
        DebugPanelCoordinator.Server.Should().NotBeNull();
        DebugPanelCoordinator.Server!.IsRunning.Should().BeTrue();
    }

    [Fact]
    public async Task ResetAsync_ShouldStopServerAndClearState()
    {
        await DebugPanelCoordinator.RegisterAsync("Memory", "/tmp/coord6", port: _port);
        DebugPanelCoordinator.Server.Should().NotBeNull();

        await DebugPanelCoordinator.ResetAsync();

        DebugPanelCoordinator.Server.Should().BeNull();
    }

    [Fact]
    public async Task DeregisterAsync_UnknownId_ShouldNotThrow()
    {
        await DebugPanelCoordinator.RegisterAsync("Memory", "/tmp/coord7", port: _port);

        // Should not throw
        await DebugPanelCoordinator.DeregisterAsync("this-does-not-exist");
    }
}
