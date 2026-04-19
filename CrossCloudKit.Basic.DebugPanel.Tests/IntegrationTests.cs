// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Database.Basic;
using CrossCloudKit.File.Basic;
using CrossCloudKit.Memory.Basic;
using CrossCloudKit.PubSub.Basic;
using CrossCloudKit.Vector.Basic;
using FluentAssertions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CrossCloudKit.Basic.DebugPanel.Tests;

/// <summary>
/// Integration tests — real Basic service instances interacting with the debug panel.
/// These tests use the <see cref="DebugPanelCoordinator"/> + real service constructors.
/// </summary>
[Collection("Coordinator")]
public class IntegrationTests : IAsyncLifetime
{
    private int _port;
    private string _tempDir = null!;

    public Task InitializeAsync()
    {
        _port = TestHelpers.GetAvailablePort();
        _tempDir = TestHelpers.CreateTempDir();
        // Ensure CROSSCLOUDKIT_DEBUG_PANEL_DISABLED is NOT set for integration tests
        Environment.SetEnvironmentVariable("CROSSCLOUDKIT_DEBUG_PANEL_DISABLED", null);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await DebugPanelCoordinator.ResetAsync();
        TestHelpers.CleanupDir(_tempDir);
        // Re-disable for other test classes
        Environment.SetEnvironmentVariable("CROSSCLOUDKIT_DEBUG_PANEL_DISABLED", "true");
    }

    [Fact]
    public async Task MemoryServiceBasic_ShouldRegisterInPanel()
    {
        Environment.SetEnvironmentVariable("CROSSCLOUDKIT_DEBUG_PANEL_PORT", _port.ToString());
        await using var memoryService = new MemoryServiceBasic();

        // Give the fire-and-forget registration time to complete
        await Task.Delay(1000);

        DebugPanelCoordinator.Server.Should().NotBeNull();
        DebugPanelCoordinator.Server!.State.Services.Values
            .Should().Contain(s => s.ServiceType == "Memory");
    }

    [Fact]
    public async Task VectorServiceBasic_ShouldRegisterInPanel()
    {
        Environment.SetEnvironmentVariable("CROSSCLOUDKIT_DEBUG_PANEL_PORT", _port.ToString());
        await using var vectorService = new VectorServiceBasic();

        await Task.Delay(1000);

        DebugPanelCoordinator.Server.Should().NotBeNull();
        DebugPanelCoordinator.Server!.State.Services.Values
            .Should().Contain(s => s.ServiceType == "Vector");
    }

    [Fact]
    public async Task MultipleBasicServices_ShouldAllAppearInPanel()
    {
        Environment.SetEnvironmentVariable("CROSSCLOUDKIT_DEBUG_PANEL_PORT", _port.ToString());

        await using var memoryService = new MemoryServiceBasic();
        var pubSubService = new PubSubServiceBasic();

        await Task.Delay(1500);

        DebugPanelCoordinator.Server.Should().NotBeNull();
        var serviceTypes = DebugPanelCoordinator.Server!.State.Services.Values
            .Select(s => s.ServiceType).ToList();

        serviceTypes.Should().Contain("Memory");
        serviceTypes.Should().Contain("PubSub");

        await pubSubService.DisposeAsync();
    }

    [Fact]
    public async Task DisposeService_ShouldDeregisterFromPanel()
    {
        Environment.SetEnvironmentVariable("CROSSCLOUDKIT_DEBUG_PANEL_PORT", _port.ToString());

        var memoryService = new MemoryServiceBasic();
        await Task.Delay(1000);

        DebugPanelCoordinator.Server.Should().NotBeNull();
        var countBefore = DebugPanelCoordinator.Server!.State.Services.Values
            .Count(s => s.ServiceType == "Memory");
        countBefore.Should().BeGreaterOrEqualTo(1);

        await memoryService.DisposeAsync();
        await Task.Delay(500);

        var countAfter = DebugPanelCoordinator.Server!.State.Services.Values
            .Count(s => s.ServiceType == "Memory");
        countAfter.Should().BeLessThan(countBefore);
    }

    [Fact]
    public async Task MultipleSameTypeServices_DifferentPaths_ShouldAllRegister()
    {
        Environment.SetEnvironmentVariable("CROSSCLOUDKIT_DEBUG_PANEL_PORT", _port.ToString());

        await using var mem1 = new MemoryServiceBasic();
        await using var mem2 = new MemoryServiceBasic();

        await Task.Delay(1500);

        DebugPanelCoordinator.Server.Should().NotBeNull();
        var memoryServices = DebugPanelCoordinator.Server!.State.Services.Values
            .Where(s => s.ServiceType == "Memory").ToList();

        memoryServices.Should().HaveCountGreaterOrEqualTo(2);
    }

    [Fact]
    public async Task PanelHtml_ShouldShowRealServices()
    {
        Environment.SetEnvironmentVariable("CROSSCLOUDKIT_DEBUG_PANEL_PORT", _port.ToString());

        await using var memoryService = new MemoryServiceBasic();
        await Task.Delay(1000);

        using var http = new HttpClient { BaseAddress = new Uri($"http://localhost:{_port}") };
        var html = await http.GetStringAsync("/");

        html.Should().Contain("Memory");
        html.Should().Contain("CrossCloudKit Debug Panel");
    }

    [Fact]
    public async Task DisabledEnvVar_ShouldPreventRegistration()
    {
        Environment.SetEnvironmentVariable("CROSSCLOUDKIT_DEBUG_PANEL_DISABLED", "true");
        Environment.SetEnvironmentVariable("CROSSCLOUDKIT_DEBUG_PANEL_PORT", _port.ToString());

        await using var memoryService = new MemoryServiceBasic();
        await Task.Delay(500);

        // Server should NOT have started
        DebugPanelCoordinator.Server.Should().BeNull();

        // Re-clear for other tests
        Environment.SetEnvironmentVariable("CROSSCLOUDKIT_DEBUG_PANEL_DISABLED", null);
    }
}
