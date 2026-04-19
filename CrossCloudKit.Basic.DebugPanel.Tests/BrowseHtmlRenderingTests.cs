// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using FluentAssertions;
using Xunit;

namespace CrossCloudKit.Basic.DebugPanel.Tests;

/// <summary>
/// Tests for browse-related UI elements in <see cref="DebugPanelHtmlRenderer"/>.
/// Covers Browse Data button rendering, browse panel section, modal overlay,
/// and XSS safety in browse-related content.
/// </summary>
public class BrowseHtmlRenderingTests : IAsyncLifetime
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

    // ── Browse button rendering ───────────────────────────────────────────

    [Fact]
    public async Task Html_ServiceWithProvider_ShowsBrowseButton()
    {
        var reg = new ServiceRegistration
        {
            InstanceId = "with-browse",
            ServiceType = "Database",
            Path = "/tmp/db1",
            ProcessId = 1,
            MachineName = "test",
            StartedAtUtc = DateTime.UtcNow
        };
        _server.State.Register(reg);
        _server.State.SetDataProvider("with-browse", new StubProvider());

        var html = await _http.GetStringAsync("/");
        html.Should().Contain("btn-browse");
        html.Should().Contain("Browse Data");
        html.Should().Contain("browse('with-browse'");
    }

    [Fact]
    public async Task Html_ServiceWithoutProvider_NoBrowseButton()
    {
        var reg = new ServiceRegistration
        {
            InstanceId = "no-browse",
            ServiceType = "PubSub",
            Path = "/tmp/ps1",
            ProcessId = 1,
            MachineName = "test",
            StartedAtUtc = DateTime.UtcNow
        };
        _server.State.Register(reg);
        // No data provider set

        var html = await _http.GetStringAsync("/");
        html.Should().Contain("PubSub");
        html.Should().NotContain("browse('no-browse'");
    }

    [Fact]
    public async Task Html_MixedServices_OnlyBrowsableGetButton()
    {
        _server.State.Register(new ServiceRegistration
        {
            InstanceId = "browsable-1",
            ServiceType = "Memory",
            Path = "/tmp/m1",
            ProcessId = 1,
            MachineName = "test",
            StartedAtUtc = DateTime.UtcNow
        });
        _server.State.SetDataProvider("browsable-1", new StubProvider());

        _server.State.Register(new ServiceRegistration
        {
            InstanceId = "non-browsable-1",
            ServiceType = "PubSub",
            Path = "/tmp/ps1",
            ProcessId = 1,
            MachineName = "test",
            StartedAtUtc = DateTime.UtcNow
        });

        var html = await _http.GetStringAsync("/");
        html.Should().Contain("browse('browsable-1'");
        html.Should().NotContain("browse('non-browsable-1'");
    }

    // ── Browse panel structure ────────────────────────────────────────────

    [Fact]
    public async Task Html_ContainsBrowsePanel()
    {
        var html = await _http.GetStringAsync("/");
        html.Should().Contain("id=\"browse-panel\"");
        html.Should().Contain("browse-panel hidden");
        html.Should().Contain("id=\"browse-title\"");
        html.Should().Contain("id=\"browse-breadcrumb\"");
        html.Should().Contain("id=\"browse-content\"");
        html.Should().Contain("id=\"browse-close\"");
    }

    // ── Modal overlay structure ───────────────────────────────────────────

    [Fact]
    public async Task Html_ContainsModalOverlay()
    {
        var html = await _http.GetStringAsync("/");
        html.Should().Contain("id=\"modal-overlay\"");
        html.Should().Contain("modal-overlay hidden");
        html.Should().Contain("id=\"modal-title\"");
        html.Should().Contain("id=\"modal-summary\"");
        html.Should().Contain("id=\"modal-content\"");
        html.Should().Contain("id=\"modal-close\"");
    }

    // ── JavaScript browse functions ───────────────────────────────────────

    [Fact]
    public async Task Html_ContainsBrowseJsFunctions()
    {
        var html = await _http.GetStringAsync("/");
        html.Should().Contain("window.browse=");
        html.Should().Contain("showContainers");
        html.Should().Contain("window.showItems=");
        html.Should().Contain("window.showDetail=");
        html.Should().Contain("escAttr");
    }

    [Fact]
    public async Task Html_JsFetches_UseBrowseApiPaths()
    {
        var html = await _http.GetStringAsync("/");
        html.Should().Contain("/api/browse/");
        html.Should().Contain("/containers");
        html.Should().Contain("/items?container=");
        html.Should().Contain("/detail?container=");
        html.Should().Contain("/browsable");
    }

    [Fact]
    public async Task Html_ModalEscapeKeyBinding()
    {
        var html = await _http.GetStringAsync("/");
        html.Should().Contain("Escape");
        html.Should().Contain("modalOverlay.classList.add('hidden')");
    }

    // ── CSS for browse elements ───────────────────────────────────────────

    [Fact]
    public async Task Html_ContainsBrowseCssClasses()
    {
        var html = await _http.GetStringAsync("/");
        html.Should().Contain(".browse-panel");
        html.Should().Contain(".container-card");
        html.Should().Contain(".item-table");
        html.Should().Contain(".modal-overlay");
        html.Should().Contain(".modal");
        html.Should().Contain(".btn-browse");
    }

    // ── XSS safety in browse context ──────────────────────────────────────

    [Fact]
    public async Task Html_XssInInstanceId_IsEscaped()
    {
        _server.State.Register(new ServiceRegistration
        {
            InstanceId = "<script>alert(1)</script>",
            ServiceType = "Memory",
            Path = "/tmp/xss",
            ProcessId = 1,
            MachineName = "test",
            StartedAtUtc = DateTime.UtcNow
        });
        _server.State.SetDataProvider("<script>alert(1)</script>", new StubProvider());

        var html = await _http.GetStringAsync("/");
        html.Should().NotContain("<script>alert(1)</script>");
    }

    [Fact]
    public async Task Html_DataTypeAttribute_IsEscaped()
    {
        _server.State.Register(new ServiceRegistration
        {
            InstanceId = "xss-type",
            ServiceType = "\"><script>xss</script>",
            Path = "/tmp/ok",
            ProcessId = 1,
            MachineName = "test",
            StartedAtUtc = DateTime.UtcNow
        });
        _server.State.SetDataProvider("xss-type", new StubProvider());

        var html = await _http.GetStringAsync("/");
        html.Should().NotContain("<script>xss</script>");
        html.Should().Contain("&lt;script&gt;xss&lt;/script&gt;");
    }

    // ── Stub ──────────────────────────────────────────────────────────────

    private sealed class StubProvider : IDebugDataProvider
    {
        public Task<List<DebugContainer>> ListContainersAsync() => Task.FromResult(new List<DebugContainer>());
        public Task<List<DebugItem>> ListItemsAsync(string container, int maxItems = 200) => Task.FromResult(new List<DebugItem>());
        public Task<DebugItemDetail?> GetItemDetailAsync(string container, string itemId) => Task.FromResult<DebugItemDetail?>(null);
    }
}
