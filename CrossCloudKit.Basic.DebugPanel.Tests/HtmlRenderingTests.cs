// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;
using FluentAssertions;
using Xunit;

namespace CrossCloudKit.Basic.DebugPanel.Tests;

/// <summary>
/// Tests for <see cref="DebugPanelHtmlRenderer"/> output quality and XSS safety.
/// </summary>
public class HtmlRenderingTests : IAsyncLifetime
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

    [Fact]
    public async Task Html_ShouldContainValidStructure()
    {
        var html = await _http.GetStringAsync("/");
        html.Should().StartWith("<!DOCTYPE html>");
        html.Should().Contain("<html");
        html.Should().Contain("</html>");
        html.Should().Contain("<head>");
        html.Should().Contain("<body>");
    }

    [Fact]
    public async Task Html_ShouldContainTitle()
    {
        var html = await _http.GetStringAsync("/");
        html.Should().Contain("<title>CrossCloudKit Debug Panel</title>");
    }

    [Fact]
    public async Task Html_ShouldContainInlineCss()
    {
        var html = await _http.GetStringAsync("/");
        html.Should().Contain("<style>");
        html.Should().Contain("</style>");
    }

    [Fact]
    public async Task Html_ShouldContainInlineJs()
    {
        var html = await _http.GetStringAsync("/");
        html.Should().Contain("<script>");
        html.Should().Contain("EventSource");
    }

    [Fact]
    public async Task Html_EmptyState_ShouldShowNoServices()
    {
        var html = await _http.GetStringAsync("/");
        html.Should().Contain("No services registered");
    }

    [Fact]
    public async Task Html_WithService_ShouldShowServiceCard()
    {
        _server.State.Register(new ServiceRegistration
        {
            InstanceId = "html-1",
            ServiceType = "Memory",
            Path = "/tmp/html-test",
            ProcessId = 42,
            MachineName = "test-machine",
            StartedAtUtc = DateTime.UtcNow
        });

        var html = await _http.GetStringAsync("/");
        html.Should().Contain("Memory");
        html.Should().Contain("/tmp/html-test");
    }

    [Fact]
    public async Task Html_ShouldGroupByServiceType()
    {
        _server.State.Register(new ServiceRegistration { InstanceId = "g1", ServiceType = "Memory", Path = "/a", ProcessId = 1, MachineName = "m", StartedAtUtc = DateTime.UtcNow });
        _server.State.Register(new ServiceRegistration { InstanceId = "g2", ServiceType = "Memory", Path = "/b", ProcessId = 1, MachineName = "m", StartedAtUtc = DateTime.UtcNow });
        _server.State.Register(new ServiceRegistration { InstanceId = "g3", ServiceType = "Database", Path = "/c", ProcessId = 1, MachineName = "m", StartedAtUtc = DateTime.UtcNow });

        var html = await _http.GetStringAsync("/");
        // Should have both service type headings
        html.Should().Contain("Memory");
        html.Should().Contain("Database");
    }

    [Fact]
    public async Task Html_WithOperations_ShouldShowTable()
    {
        _server.State.AddOperation(new OperationEvent
        {
            InstanceId = "ops-1",
            ServiceType = "Vector",
            OperationName = "Query",
            Details = "collection=docs",
            DurationMs = 123,
            TimestampUtc = DateTime.UtcNow,
            Success = true
        });

        var html = await _http.GetStringAsync("/");
        html.Should().Contain("Query");
        html.Should().Contain("123");
    }

    // ── XSS Safety ────────────────────────────────────────────────────────

    [Fact]
    public async Task Html_ShouldEscapeXssInServiceType()
    {
        _server.State.Register(new ServiceRegistration
        {
            InstanceId = "xss-1",
            ServiceType = "<script>alert('xss')</script>",
            Path = "/tmp/safe",
            ProcessId = 1,
            MachineName = "test",
            StartedAtUtc = DateTime.UtcNow
        });

        var html = await _http.GetStringAsync("/");
        html.Should().NotContain("<script>alert('xss')</script>");
        html.Should().Contain("&lt;script&gt;");
    }

    [Fact]
    public async Task Html_ShouldEscapeXssInPath()
    {
        _server.State.Register(new ServiceRegistration
        {
            InstanceId = "xss-2",
            ServiceType = "Memory",
            Path = "<img src=x onerror=alert(1)>",
            ProcessId = 1,
            MachineName = "test",
            StartedAtUtc = DateTime.UtcNow
        });

        var html = await _http.GetStringAsync("/");
        html.Should().NotContain("<img src=x");
        html.Should().Contain("&lt;img");
    }

    [Fact]
    public async Task Html_ShouldEscapeXssInOperationName()
    {
        _server.State.AddOperation(new OperationEvent
        {
            InstanceId = "xss-3",
            ServiceType = "DB",
            OperationName = "<script>alert('xss')</script>",
            TimestampUtc = DateTime.UtcNow,
            Success = true
        });

        var html = await _http.GetStringAsync("/");
        html.Should().NotContain("<script>alert('xss')</script>");
        html.Should().Contain("&lt;script&gt;");
    }

    [Fact]
    public async Task Html_ShouldEscapeXssInDetails()
    {
        _server.State.AddOperation(new OperationEvent
        {
            InstanceId = "xss-4",
            ServiceType = "DB",
            OperationName = "Put",
            Details = "<b>bold</b><script>document.cookie</script>",
            TimestampUtc = DateTime.UtcNow,
            Success = true
        });

        var html = await _http.GetStringAsync("/");
        html.Should().NotContain("<b>bold</b>");
        html.Should().Contain("&lt;b&gt;bold&lt;/b&gt;");
    }

    [Fact]
    public async Task Html_ShouldEscapeXssInMachineName()
    {
        _server.State.Register(new ServiceRegistration
        {
            InstanceId = "xss-5",
            ServiceType = "File",
            Path = "/tmp/ok",
            ProcessId = 1,
            MachineName = "<script>steal()</script>",
            StartedAtUtc = DateTime.UtcNow
        });

        var html = await _http.GetStringAsync("/");
        html.Should().NotContain("<script>steal()</script>");
        html.Should().Contain("&lt;script&gt;");
    }
}
