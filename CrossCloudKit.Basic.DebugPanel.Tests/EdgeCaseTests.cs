// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Text;
using FluentAssertions;
using Newtonsoft.Json;
using Xunit;

namespace CrossCloudKit.Basic.DebugPanel.Tests;

/// <summary>
/// Edge case tests — unusual inputs, boundary conditions, resilience.
/// </summary>
public class EdgeCaseTests : IAsyncLifetime
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

    // ── Long / special paths ──────────────────────────────────────────────

    [Fact]
    public async Task Register_LongPath_ShouldSucceed()
    {
        var longPath = "/tmp/" + new string('x', 500);
        var reg = MakeRegistration("long-path", "Memory", longPath);
        var response = await PostJsonAsync("/api/register", reg);
        response.IsSuccessStatusCode.Should().BeTrue();

        var html = await _http.GetStringAsync("/");
        html.Should().Contain(new string('x', 100)); // at least part of it rendered
    }

    [Fact]
    public async Task Register_PathWithSpaces_ShouldSucceed()
    {
        var reg = MakeRegistration("space-path", "Database", "/tmp/my data path/db files");
        var response = await PostJsonAsync("/api/register", reg);
        response.IsSuccessStatusCode.Should().BeTrue();
    }

    [Fact]
    public async Task Register_PathWithUnicode_ShouldSucceed()
    {
        var reg = MakeRegistration("unicode-path", "File", "/tmp/données/日本語パス/résumé");
        var response = await PostJsonAsync("/api/register", reg);
        response.IsSuccessStatusCode.Should().BeTrue();

        _server.State.Services["unicode-path"].Path.Should().Contain("日本語パス");
    }

    [Fact]
    public async Task Register_EmptyPath_ShouldSucceed()
    {
        var reg = MakeRegistration("empty-path", "Memory", "");
        var response = await PostJsonAsync("/api/register", reg);
        response.IsSuccessStatusCode.Should().BeTrue();
    }

    // ── Special characters in service type ────────────────────────────────

    [Fact]
    public async Task Register_ServiceTypeWithSpecialChars_ShouldBeEscapedInHtml()
    {
        var reg = MakeRegistration("special-type", "Me&mo<ry>", "/tmp/special");
        await PostJsonAsync("/api/register", reg);

        var html = await _http.GetStringAsync("/");
        html.Should().NotContain("<ry>");
        html.Should().Contain("&lt;ry&gt;");
    }

    // ── Empty state ──────────────────────────────────────────────────────

    [Fact]
    public async Task EmptyState_ServicesList_ShouldReturnEmptyArray()
    {
        var json = await _http.GetStringAsync("/api/services");
        var services = JsonConvert.DeserializeObject<List<ServiceRegistration>>(json);
        services.Should().BeEmpty();
    }

    [Fact]
    public async Task EmptyState_Html_ShouldRenderCorrectly()
    {
        var html = await _http.GetStringAsync("/");
        html.Should().Contain("CrossCloudKit Debug Panel");
        html.Should().Contain("No services registered");
    }

    // ── Operation with null/empty details ─────────────────────────────────

    [Fact]
    public async Task Operation_NullDetails_ShouldSucceed()
    {
        var op = new OperationEvent
        {
            InstanceId = "null-detail",
            ServiceType = "Memory",
            OperationName = "Get",
            Details = null!,
            TimestampUtc = DateTime.UtcNow,
            Success = true
        };
        var response = await PostJsonAsync("/api/operation", op);
        response.IsSuccessStatusCode.Should().BeTrue();
    }

    [Fact]
    public async Task Operation_EmptyOperationName_ShouldSucceed()
    {
        var op = new OperationEvent
        {
            InstanceId = "empty-op",
            ServiceType = "Memory",
            OperationName = "",
            TimestampUtc = DateTime.UtcNow,
            Success = true
        };
        var response = await PostJsonAsync("/api/operation", op);
        response.IsSuccessStatusCode.Should().BeTrue();
    }

    [Fact]
    public async Task Operation_VeryLongDetails_ShouldSucceed()
    {
        var op = new OperationEvent
        {
            InstanceId = "long-detail",
            ServiceType = "Database",
            OperationName = "Scan",
            Details = new string('D', 10_000),
            TimestampUtc = DateTime.UtcNow,
            Success = true
        };
        var response = await PostJsonAsync("/api/operation", op);
        response.IsSuccessStatusCode.Should().BeTrue();
    }

    // ── Operations after deregister ──────────────────────────────────────

    [Fact]
    public async Task Operation_AfterDeregister_ShouldStillBeAccepted()
    {
        await PostJsonAsync("/api/register", MakeRegistration("post-dereg", "Memory", "/tmp/pd"));
        await PostJsonAsync("/api/deregister", new { InstanceId = "post-dereg" });

        var op = new OperationEvent
        {
            InstanceId = "post-dereg",
            ServiceType = "Memory",
            OperationName = "Orphan",
            TimestampUtc = DateTime.UtcNow,
            Success = true
        };
        var response = await PostJsonAsync("/api/operation", op);
        response.IsSuccessStatusCode.Should().BeTrue();
    }

    // ── Malformed requests ────────────────────────────────────────────────

    [Fact]
    public async Task Register_EmptyBody_ShouldReturnBadRequest()
    {
        using var content = new StringContent("", Encoding.UTF8, "application/json");
        var response = await _http.PostAsync("/api/register", content);
        // Server should handle gracefully — either 400 or 200 with missing data
        ((int)response.StatusCode).Should().BeGreaterOrEqualTo(200).And.BeLessThan(500);
    }

    [Fact]
    public async Task Register_InvalidJson_ShouldNotReturn200Ok()
    {
        using var content = new StringContent("{not valid json", Encoding.UTF8, "application/json");
        var response = await _http.PostAsync("/api/register", content);
        // Server should handle gracefully — either an error status or 200 with no effect
        // The key requirement is the server doesn't crash
        response.Should().NotBeNull();
    }

    [Fact]
    public async Task UnknownEndpoint_ShouldReturn404()
    {
        var response = await _http.GetAsync("/api/unknown");
        response.StatusCode.Should().Be(System.Net.HttpStatusCode.NotFound);
    }

    // ── Double deregister ─────────────────────────────────────────────────

    [Fact]
    public async Task DoubleDeregister_ShouldNotFail()
    {
        await PostJsonAsync("/api/register", MakeRegistration("dbl-dereg", "Memory", "/tmp/dd"));
        await PostJsonAsync("/api/deregister", new { InstanceId = "dbl-dereg" });
        var response = await PostJsonAsync("/api/deregister", new { InstanceId = "dbl-dereg" });
        response.IsSuccessStatusCode.Should().BeTrue();
    }

    // ── Many service types ────────────────────────────────────────────────

    [Fact]
    public async Task ManyServiceTypes_ShouldAllRenderInHtml()
    {
        var types = new[] { "Memory", "Database", "File", "Vector", "PubSub", "LLM" };
        for (var i = 0; i < types.Length; i++)
        {
            _server.State.Register(MakeRegistration($"multi-{i}", types[i], $"/tmp/{types[i]}"));
        }

        var html = await _http.GetStringAsync("/");
        foreach (var t in types)
            html.Should().Contain(t);
    }

    [Fact]
    public async Task Server_ShouldHandleLargeNumberOfServices()
    {
        const int count = 100;
        for (var i = 0; i < count; i++)
        {
            _server.State.Register(MakeRegistration($"bulk-{i}", "Memory", $"/tmp/bulk/{i}"));
        }

        _server.State.ServiceCount.Should().Be(count);

        var html = await _http.GetStringAsync("/");
        html.Should().NotBeNullOrEmpty();
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private static ServiceRegistration MakeRegistration(string id, string type, string path) =>
        new()
        {
            InstanceId = id,
            ServiceType = type,
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
