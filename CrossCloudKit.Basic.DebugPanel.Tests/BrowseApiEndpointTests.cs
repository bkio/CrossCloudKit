// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;
using System.Text;
using FluentAssertions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CrossCloudKit.Basic.DebugPanel.Tests;

/// <summary>
/// Tests for the /api/browse/* HTTP endpoints on the debug panel server.
/// Covers containers, items, detail, browsable check, error handling,
/// missing providers, invalid parameters, and provider exceptions.
/// </summary>
public class BrowseApiEndpointTests : IAsyncLifetime
{
    private DebugPanelServer _server = null!;
    private HttpClient _http = null!;
    private int _port;

    public async Task InitializeAsync()
    {
        _port = TestHelpers.GetAvailablePort();
        _server = new DebugPanelServer();
        await _server.StartAsync(_port);
        _http = new HttpClient
        {
            BaseAddress = new Uri($"http://localhost:{_port}"),
            Timeout = TimeSpan.FromSeconds(10)
        };
    }

    public async Task DisposeAsync()
    {
        _http.Dispose();
        await _server.DisposeAsync();
    }

    // ── /api/browse/{instanceId}/browsable ────────────────────────────────

    [Fact]
    public async Task Browsable_WithProvider_ReturnsTrue()
    {
        var instanceId = RegisterService("Memory", "/tmp/b1");
        _server.State.SetDataProvider(instanceId, new FakeDataProvider());

        var response = await _http.GetStringAsync($"/api/browse/{instanceId}/browsable");
        var result = JObject.Parse(response);
        result["Browsable"]!.Value<bool>().Should().BeTrue();
    }

    [Fact]
    public async Task Browsable_WithoutProvider_ReturnsFalse()
    {
        var instanceId = RegisterService("PubSub", "/tmp/b2");

        var response = await _http.GetStringAsync($"/api/browse/{instanceId}/browsable");
        var result = JObject.Parse(response);
        result["Browsable"]!.Value<bool>().Should().BeFalse();
    }

    [Fact]
    public async Task Browsable_NonExistentInstance_ReturnsFalse()
    {
        var response = await _http.GetStringAsync("/api/browse/nonexistent/browsable");
        var result = JObject.Parse(response);
        result["Browsable"]!.Value<bool>().Should().BeFalse();
    }

    // ── /api/browse/{instanceId}/containers ───────────────────────────────

    [Fact]
    public async Task Containers_WithProvider_ReturnsContainerList()
    {
        var instanceId = RegisterService("Database", "/tmp/c1");
        _server.State.SetDataProvider(instanceId, new FakeDataProvider(
            containers: new List<DebugContainer>
            {
                new() { Name = "Users", ItemCount = 10, Properties = new() { ["Engine"] = "Basic" } },
                new() { Name = "Orders", ItemCount = 5 }
            }));

        var response = await _http.GetStringAsync($"/api/browse/{instanceId}/containers");
        var containers = JsonConvert.DeserializeObject<List<DebugContainer>>(response);
        containers.Should().HaveCount(2);
        containers![0].Name.Should().Be("Users");
        containers[0].ItemCount.Should().Be(10);
        containers[0].Properties.Should().ContainKey("Engine");
        containers[1].Name.Should().Be("Orders");
    }

    [Fact]
    public async Task Containers_EmptyList_ReturnsEmptyArray()
    {
        var instanceId = RegisterService("Database", "/tmp/c2");
        _server.State.SetDataProvider(instanceId, new FakeDataProvider(
            containers: new List<DebugContainer>()));

        var response = await _http.GetStringAsync($"/api/browse/{instanceId}/containers");
        var containers = JsonConvert.DeserializeObject<List<DebugContainer>>(response);
        containers.Should().BeEmpty();
    }

    [Fact]
    public async Task Containers_NoProvider_Returns404()
    {
        var instanceId = RegisterService("PubSub", "/tmp/c3");

        var response = await _http.GetAsync($"/api/browse/{instanceId}/containers");
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Containers_ProviderThrows_Returns500()
    {
        var instanceId = RegisterService("Database", "/tmp/c4");
        _server.State.SetDataProvider(instanceId, new ThrowingDataProvider(
            new IOException("Disk failure")));

        var response = await _http.GetAsync($"/api/browse/{instanceId}/containers");
        response.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
        var body = await response.Content.ReadAsStringAsync();
        body.Should().Contain("Disk failure");
    }

    // ── /api/browse/{instanceId}/items ────────────────────────────────────

    [Fact]
    public async Task Items_ValidContainer_ReturnsItemList()
    {
        var instanceId = RegisterService("Database", "/tmp/i1");
        _server.State.SetDataProvider(instanceId, new FakeDataProvider(
            items: new Dictionary<string, List<DebugItem>>
            {
                ["Users"] = new()
                {
                    new() { Id = "u1", Label = "Alice", Properties = new() { ["Age"] = "30" } },
                    new() { Id = "u2", Label = "Bob", HasDetail = false }
                }
            }));

        var response = await _http.GetStringAsync($"/api/browse/{instanceId}/items?container=Users");
        var items = JsonConvert.DeserializeObject<List<DebugItem>>(response);
        items.Should().HaveCount(2);
        items![0].Id.Should().Be("u1");
        items[0].Label.Should().Be("Alice");
        items[1].HasDetail.Should().BeFalse();
    }

    [Fact]
    public async Task Items_MissingContainerParam_Returns400()
    {
        var instanceId = RegisterService("Database", "/tmp/i2");
        _server.State.SetDataProvider(instanceId, new FakeDataProvider());

        var response = await _http.GetAsync($"/api/browse/{instanceId}/items");
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Items_EmptyContainerParam_Returns400()
    {
        var instanceId = RegisterService("Database", "/tmp/i3");
        _server.State.SetDataProvider(instanceId, new FakeDataProvider());

        var response = await _http.GetAsync($"/api/browse/{instanceId}/items?container=");
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Items_NoProvider_Returns404()
    {
        var instanceId = RegisterService("PubSub", "/tmp/i4");

        var response = await _http.GetAsync($"/api/browse/{instanceId}/items?container=X");
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Items_MaxParam_IsRespected()
    {
        var fakeItems = Enumerable.Range(0, 100).Select(i =>
            new DebugItem { Id = $"item-{i}" }).ToList();
        var instanceId = RegisterService("Database", "/tmp/i5");
        _server.State.SetDataProvider(instanceId, new FakeDataProvider(
            items: new() { ["T"] = fakeItems }));

        // The FakeDataProvider respects maxItems by returning up to maxItems
        var response = await _http.GetStringAsync($"/api/browse/{instanceId}/items?container=T&max=5");
        var items = JsonConvert.DeserializeObject<List<DebugItem>>(response);
        items.Should().HaveCount(5);
    }

    [Fact]
    public async Task Items_MaxParamCappedAt1000()
    {
        var fakeItems = Enumerable.Range(0, 1500).Select(i =>
            new DebugItem { Id = $"item-{i}" }).ToList();
        var instanceId = RegisterService("Database", "/tmp/i6");
        _server.State.SetDataProvider(instanceId, new FakeDataProvider(
            items: new() { ["T"] = fakeItems }));

        var response = await _http.GetStringAsync($"/api/browse/{instanceId}/items?container=T&max=9999");
        var items = JsonConvert.DeserializeObject<List<DebugItem>>(response);
        items.Should().HaveCount(1000);
    }

    [Fact]
    public async Task Items_InvalidMaxParam_UsesDefault200()
    {
        var fakeItems = Enumerable.Range(0, 300).Select(i =>
            new DebugItem { Id = $"item-{i}" }).ToList();
        var instanceId = RegisterService("Database", "/tmp/i7");
        _server.State.SetDataProvider(instanceId, new FakeDataProvider(
            items: new() { ["T"] = fakeItems }));

        var response = await _http.GetStringAsync($"/api/browse/{instanceId}/items?container=T&max=notanumber");
        var items = JsonConvert.DeserializeObject<List<DebugItem>>(response);
        items.Should().HaveCount(200);
    }

    [Fact]
    public async Task Items_ProviderThrows_Returns500()
    {
        var instanceId = RegisterService("Database", "/tmp/i8");
        _server.State.SetDataProvider(instanceId, new ThrowingDataProvider(
            new InvalidOperationException("table locked")));

        var response = await _http.GetAsync($"/api/browse/{instanceId}/items?container=X");
        response.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
    }

    [Fact]
    public async Task Items_UrlEncodedContainerName_IsDecoded()
    {
        var instanceId = RegisterService("Database", "/tmp/i9");
        _server.State.SetDataProvider(instanceId, new FakeDataProvider(
            items: new()
            {
                ["my table/with spaces"] = new()
                {
                    new() { Id = "i1", Label = "ok" }
                }
            }));

        var response = await _http.GetStringAsync(
            $"/api/browse/{instanceId}/items?container={Uri.EscapeDataString("my table/with spaces")}");
        var items = JsonConvert.DeserializeObject<List<DebugItem>>(response);
        items.Should().ContainSingle();
    }

    // ── /api/browse/{instanceId}/detail ───────────────────────────────────

    [Fact]
    public async Task Detail_ExistingItem_ReturnsDetail()
    {
        var instanceId = RegisterService("Database", "/tmp/d1");
        _server.State.SetDataProvider(instanceId, new FakeDataProvider(
            details: new()
            {
                [("Users", "u1")] = new DebugItemDetail
                {
                    Id = "u1",
                    ContentJson = "{ \"Name\": \"Alice\" }",
                    Summary = "User record"
                }
            }));

        var response = await _http.GetStringAsync(
            $"/api/browse/{instanceId}/detail?container=Users&id=u1");
        var detail = JsonConvert.DeserializeObject<DebugItemDetail>(response);
        detail!.Id.Should().Be("u1");
        detail.ContentJson.Should().Contain("Alice");
        detail.Summary.Should().Be("User record");
    }

    [Fact]
    public async Task Detail_NonExistentItem_Returns404()
    {
        var instanceId = RegisterService("Database", "/tmp/d2");
        _server.State.SetDataProvider(instanceId, new FakeDataProvider());

        var response = await _http.GetAsync(
            $"/api/browse/{instanceId}/detail?container=T&id=missing");
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Detail_MissingContainerParam_Returns400()
    {
        var instanceId = RegisterService("Database", "/tmp/d3");
        _server.State.SetDataProvider(instanceId, new FakeDataProvider());

        var response = await _http.GetAsync(
            $"/api/browse/{instanceId}/detail?id=u1");
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Detail_MissingIdParam_Returns400()
    {
        var instanceId = RegisterService("Database", "/tmp/d4");
        _server.State.SetDataProvider(instanceId, new FakeDataProvider());

        var response = await _http.GetAsync(
            $"/api/browse/{instanceId}/detail?container=T");
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Detail_NoProvider_Returns404()
    {
        var instanceId = RegisterService("PubSub", "/tmp/d5");

        var response = await _http.GetAsync(
            $"/api/browse/{instanceId}/detail?container=T&id=i1");
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Detail_ProviderThrows_Returns500()
    {
        var instanceId = RegisterService("Database", "/tmp/d6");
        _server.State.SetDataProvider(instanceId, new ThrowingDataProvider(
            new UnauthorizedAccessException("access denied")));

        var response = await _http.GetAsync(
            $"/api/browse/{instanceId}/detail?container=T&id=i1");
        response.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
        var body = await response.Content.ReadAsStringAsync();
        body.Should().Contain("access denied");
    }

    // ── Cross-endpoint consistency ────────────────────────────────────────

    [Fact]
    public async Task BrowseAfterDeregister_Returns404()
    {
        var instanceId = RegisterService("Memory", "/tmp/dr1");
        _server.State.SetDataProvider(instanceId, new FakeDataProvider(
            containers: new List<DebugContainer> { new() { Name = "T" } }));

        // Verify it works
        var r1 = await _http.GetAsync($"/api/browse/{instanceId}/containers");
        r1.StatusCode.Should().Be(HttpStatusCode.OK);

        // Deregister
        _server.State.Deregister(instanceId);

        // Provider should be removed too
        var r2 = await _http.GetAsync($"/api/browse/{instanceId}/containers");
        r2.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task MultipleProviders_IsolatedByInstanceId()
    {
        var id1 = RegisterService("Database", "/tmp/iso1");
        var id2 = RegisterService("Database", "/tmp/iso2");

        _server.State.SetDataProvider(id1, new FakeDataProvider(
            containers: new List<DebugContainer> { new() { Name = "TableA" } }));
        _server.State.SetDataProvider(id2, new FakeDataProvider(
            containers: new List<DebugContainer> { new() { Name = "TableB" } }));

        var r1 = await _http.GetStringAsync($"/api/browse/{id1}/containers");
        var c1 = JsonConvert.DeserializeObject<List<DebugContainer>>(r1);
        c1![0].Name.Should().Be("TableA");

        var r2 = await _http.GetStringAsync($"/api/browse/{id2}/containers");
        var c2 = JsonConvert.DeserializeObject<List<DebugContainer>>(r2);
        c2![0].Name.Should().Be("TableB");
    }

    // ── Concurrent browse requests ────────────────────────────────────────

    [Fact]
    public async Task ConcurrentBrowseRequests_DoNotCrash()
    {
        var instanceId = RegisterService("Database", "/tmp/conc1");
        _server.State.SetDataProvider(instanceId, new FakeDataProvider(
            containers: new List<DebugContainer>
            {
                new() { Name = "T1", ItemCount = 5 },
                new() { Name = "T2", ItemCount = 3 }
            },
            items: new()
            {
                ["T1"] = Enumerable.Range(0, 5).Select(i => new DebugItem { Id = $"i{i}" }).ToList()
            }));

        var tasks = Enumerable.Range(0, 30).Select(i =>
        {
            return (i % 3) switch
            {
                0 => _http.GetAsync($"/api/browse/{instanceId}/containers"),
                1 => _http.GetAsync($"/api/browse/{instanceId}/items?container=T1"),
                _ => _http.GetAsync($"/api/browse/{instanceId}/browsable")
            };
        }).ToArray();

        var responses = await Task.WhenAll(tasks);
        responses.Should().AllSatisfy(r => r.IsSuccessStatusCode.Should().BeTrue());
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private string RegisterService(string type, string path)
    {
        var instanceId = Guid.NewGuid().ToString("N");
        _server.State.Register(new ServiceRegistration
        {
            InstanceId = instanceId,
            ServiceType = type,
            Path = path,
            ProcessId = Environment.ProcessId,
            MachineName = Environment.MachineName,
            StartedAtUtc = DateTime.UtcNow
        });
        return instanceId;
    }

    /// <summary>Configurable fake data provider for endpoint tests.</summary>
    private sealed class FakeDataProvider : IDebugDataProvider
    {
        private readonly List<DebugContainer> _containers;
        private readonly Dictionary<string, List<DebugItem>> _items;
        private readonly Dictionary<(string, string), DebugItemDetail> _details;

        public FakeDataProvider(
            List<DebugContainer>? containers = null,
            Dictionary<string, List<DebugItem>>? items = null,
            Dictionary<(string, string), DebugItemDetail>? details = null)
        {
            _containers = containers ?? new();
            _items = items ?? new();
            _details = details ?? new();
        }

        public Task<List<DebugContainer>> ListContainersAsync() =>
            Task.FromResult(_containers);

        public Task<List<DebugItem>> ListItemsAsync(string container, int maxItems = 200)
        {
            if (_items.TryGetValue(container, out var items))
                return Task.FromResult(items.Take(maxItems).ToList());
            return Task.FromResult(new List<DebugItem>());
        }

        public Task<DebugItemDetail?> GetItemDetailAsync(string container, string itemId)
        {
            _details.TryGetValue((container, itemId), out var detail);
            return Task.FromResult(detail);
        }
    }

    /// <summary>Data provider that always throws, for error handling tests.</summary>
    private sealed class ThrowingDataProvider : IDebugDataProvider
    {
        private readonly Exception _exception;

        public ThrowingDataProvider(Exception exception)
        {
            _exception = exception;
        }

        public Task<List<DebugContainer>> ListContainersAsync() =>
            throw _exception;

        public Task<List<DebugItem>> ListItemsAsync(string container, int maxItems = 200) =>
            throw _exception;

        public Task<DebugItemDetail?> GetItemDetailAsync(string container, string itemId) =>
            throw _exception;
    }
}
