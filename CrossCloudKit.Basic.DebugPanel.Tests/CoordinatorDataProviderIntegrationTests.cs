// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Text;
using CrossCloudKit.Database.Basic;
using CrossCloudKit.File.Basic;
using CrossCloudKit.Memory.Basic;
using CrossCloudKit.Utilities.Common;
using CrossCloudKit.Vector.Basic;
using FluentAssertions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CrossCloudKit.Basic.DebugPanel.Tests;

/// <summary>
/// Integration tests that verify the full flow: Basic service creates provider,
/// coordinator registers it, browse APIs serve data from real file-system state.
/// Also tests coordinator-level provider passing and lifecycle.
/// </summary>
[Collection("Coordinator")]
public class CoordinatorDataProviderIntegrationTests : IAsyncLifetime
{
    private int _port;
    private HttpClient _http = null!;
    private readonly List<string> _tempDirs = new();

    public async Task InitializeAsync()
    {
        _port = TestHelpers.GetAvailablePort();
        Environment.SetEnvironmentVariable("CROSSCLOUDKIT_DEBUG_PANEL_DISABLED", null);
        _http = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
    }

    public async Task DisposeAsync()
    {
        _http.Dispose();
        await DebugPanelCoordinator.ResetAsync();
        Environment.SetEnvironmentVariable("CROSSCLOUDKIT_DEBUG_PANEL_DISABLED", "true");
        foreach (var dir in _tempDirs)
            TestHelpers.CleanupDir(dir);
    }

    private string CreateTempDir()
    {
        var dir = TestHelpers.CreateTempDir();
        _tempDirs.Add(dir);
        return dir;
    }

    // ── Coordinator passes provider to server state ───────────────────────

    [Fact]
    public async Task RegisterWithProvider_ProviderStoredOnServer()
    {
        var provider = new StubProvider();
        var tracker = await DebugPanelCoordinator.RegisterAsync(
            "Memory", "/tmp/cprov1", provider, port: _port);

        tracker.Should().NotBeNull();
        DebugPanelCoordinator.Server.Should().NotBeNull();

        var instanceId = DebugPanelCoordinator.Server!.State.Services.Keys.First();
        DebugPanelCoordinator.Server.State.HasDataProvider(instanceId).Should().BeTrue();
    }

    [Fact]
    public async Task RegisterWithoutProvider_NoProviderOnServer()
    {
        var tracker = await DebugPanelCoordinator.RegisterAsync(
            "PubSub", "/tmp/cprov2", port: _port);

        tracker.Should().NotBeNull();
        var instanceId = DebugPanelCoordinator.Server!.State.Services.Keys.First();
        DebugPanelCoordinator.Server.State.HasDataProvider(instanceId).Should().BeFalse();
    }

    [Fact]
    public async Task RegisterMultiple_MixedProviders_EachTracked()
    {
        var p1 = new StubProvider();
        var t1 = await DebugPanelCoordinator.RegisterAsync(
            "Database", "/tmp/cprov3a", p1, port: _port);

        var t2 = await DebugPanelCoordinator.RegisterAsync(
            "PubSub", "/tmp/cprov3b", port: _port);

        var p3 = new StubProvider();
        var t3 = await DebugPanelCoordinator.RegisterAsync(
            "Memory", "/tmp/cprov3c", p3, port: _port);

        DebugPanelCoordinator.Server!.State.ServiceCount.Should().Be(3);

        var services = DebugPanelCoordinator.Server.State.Services.Values.ToList();
        var dbSvc = services.First(s => s.ServiceType == "Database");
        var psSvc = services.First(s => s.ServiceType == "PubSub");
        var memSvc = services.First(s => s.ServiceType == "Memory");

        DebugPanelCoordinator.Server.State.HasDataProvider(dbSvc.InstanceId).Should().BeTrue();
        DebugPanelCoordinator.Server.State.HasDataProvider(psSvc.InstanceId).Should().BeFalse();
        DebugPanelCoordinator.Server.State.HasDataProvider(memSvc.InstanceId).Should().BeTrue();
    }

    [Fact]
    public async Task DeregisterService_AlsoRemovesProvider()
    {
        var provider = new StubProvider();
        await DebugPanelCoordinator.RegisterAsync(
            "Memory", "/tmp/cprov4", provider, port: _port);

        var instanceId = DebugPanelCoordinator.Server!.State.Services.Keys.First();
        DebugPanelCoordinator.Server.State.HasDataProvider(instanceId).Should().BeTrue();

        await DebugPanelCoordinator.DeregisterAsync(instanceId);
        DebugPanelCoordinator.Server.State.HasDataProvider(instanceId).Should().BeFalse();
    }

    // ── Full end-to-end with real database provider ───────────────────────

    [Fact]
    public async Task EndToEnd_DatabaseProvider_BrowseViaHttp()
    {
        var dbPath = CreateTempDir();
        var tablePath = Path.Combine(dbPath, "Users");
        Directory.CreateDirectory(tablePath);
        System.IO.File.WriteAllText(Path.Combine(tablePath, "alice.json"),
            new JObject { ["Name"] = "Alice", ["Age"] = 30 }.ToString());
        System.IO.File.WriteAllText(Path.Combine(tablePath, "bob.json"),
            new JObject { ["Name"] = "Bob", ["Age"] = 25 }.ToString());

        var provider = new DatabaseDebugDataProvider(dbPath);
        await DebugPanelCoordinator.RegisterAsync("Database", dbPath, provider, port: _port);

        _http.BaseAddress = new Uri($"http://localhost:{_port}");
        var instanceId = DebugPanelCoordinator.Server!.State.Services.Keys.First();

        // 1. Check browsable
        var browsable = await _http.GetStringAsync($"/api/browse/{instanceId}/browsable");
        browsable.Should().Contain("true");

        // 2. List containers
        var containersJson = await _http.GetStringAsync($"/api/browse/{instanceId}/containers");
        var containers = JsonConvert.DeserializeObject<List<DebugContainer>>(containersJson);
        containers.Should().ContainSingle();
        containers![0].Name.Should().Be("Users");
        containers[0].ItemCount.Should().Be(2);

        // 3. List items
        var itemsJson = await _http.GetStringAsync($"/api/browse/{instanceId}/items?container=Users");
        var items = JsonConvert.DeserializeObject<List<DebugItem>>(itemsJson);
        items.Should().HaveCount(2);
        items!.Should().Contain(i => i.Id == "alice");
        items.Should().Contain(i => i.Id == "bob");

        // 4. Get detail
        var detailJson = await _http.GetStringAsync($"/api/browse/{instanceId}/detail?container=Users&id=alice");
        var detail = JsonConvert.DeserializeObject<DebugItemDetail>(detailJson);
        detail!.Id.Should().Be("alice");
        detail.ContentJson.Should().Contain("Alice");
    }

    // ── Full end-to-end with real vector provider ─────────────────────────

    [Fact]
    public async Task EndToEnd_VectorProvider_BrowseViaHttp()
    {
        var storePath = CreateTempDir();
        var collDir = Path.Combine(storePath, "docs");
        Directory.CreateDirectory(collDir);
        System.IO.File.WriteAllText(Path.Combine(collDir, "_meta.json"),
            new JObject { ["VectorDimensions"] = 4, ["DistanceMetric"] = "Cosine" }.ToString());

        var point = new { Id = "vec-1", Vector = new float[] { 0.1f, 0.2f, 0.3f, 0.4f },
            Metadata = new JObject { ["title"] = "Test" } };
        var encoded = EncodingUtilities.Base64EncodeNoPadding("vec-1");
        System.IO.File.WriteAllText(Path.Combine(collDir, $"{encoded}.json"),
            JsonConvert.SerializeObject(point));

        var provider = new VectorDebugDataProvider(storePath);
        await DebugPanelCoordinator.RegisterAsync("Vector", storePath, provider, port: _port);

        _http.BaseAddress = new Uri($"http://localhost:{_port}");
        var instanceId = DebugPanelCoordinator.Server!.State.Services.Keys.First();

        // Containers
        var containersJson = await _http.GetStringAsync($"/api/browse/{instanceId}/containers");
        var containers = JsonConvert.DeserializeObject<List<DebugContainer>>(containersJson);
        containers![0].Properties!["Dimensions"].Should().Be("4");

        // Items
        var itemsJson = await _http.GetStringAsync($"/api/browse/{instanceId}/items?container=docs");
        var items = JsonConvert.DeserializeObject<List<DebugItem>>(itemsJson);
        items![0].Id.Should().Be("vec-1");

        // Detail
        var detailJson = await _http.GetStringAsync($"/api/browse/{instanceId}/detail?container=docs&id=vec-1");
        var detail = JsonConvert.DeserializeObject<DebugItemDetail>(detailJson);
        detail!.ContentJson.Should().Contain("Test");
    }

    // ── Full end-to-end with real memory provider ─────────────────────────

    [Fact]
    public async Task EndToEnd_MemoryProvider_BrowseViaHttp()
    {
        var storePath = CreateTempDir();
        var scopeData = new
        {
            KeyValues = new Dictionary<string, Primitive>
            {
                ["Name"] = new Primitive("Alice"),
                ["Count"] = new Primitive(42L)
            },
            Lists = new Dictionary<string, List<Primitive>>
            {
                ["tags"] = new() { new Primitive("admin") }
            },
            ExpiryTime = (DateTime?)null
        };
        var scopeFileName = EncodingUtilities.Base64EncodeNoPadding("session:abc");
        System.IO.File.WriteAllText(Path.Combine(storePath, $"{scopeFileName}.json"),
            JsonConvert.SerializeObject(scopeData), Encoding.UTF8);

        var provider = new MemoryDebugDataProvider(storePath);
        await DebugPanelCoordinator.RegisterAsync("Memory", storePath, provider, port: _port);

        _http.BaseAddress = new Uri($"http://localhost:{_port}");
        var instanceId = DebugPanelCoordinator.Server!.State.Services.Keys.First();

        // Containers — should decode scope name
        var containersJson = await _http.GetStringAsync($"/api/browse/{instanceId}/containers");
        var containers = JsonConvert.DeserializeObject<List<DebugContainer>>(containersJson);
        containers![0].Name.Should().Be("session:abc");
        containers[0].ItemCount.Should().Be(3); // 2 keys + 1 list

        // Items
        var itemsJson = await _http.GetStringAsync(
            $"/api/browse/{instanceId}/items?container={Uri.EscapeDataString("session:abc")}");
        var items = JsonConvert.DeserializeObject<List<DebugItem>>(itemsJson);
        items.Should().HaveCount(3);
        items!.Should().Contain(i => i.Id == "kv:Name");
        items.Should().Contain(i => i.Id == "kv:Count");
        items.Should().Contain(i => i.Id == "list:tags");

        // Detail for key
        var kvDetailJson = await _http.GetStringAsync(
            $"/api/browse/{instanceId}/detail?container={Uri.EscapeDataString("session:abc")}&id=kv:Name");
        var kvDetail = JsonConvert.DeserializeObject<DebugItemDetail>(kvDetailJson);
        kvDetail!.ContentJson.Should().Contain("Alice");
    }

    // ── Full end-to-end with real file provider ───────────────────────────

    [Fact]
    public async Task EndToEnd_FileProvider_BrowseViaHttp()
    {
        var basePath = CreateTempDir();
        var bucketDir = Path.Combine(basePath, "uploads");
        Directory.CreateDirectory(bucketDir);
        System.IO.File.WriteAllText(Path.Combine(bucketDir, "readme.txt"), "Hello world");
        var subDir = Path.Combine(bucketDir, "docs");
        Directory.CreateDirectory(subDir);
        System.IO.File.WriteAllText(Path.Combine(subDir, "spec.pdf"), "pdf content");

        var provider = new FileDebugDataProvider(basePath);
        await DebugPanelCoordinator.RegisterAsync("File", basePath, provider, port: _port);

        _http.BaseAddress = new Uri($"http://localhost:{_port}");
        var instanceId = DebugPanelCoordinator.Server!.State.Services.Keys.First();

        // Containers
        var containersJson = await _http.GetStringAsync($"/api/browse/{instanceId}/containers");
        var containers = JsonConvert.DeserializeObject<List<DebugContainer>>(containersJson);
        containers![0].Name.Should().Be("uploads");
        containers[0].ItemCount.Should().Be(2);

        // Items
        var itemsJson = await _http.GetStringAsync($"/api/browse/{instanceId}/items?container=uploads");
        var items = JsonConvert.DeserializeObject<List<DebugItem>>(itemsJson);
        items.Should().HaveCount(2);
        items!.Should().Contain(i => i.Id == "readme.txt");
        items.Should().Contain(i => i.Id == "docs/spec.pdf");

        // HasDetail should be false for files
        items.Should().AllSatisfy(i => i.HasDetail.Should().BeFalse());
    }

    // ── Dashboard HTML shows Browse button for browsable providers ─────────

    [Fact]
    public async Task DashboardHtml_ShowsBrowseButtonForRegisteredProvider()
    {
        var dbPath = CreateTempDir();
        Directory.CreateDirectory(Path.Combine(dbPath, "T1"));

        var provider = new DatabaseDebugDataProvider(dbPath);
        await DebugPanelCoordinator.RegisterAsync("Database", dbPath, provider, port: _port);

        _http.BaseAddress = new Uri($"http://localhost:{_port}");
        var html = await _http.GetStringAsync("/");

        html.Should().Contain("Browse Data");
        html.Should().Contain("btn-browse");
    }

    // ── Stub ──────────────────────────────────────────────────────────────

    private sealed class StubProvider : IDebugDataProvider
    {
        public Task<List<DebugContainer>> ListContainersAsync() =>
            Task.FromResult(new List<DebugContainer>());
        public Task<List<DebugItem>> ListItemsAsync(string container, int maxItems = 200) =>
            Task.FromResult(new List<DebugItem>());
        public Task<DebugItemDetail?> GetItemDetailAsync(string container, string itemId) =>
            Task.FromResult<DebugItemDetail?>(null);
    }
}
