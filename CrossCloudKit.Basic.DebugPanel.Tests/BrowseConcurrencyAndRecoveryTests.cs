// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;
using System.Text;
using CrossCloudKit.Database.Basic;
using CrossCloudKit.File.Basic;
using CrossCloudKit.Memory.Basic;
using CrossCloudKit.Vector.Basic;
using FluentAssertions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CrossCloudKit.Basic.DebugPanel.Tests;

/// <summary>
/// Concurrency, crash recovery, and stress tests for the browse data provider system.
/// Tests race conditions, file-system mutations during browsing, provider failures
/// mid-request, rapid provider replacement, and server restart recovery.
/// </summary>
public class BrowseConcurrencyAndRecoveryTests : IAsyncLifetime
{
    private DebugPanelServer _server = null!;
    private HttpClient _http = null!;
    private int _port;
    private readonly List<string> _tempDirs = new();

    public async Task InitializeAsync()
    {
        _port = TestHelpers.GetAvailablePort();
        _server = new DebugPanelServer();
        await _server.StartAsync(_port);
        _http = new HttpClient
        {
            BaseAddress = new Uri($"http://localhost:{_port}"),
            Timeout = TimeSpan.FromSeconds(15)
        };
    }

    public async Task DisposeAsync()
    {
        _http.Dispose();
        await _server.DisposeAsync();
        foreach (var dir in _tempDirs)
            TestHelpers.CleanupDir(dir);
    }

    private string CreateTempDir()
    {
        var dir = TestHelpers.CreateTempDir();
        _tempDirs.Add(dir);
        return dir;
    }

    // ── Concurrent browse requests across multiple providers ──────────────

    [Fact]
    public async Task ConcurrentBrowse_MultipleProviders_AllSucceed()
    {
        var ids = new string[5];
        for (var i = 0; i < 5; i++)
        {
            ids[i] = RegisterService($"Type{i}", $"/tmp/multi-{i}");
            _server.State.SetDataProvider(ids[i], new SlowProvider(delay: TimeSpan.FromMilliseconds(50),
                containers: new List<DebugContainer> { new() { Name = $"Container{i}", ItemCount = i } }));
        }

        var tasks = ids.SelectMany(id => Enumerable.Range(0, 10).Select(_ =>
            _http.GetAsync($"/api/browse/{id}/containers"))).ToArray();

        var responses = await Task.WhenAll(tasks);
        responses.Should().AllSatisfy(r => r.StatusCode.Should().Be(HttpStatusCode.OK));
    }

    // ── Provider replaced mid-request ─────────────────────────────────────

    [Fact]
    public async Task ProviderReplacedDuringRequest_NewProviderUsedForNextRequest()
    {
        var instanceId = RegisterService("Database", "/tmp/replace1");

        var oldProvider = new ConfigurableProvider(containers: new List<DebugContainer>
            { new() { Name = "OldTable" } });
        _server.State.SetDataProvider(instanceId, oldProvider);

        var r1 = await _http.GetStringAsync($"/api/browse/{instanceId}/containers");
        r1.Should().Contain("OldTable");

        // Replace provider
        var newProvider = new ConfigurableProvider(containers: new List<DebugContainer>
            { new() { Name = "NewTable" } });
        _server.State.SetDataProvider(instanceId, newProvider);

        var r2 = await _http.GetStringAsync($"/api/browse/{instanceId}/containers");
        r2.Should().Contain("NewTable");
        r2.Should().NotContain("OldTable");
    }

    // ── Provider throws intermittently ────────────────────────────────────

    [Fact]
    public async Task IntermittentProviderFailure_DoesNotAffectOtherRequests()
    {
        var instanceId = RegisterService("Database", "/tmp/flaky");
        var provider = new FlakyProvider(failEveryN: 3);
        _server.State.SetDataProvider(instanceId, provider);

        var results = new List<HttpStatusCode>();
        for (var i = 0; i < 12; i++)
        {
            var r = await _http.GetAsync($"/api/browse/{instanceId}/containers");
            results.Add(r.StatusCode);
        }

        // Every 3rd should fail, others succeed
        results.Count(r => r == HttpStatusCode.OK).Should().BeGreaterThan(0);
        results.Count(r => r == HttpStatusCode.InternalServerError).Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task ProviderFailure_DoesNotCrashServer()
    {
        var instanceId = RegisterService("Database", "/tmp/crash1");
        _server.State.SetDataProvider(instanceId, new AlwaysThrowingProvider());

        // Browse should fail gracefully
        var r = await _http.GetAsync($"/api/browse/{instanceId}/containers");
        r.StatusCode.Should().Be(HttpStatusCode.InternalServerError);

        // Server should still be healthy
        _server.IsRunning.Should().BeTrue();
        var health = await _http.GetAsync("/api/services");
        health.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    // ── File system changes during browsing ───────────────────────────────

    [Fact]
    public async Task DatabaseProvider_FilesAddedDuringBrowsing_ReflectedOnNextCall()
    {
        var dbPath = CreateTempDir();
        var tablePath = Path.Combine(dbPath, "Users");
        Directory.CreateDirectory(tablePath);
        System.IO.File.WriteAllText(Path.Combine(tablePath, "u1.json"), "{\"Name\":\"Alice\"}");

        var provider = new DatabaseDebugDataProvider(dbPath);
        var instanceId = RegisterService("Database", dbPath);
        _server.State.SetDataProvider(instanceId, provider);

        var r1 = await _http.GetStringAsync($"/api/browse/{instanceId}/items?container=Users");
        var items1 = JsonConvert.DeserializeObject<List<DebugItem>>(r1);
        items1.Should().ContainSingle();

        // Add a new item
        System.IO.File.WriteAllText(Path.Combine(tablePath, "u2.json"), "{\"Name\":\"Bob\"}");

        var r2 = await _http.GetStringAsync($"/api/browse/{instanceId}/items?container=Users");
        var items2 = JsonConvert.DeserializeObject<List<DebugItem>>(r2);
        items2.Should().HaveCount(2);
    }

    [Fact]
    public async Task DatabaseProvider_FileDeletedDuringBrowsing_ReflectedOnNextCall()
    {
        var dbPath = CreateTempDir();
        var tablePath = Path.Combine(dbPath, "Users");
        Directory.CreateDirectory(tablePath);
        System.IO.File.WriteAllText(Path.Combine(tablePath, "u1.json"), "{\"Name\":\"Alice\"}");
        System.IO.File.WriteAllText(Path.Combine(tablePath, "u2.json"), "{\"Name\":\"Bob\"}");

        var provider = new DatabaseDebugDataProvider(dbPath);
        var instanceId = RegisterService("Database", dbPath);
        _server.State.SetDataProvider(instanceId, provider);

        var r1 = await _http.GetStringAsync($"/api/browse/{instanceId}/items?container=Users");
        var items1 = JsonConvert.DeserializeObject<List<DebugItem>>(r1);
        items1.Should().HaveCount(2);

        // Delete a file
        System.IO.File.Delete(Path.Combine(tablePath, "u1.json"));

        var r2 = await _http.GetStringAsync($"/api/browse/{instanceId}/items?container=Users");
        var items2 = JsonConvert.DeserializeObject<List<DebugItem>>(r2);
        items2.Should().ContainSingle();
    }

    [Fact]
    public async Task DatabaseProvider_TableDeletedDuringBrowsing_ReturnsEmpty()
    {
        var dbPath = CreateTempDir();
        var tablePath = Path.Combine(dbPath, "TempTable");
        Directory.CreateDirectory(tablePath);
        System.IO.File.WriteAllText(Path.Combine(tablePath, "item.json"), "{}");

        var provider = new DatabaseDebugDataProvider(dbPath);
        var instanceId = RegisterService("Database", dbPath);
        _server.State.SetDataProvider(instanceId, provider);

        var r1 = await _http.GetStringAsync($"/api/browse/{instanceId}/containers");
        r1.Should().Contain("TempTable");

        // Delete the table directory
        Directory.Delete(tablePath, true);

        var r2 = await _http.GetStringAsync($"/api/browse/{instanceId}/containers");
        r2.Should().NotContain("TempTable");

        // Listing items should return empty, not crash
        var r3 = await _http.GetStringAsync($"/api/browse/{instanceId}/items?container=TempTable");
        var items = JsonConvert.DeserializeObject<List<DebugItem>>(r3);
        items.Should().BeEmpty();
    }

    // ── Concurrent writes and reads on real providers ─────────────────────

    [Fact]
    public async Task DatabaseProvider_ConcurrentReadsWhileWriting_DoesNotCrash()
    {
        var dbPath = CreateTempDir();
        var tablePath = Path.Combine(dbPath, "Active");
        Directory.CreateDirectory(tablePath);

        var provider = new DatabaseDebugDataProvider(dbPath);
        var instanceId = RegisterService("Database", dbPath);
        _server.State.SetDataProvider(instanceId, provider);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));

        // Writer: keep adding files
        var writer = Task.Run(async () =>
        {
            var i = 0;
            while (!cts.Token.IsCancellationRequested)
            {
                System.IO.File.WriteAllText(Path.Combine(tablePath, $"item_{i++}.json"), $"{{\"I\":{i}}}");
                await Task.Delay(10, CancellationToken.None);
            }
        }, cts.Token);

        // Readers: keep browsing
        var readers = Enumerable.Range(0, 5).Select(_ => Task.Run(async () =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    await _http.GetAsync($"/api/browse/{instanceId}/containers", cts.Token);
                    await _http.GetAsync($"/api/browse/{instanceId}/items?container=Active", cts.Token);
                }
                catch (OperationCanceledException) { break; }
                catch (HttpRequestException) { /* server may be slow */ }
            }
        }, cts.Token)).ToArray();

        try { await Task.WhenAll(readers.Append(writer)); }
        catch (OperationCanceledException) { /* expected */ }

        // Server should still be healthy
        _server.IsRunning.Should().BeTrue();
    }

    // ── Rapid provider registration/deregistration ────────────────────────

    [Fact]
    public async Task RapidRegisterDeregister_DoesNotLoseProviders()
    {
        for (var i = 0; i < 20; i++)
        {
            var id = RegisterService("Memory", $"/tmp/rapid-{i}");
            _server.State.SetDataProvider(id, new ConfigurableProvider(
                containers: new List<DebugContainer> { new() { Name = $"T{i}" } }));

            var r = await _http.GetAsync($"/api/browse/{id}/browsable");
            r.StatusCode.Should().Be(HttpStatusCode.OK);
            var body = await r.Content.ReadAsStringAsync();
            body.Should().Contain("true");

            _server.State.Deregister(id);

            var r2 = await _http.GetAsync($"/api/browse/{id}/browsable");
            var body2 = await r2.Content.ReadAsStringAsync();
            body2.Should().Contain("false");
        }
    }

    // ── Server stop/restart with providers ────────────────────────────────

    [Fact]
    public async Task ServerRestart_ProvidersAreCleared()
    {
        var instanceId = RegisterService("Database", "/tmp/restart1");
        _server.State.SetDataProvider(instanceId, new ConfigurableProvider(
            containers: new List<DebugContainer> { new() { Name = "T1" } }));

        // Stop and restart
        _http.Dispose();
        await _server.DisposeAsync();

        var newPort = TestHelpers.GetAvailablePort();
        _server = new DebugPanelServer();
        await _server.StartAsync(newPort);
        _http = new HttpClient
        {
            BaseAddress = new Uri($"http://localhost:{newPort}"),
            Timeout = TimeSpan.FromSeconds(10)
        };

        // After restart, state is fresh — no providers
        _server.State.HasDataProvider(instanceId).Should().BeFalse();
        var r = await _http.GetAsync($"/api/browse/{instanceId}/containers");
        r.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // ── Large dataset browsing ────────────────────────────────────────────

    [Fact]
    public async Task BrowseLargeContainerList_Succeeds()
    {
        var instanceId = RegisterService("Database", "/tmp/large1");
        var containers = Enumerable.Range(0, 500).Select(i =>
            new DebugContainer { Name = $"Table_{i:D4}", ItemCount = i * 10 }).ToList();
        _server.State.SetDataProvider(instanceId, new ConfigurableProvider(containers: containers));

        var response = await _http.GetStringAsync($"/api/browse/{instanceId}/containers");
        var result = JsonConvert.DeserializeObject<List<DebugContainer>>(response);
        result.Should().HaveCount(500);
    }

    [Fact]
    public async Task BrowseLargeItemList_RespectsCap()
    {
        var instanceId = RegisterService("Database", "/tmp/large2");
        var items = Enumerable.Range(0, 2000).Select(i =>
            new DebugItem { Id = $"item_{i:D5}" }).ToList();
        _server.State.SetDataProvider(instanceId, new ConfigurableProvider(
            items: new() { ["BigTable"] = items }));

        // Default max is 200, but server caps at 1000
        var response = await _http.GetStringAsync($"/api/browse/{instanceId}/items?container=BigTable&max=2000");
        var result = JsonConvert.DeserializeObject<List<DebugItem>>(response);
        result.Should().HaveCount(1000);
    }

    // ── Stress: many concurrent requests ──────────────────────────────────

    [Fact]
    public async Task StressTest_100ConcurrentBrowseRequests()
    {
        var instanceId = RegisterService("Memory", "/tmp/stress1");
        _server.State.SetDataProvider(instanceId, new ConfigurableProvider(
            containers: new List<DebugContainer>
            {
                new() { Name = "Scope1", ItemCount = 50 }
            },
            items: new()
            {
                ["Scope1"] = Enumerable.Range(0, 50).Select(i =>
                    new DebugItem { Id = $"kv:key{i}", Label = $"key{i}" }).ToList()
            },
            details: new()
            {
                [("Scope1", "kv:key0")] = new DebugItemDetail
                {
                    Id = "kv:key0",
                    ContentJson = "{\"Value\":\"test\"}",
                    Summary = "test"
                }
            }));

        var tasks = Enumerable.Range(0, 100).Select(i =>
        {
            return (i % 4) switch
            {
                0 => _http.GetAsync($"/api/browse/{instanceId}/containers"),
                1 => _http.GetAsync($"/api/browse/{instanceId}/items?container=Scope1"),
                2 => _http.GetAsync($"/api/browse/{instanceId}/detail?container=Scope1&id=kv:key0"),
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

    /// <summary>Provider with configurable delay to test concurrency.</summary>
    private sealed class SlowProvider : IDebugDataProvider
    {
        private readonly TimeSpan _delay;
        private readonly List<DebugContainer> _containers;

        public SlowProvider(TimeSpan delay, List<DebugContainer> containers)
        {
            _delay = delay;
            _containers = containers;
        }

        public async Task<List<DebugContainer>> ListContainersAsync()
        {
            await Task.Delay(_delay);
            return _containers;
        }

        public async Task<List<DebugItem>> ListItemsAsync(string container, int maxItems = 200)
        {
            await Task.Delay(_delay);
            return new List<DebugItem>();
        }

        public async Task<DebugItemDetail?> GetItemDetailAsync(string container, string itemId)
        {
            await Task.Delay(_delay);
            return null;
        }
    }

    /// <summary>Provider that fails every N-th call.</summary>
    private sealed class FlakyProvider : IDebugDataProvider
    {
        private readonly int _failEveryN;
        private int _callCount;

        public FlakyProvider(int failEveryN) { _failEveryN = failEveryN; }

        public Task<List<DebugContainer>> ListContainersAsync()
        {
            if (Interlocked.Increment(ref _callCount) % _failEveryN == 0)
                throw new IOException("Simulated disk error");
            return Task.FromResult(new List<DebugContainer> { new() { Name = "T" } });
        }

        public Task<List<DebugItem>> ListItemsAsync(string container, int maxItems = 200) =>
            Task.FromResult(new List<DebugItem>());

        public Task<DebugItemDetail?> GetItemDetailAsync(string container, string itemId) =>
            Task.FromResult<DebugItemDetail?>(null);
    }

    /// <summary>Provider that always throws.</summary>
    private sealed class AlwaysThrowingProvider : IDebugDataProvider
    {
        public Task<List<DebugContainer>> ListContainersAsync() =>
            throw new InvalidOperationException("critical failure");

        public Task<List<DebugItem>> ListItemsAsync(string container, int maxItems = 200) =>
            throw new InvalidOperationException("critical failure");

        public Task<DebugItemDetail?> GetItemDetailAsync(string container, string itemId) =>
            throw new InvalidOperationException("critical failure");
    }

    /// <summary>Simple configurable provider.</summary>
    private sealed class ConfigurableProvider : IDebugDataProvider
    {
        private readonly List<DebugContainer> _containers;
        private readonly Dictionary<string, List<DebugItem>> _items;
        private readonly Dictionary<(string, string), DebugItemDetail> _details;

        public ConfigurableProvider(
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
}
