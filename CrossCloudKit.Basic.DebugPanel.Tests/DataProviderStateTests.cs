// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using FluentAssertions;
using Xunit;

namespace CrossCloudKit.Basic.DebugPanel.Tests;

/// <summary>
/// Tests for <see cref="DebugPanelState"/> data provider storage/retrieval,
/// lifecycle management, and thread-safety of provider operations.
/// </summary>
public class DataProviderStateTests
{
    // ── SetDataProvider / GetDataProvider ──────────────────────────────────

    [Fact]
    public void SetAndGet_ReturnsProvider()
    {
        var state = new DebugPanelState();
        var provider = new StubProvider();

        state.SetDataProvider("inst-1", provider);
        state.GetDataProvider("inst-1").Should().BeSameAs(provider);
    }

    [Fact]
    public void Get_UnknownInstance_ReturnsNull()
    {
        var state = new DebugPanelState();
        state.GetDataProvider("unknown").Should().BeNull();
    }

    [Fact]
    public void HasDataProvider_Exists_ReturnsTrue()
    {
        var state = new DebugPanelState();
        state.SetDataProvider("inst-1", new StubProvider());
        state.HasDataProvider("inst-1").Should().BeTrue();
    }

    [Fact]
    public void HasDataProvider_NotExists_ReturnsFalse()
    {
        var state = new DebugPanelState();
        state.HasDataProvider("inst-1").Should().BeFalse();
    }

    [Fact]
    public void Set_Overwrite_ReplacesProvider()
    {
        var state = new DebugPanelState();
        var p1 = new StubProvider();
        var p2 = new StubProvider();

        state.SetDataProvider("inst-1", p1);
        state.SetDataProvider("inst-1", p2);
        state.GetDataProvider("inst-1").Should().BeSameAs(p2);
    }

    // ── Deregister removes provider ───────────────────────────────────────

    [Fact]
    public void Deregister_RemovesProvider()
    {
        var state = new DebugPanelState();
        state.Register(new ServiceRegistration
        {
            InstanceId = "inst-1",
            ServiceType = "Memory",
            Path = "/tmp",
            ProcessId = 1,
            MachineName = "m",
            StartedAtUtc = DateTime.UtcNow
        });
        state.SetDataProvider("inst-1", new StubProvider());
        state.HasDataProvider("inst-1").Should().BeTrue();

        state.Deregister("inst-1");
        state.HasDataProvider("inst-1").Should().BeFalse();
        state.GetDataProvider("inst-1").Should().BeNull();
    }

    [Fact]
    public void Deregister_NonExistent_DoesNotThrow()
    {
        var state = new DebugPanelState();
        state.SetDataProvider("inst-1", new StubProvider());

        // Deregister a different instance — should not affect inst-1
        state.Deregister("other");
        state.HasDataProvider("inst-1").Should().BeTrue();
    }

    // ── Multiple providers ────────────────────────────────────────────────

    [Fact]
    public void MultipleProviders_Isolated()
    {
        var state = new DebugPanelState();
        var p1 = new StubProvider();
        var p2 = new StubProvider();
        var p3 = new StubProvider();

        state.SetDataProvider("db-1", p1);
        state.SetDataProvider("mem-1", p2);
        state.SetDataProvider("vec-1", p3);

        state.GetDataProvider("db-1").Should().BeSameAs(p1);
        state.GetDataProvider("mem-1").Should().BeSameAs(p2);
        state.GetDataProvider("vec-1").Should().BeSameAs(p3);
    }

    // ── Thread-safety ─────────────────────────────────────────────────────

    [Fact]
    public async Task ConcurrentSetAndGet_DoesNotThrow()
    {
        var state = new DebugPanelState();
        const int threadCount = 50;

        var tasks = Enumerable.Range(0, threadCount).Select(i => Task.Run(() =>
        {
            var id = $"inst-{i}";
            state.SetDataProvider(id, new StubProvider());
            state.HasDataProvider(id).Should().BeTrue();
            state.GetDataProvider(id).Should().NotBeNull();
        })).ToArray();

        await Task.WhenAll(tasks);
    }

    [Fact]
    public async Task ConcurrentSetAndDeregister_DoesNotThrow()
    {
        var state = new DebugPanelState();
        const int threadCount = 50;

        // Pre-register services
        for (var i = 0; i < threadCount; i++)
        {
            state.Register(new ServiceRegistration
            {
                InstanceId = $"inst-{i}",
                ServiceType = "Test",
                Path = $"/tmp/{i}",
                ProcessId = 1,
                MachineName = "m",
                StartedAtUtc = DateTime.UtcNow
            });
            state.SetDataProvider($"inst-{i}", new StubProvider());
        }

        // Concurrently deregister half, set new providers for the rest
        var tasks = Enumerable.Range(0, threadCount).Select(i => Task.Run(() =>
        {
            if (i % 2 == 0)
                state.Deregister($"inst-{i}");
            else
                state.SetDataProvider($"inst-{i}", new StubProvider());
        })).ToArray();

        await Task.WhenAll(tasks);
        // No crash = success
    }

    [Fact]
    public async Task ConcurrentReadsDuringWrites_DoNotThrow()
    {
        var state = new DebugPanelState();
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        // Writer task
        var writer = Task.Run(async () =>
        {
            var i = 0;
            while (!cts.Token.IsCancellationRequested)
            {
                state.SetDataProvider($"inst-{i % 100}", new StubProvider());
                i++;
                await Task.Yield();
            }
        }, cts.Token);

        // Reader tasks
        var readers = Enumerable.Range(0, 10).Select(_ => Task.Run(async () =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                for (var j = 0; j < 100; j++)
                {
                    state.HasDataProvider($"inst-{j}");
                    state.GetDataProvider($"inst-{j}");
                }
                await Task.Yield();
            }
        }, cts.Token)).ToArray();

        try { await Task.WhenAll(readers.Append(writer)); }
        catch (OperationCanceledException) { /* expected */ }
    }

    // ── Stub ──────────────────────────────────────────────────────────────

    private sealed class StubProvider : IDebugDataProvider
    {
        public Task<List<DebugContainer>> ListContainersAsync() => Task.FromResult(new List<DebugContainer>());
        public Task<List<DebugItem>> ListItemsAsync(string container, int maxItems = 200) => Task.FromResult(new List<DebugItem>());
        public Task<DebugItemDetail?> GetItemDetailAsync(string container, string itemId) => Task.FromResult<DebugItemDetail?>(null);
    }
}
