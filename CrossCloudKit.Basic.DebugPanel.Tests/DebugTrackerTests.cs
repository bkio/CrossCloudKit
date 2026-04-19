// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using FluentAssertions;
using Xunit;

namespace CrossCloudKit.Basic.DebugPanel.Tests;

/// <summary>
/// Tests for <see cref="DebugTracker"/> and <see cref="DebugTracker.OperationScope"/>.
/// </summary>
public class DebugTrackerTests : IAsyncLifetime
{
    private DebugPanelServer _server = null!;
    private HttpClient _http = null!;
    private int _port;

    public async Task InitializeAsync()
    {
        _port = TestHelpers.GetAvailablePort();
        _server = new DebugPanelServer();
        await _server.StartAsync(_port);
        _http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
    }

    public async Task DisposeAsync()
    {
        _http.Dispose();
        await _server.DisposeAsync();
    }

    [Fact]
    public async Task BeginOperation_ShouldLogToServer()
    {
        var tracker = new DebugTracker("t1", "Memory", _http, $"http://localhost:{_port}");

        using (var op = tracker.BeginOperation("SetKeyValues", "scope=test"))
        {
            await Task.Delay(10); // simulate work
        }

        // Wait for fire-and-forget POST to arrive
        await Task.Delay(500);

        _server.State.Operations.Should().Contain(o =>
            o.InstanceId == "t1" && o.OperationName == "SetKeyValues" && o.Success);
    }

    [Fact]
    public async Task BeginOperation_MarkFailed_ShouldLogFailure()
    {
        var tracker = new DebugTracker("t2", "Database", _http, $"http://localhost:{_port}");

        using (var op = tracker.BeginOperation("PutItem", "table=Users"))
        {
            op!.MarkFailed();
        }

        await Task.Delay(500);

        _server.State.Operations.Should().Contain(o =>
            o.InstanceId == "t2" && o.OperationName == "PutItem" && !o.Success);
    }

    [Fact]
    public async Task BeginOperation_ShouldMeasureDuration()
    {
        var tracker = new DebugTracker("t3", "Vector", _http, $"http://localhost:{_port}");

        using (tracker.BeginOperation("Query", "collection=docs"))
        {
            await Task.Delay(50);
        }

        await Task.Delay(500);

        _server.State.Operations.Should().Contain(o =>
            o.InstanceId == "t3" && o.DurationMs >= 30);
    }

    [Fact]
    public void BeginOperation_AfterMarkDisposed_ShouldReturnNull()
    {
        var tracker = new DebugTracker("t4", "Memory", _http, $"http://localhost:{_port}");
        tracker.MarkDisposed();

        var op = tracker.BeginOperation("SetKeyValues");
        op.Should().BeNull();
    }

    [Fact]
    public void BeginOperation_WithNullTracker_ShouldNotThrow()
    {
        // Simulates the pattern: _debugTracker?.BeginOperation(...)
        DebugTracker? tracker = null;
        var op = tracker?.BeginOperation("Test");
        op.Should().BeNull();
    }

    [Fact]
    public async Task OperationScope_DoubleDispose_ShouldNotThrow()
    {
        var tracker = new DebugTracker("t5", "File", _http, $"http://localhost:{_port}");

        var op = tracker.BeginOperation("UploadFile")!;
        op.Dispose();
        op.Dispose(); // should not throw or double-post

        await Task.Delay(500);
        _server.State.Operations.Count(o => o.InstanceId == "t5").Should().Be(1);
    }

    [Fact]
    public async Task Tracker_ShouldSilentlyIgnoreServerErrors()
    {
        // Tracker pointing to a non-existent server
        var tracker = new DebugTracker("t6", "Memory", _http, "http://localhost:1");

        // This should NOT throw
        using (tracker.BeginOperation("SetKeyValues"))
        {
            await Task.Delay(10);
        }

        await Task.Delay(200); // let fire-and-forget complete
    }

    [Fact]
    public async Task MultipleTrackers_ShouldLogIndependently()
    {
        var tracker1 = new DebugTracker("m1", "Memory", _http, $"http://localhost:{_port}");
        var tracker2 = new DebugTracker("m2", "Vector", _http, $"http://localhost:{_port}");

        using (tracker1.BeginOperation("GetKeyValue"))
        {
        }

        using (tracker2.BeginOperation("Query"))
        {
        }

        await Task.Delay(500);

        _server.State.Operations.Should().Contain(o => o.InstanceId == "m1");
        _server.State.Operations.Should().Contain(o => o.InstanceId == "m2");
    }
}
