// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using FluentAssertions;
using Xunit;

namespace CrossCloudKit.Basic.DebugPanel.Tests;

/// <summary>
/// Tests for <see cref="DebugPanelState"/> ring buffer and concurrent data structures.
/// </summary>
public class DebugPanelStateTests
{
    [Fact]
    public void Register_ShouldAddService()
    {
        var state = new DebugPanelState();
        var reg = MakeRegistration("r1", "Memory", "/tmp/mem");
        state.Register(reg);

        state.Services.Should().ContainKey("r1");
        state.ServiceCount.Should().Be(1);
        state.OperationCountByInstance.Should().ContainKey("r1");
    }

    [Fact]
    public void Register_SameIdTwice_ShouldOverwrite()
    {
        var state = new DebugPanelState();
        state.Register(MakeRegistration("r1", "Memory", "/tmp/a"));
        state.Register(MakeRegistration("r1", "Memory", "/tmp/b"));

        state.ServiceCount.Should().Be(1);
        state.Services["r1"].Path.Should().Be("/tmp/b");
    }

    [Fact]
    public void Deregister_ShouldRemoveService()
    {
        var state = new DebugPanelState();
        state.Register(MakeRegistration("d1", "Database", "/tmp/db"));
        state.Deregister("d1").Should().BeTrue();
        state.ServiceCount.Should().Be(0);
        state.Services.Should().NotContainKey("d1");
        state.OperationCountByInstance.Should().NotContainKey("d1");
    }

    [Fact]
    public void Deregister_Unknown_ShouldReturnFalse()
    {
        var state = new DebugPanelState();
        state.Deregister("nope").Should().BeFalse();
    }

    [Fact]
    public void AddOperation_ShouldEnqueue()
    {
        var state = new DebugPanelState();
        state.Register(MakeRegistration("o1", "Vector", "/tmp/v"));
        state.AddOperation(MakeOperation("o1", "Upsert"));

        state.Operations.Should().ContainSingle(o => o.OperationName == "Upsert");
        state.OperationCountByInstance["o1"].Should().Be(1);
    }

    [Fact]
    public void AddOperation_ShouldIncrementCountPerInstance()
    {
        var state = new DebugPanelState();
        state.Register(MakeRegistration("o2", "Memory", "/tmp/m"));

        for (var i = 0; i < 10; i++)
            state.AddOperation(MakeOperation("o2", "Get"));

        state.OperationCountByInstance["o2"].Should().Be(10);
    }

    [Fact]
    public void AddOperation_ShouldCapAtMaxSize()
    {
        var state = new DebugPanelState();
        for (var i = 0; i < DebugPanelState.MaxOperationEvents + 200; i++)
            state.AddOperation(MakeOperation("x", $"Op_{i}"));

        state.Operations.Count.Should().BeLessOrEqualTo(DebugPanelState.MaxOperationEvents);
    }

    [Fact]
    public void AddOperation_RingBuffer_ShouldEvictOldest()
    {
        var state = new DebugPanelState();
        for (var i = 0; i < DebugPanelState.MaxOperationEvents + 5; i++)
            state.AddOperation(MakeOperation("x", $"Op_{i}"));

        // The first few should be evicted
        state.Operations.Should().NotContain(o => o.OperationName == "Op_0");
        // The last should remain
        state.Operations.Should().Contain(o => o.OperationName == $"Op_{DebugPanelState.MaxOperationEvents + 4}");
    }

    [Fact]
    public void AddOperation_ForUnknownInstance_ShouldStillWork()
    {
        var state = new DebugPanelState();
        state.AddOperation(MakeOperation("unknown", "Test"));
        state.Operations.Should().ContainSingle();
        state.OperationCountByInstance["unknown"].Should().Be(1);
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private static ServiceRegistration MakeRegistration(string id, string type, string path) =>
        new() { InstanceId = id, ServiceType = type, Path = path, ProcessId = 1, MachineName = "test", StartedAtUtc = DateTime.UtcNow };

    private static OperationEvent MakeOperation(string instanceId, string operationName) =>
        new() { InstanceId = instanceId, ServiceType = "Test", OperationName = operationName, TimestampUtc = DateTime.UtcNow, Success = true };
}
