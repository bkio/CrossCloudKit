// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Text;
using CrossCloudKit.Memory.Basic;
using CrossCloudKit.Utilities.Common;
using FluentAssertions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;


namespace CrossCloudKit.Basic.DebugPanel.Tests;

/// <summary>
/// Tests for <see cref="MemoryDebugDataProvider"/> — file-system-based memory service browsing.
/// Covers scopes, key-values, lists, expiry, Primitive types, base64 encoding, corrupt files.
/// </summary>
public class MemoryDebugDataProviderTests : IDisposable
{
    private readonly string _storagePath;
    private readonly IDebugDataProvider _provider;

    public MemoryDebugDataProviderTests()
    {
        _storagePath = TestHelpers.CreateTempDir();
        _provider = new MemoryDebugDataProvider(_storagePath);
    }

    public void Dispose()
    {
        TestHelpers.CleanupDir(_storagePath);
    }

    // ── ListContainersAsync ───────────────────────────────────────────────

    [Fact]
    public async Task ListContainers_EmptyStorage_ReturnsEmpty()
    {
        var containers = await _provider.ListContainersAsync();
        containers.Should().BeEmpty();
    }

    [Fact]
    public async Task ListContainers_NonExistentPath_ReturnsEmpty()
    {
        var provider = new MemoryDebugDataProvider(Path.Combine(_storagePath, "nope"));
        var containers = await provider.ListContainersAsync();
        containers.Should().BeEmpty();
    }

    [Fact]
    public async Task ListContainers_SingleScope_ShowsDecodedName()
    {
        WriteScope("user:123", new Dictionary<string, Primitive>
        {
            ["Name"] = new Primitive("Alice")
        });

        var containers = await _provider.ListContainersAsync();
        containers.Should().ContainSingle();
        containers[0].Name.Should().Be("user:123");
    }

    [Fact]
    public async Task ListContainers_ShowsKeyAndListCounts()
    {
        WriteScope("scope1",
            new Dictionary<string, Primitive>
            {
                ["k1"] = new Primitive("v1"),
                ["k2"] = new Primitive("v2")
            },
            new Dictionary<string, List<Primitive>>
            {
                ["list1"] = new() { new Primitive("a") }
            });

        var containers = await _provider.ListContainersAsync();
        containers[0].ItemCount.Should().Be(3); // 2 keys + 1 list
        containers[0].Properties.Should().ContainKey("Keys");
        containers[0].Properties!["Keys"].Should().Be("2");
        containers[0].Properties.Should().ContainKey("Lists");
        containers[0].Properties!["Lists"].Should().Be("1");
    }

    [Fact]
    public async Task ListContainers_ShowsExpiry()
    {
        var expiry = DateTime.UtcNow.AddHours(1);
        WriteScope("expiring", new Dictionary<string, Primitive>
        {
            ["k"] = new Primitive("v")
        }, expiry: expiry);

        var containers = await _provider.ListContainersAsync();
        containers[0].Properties.Should().ContainKey("Expires");
    }

    [Fact]
    public async Task ListContainers_ExpiredScope_IsSkipped()
    {
        var expired = DateTime.UtcNow.AddHours(-1);
        WriteScope("expired", new Dictionary<string, Primitive>
        {
            ["k"] = new Primitive("v")
        }, expiry: expired);

        var containers = await _provider.ListContainersAsync();
        containers.Should().BeEmpty();
    }

    [Fact]
    public async Task ListContainers_SkipsMutexFiles()
    {
        WriteScope("real", new Dictionary<string, Primitive>
        {
            ["k"] = new Primitive("v")
        });
        // Write a mutex file (pattern: {base64scope}_mutex.json)
        var mutexBase = EncodingUtilities.Base64EncodeNoPadding("something");
        System.IO.File.WriteAllText(Path.Combine(_storagePath, $"{mutexBase}_mutex.json"), "{}");

        var containers = await _provider.ListContainersAsync();
        containers.Should().ContainSingle();
        containers[0].Name.Should().Be("real");
    }

    [Fact]
    public async Task ListContainers_MultipleScopes()
    {
        WriteScope("scope-a", new Dictionary<string, Primitive> { ["k"] = new Primitive("a") });
        WriteScope("scope-b", new Dictionary<string, Primitive> { ["k"] = new Primitive("b") });
        WriteScope("scope-c", new Dictionary<string, Primitive> { ["k"] = new Primitive("c") });

        var containers = await _provider.ListContainersAsync();
        containers.Should().HaveCount(3);
    }

    [Fact]
    public async Task ListContainers_CorruptFile_IsSkipped()
    {
        WriteScope("good", new Dictionary<string, Primitive> { ["k"] = new Primitive("v") });
        var badName = EncodingUtilities.Base64EncodeNoPadding("bad");
        System.IO.File.WriteAllText(Path.Combine(_storagePath, $"{badName}.json"), "not json {{{");

        var containers = await _provider.ListContainersAsync();
        containers.Should().ContainSingle();
        containers[0].Name.Should().Be("good");
    }

    // ── ListItemsAsync ────────────────────────────────────────────────────

    [Fact]
    public async Task ListItems_NonExistentScope_ReturnsEmpty()
    {
        var items = await _provider.ListItemsAsync("nope");
        items.Should().BeEmpty();
    }

    [Fact]
    public async Task ListItems_ScopeWithKeyValues_ShowsKvItems()
    {
        WriteScope("s", new Dictionary<string, Primitive>
        {
            ["Name"] = new Primitive("Alice"),
            ["Age"] = new Primitive(30L),
            ["Active"] = new Primitive(true)
        });

        var items = await _provider.ListItemsAsync("s");
        items.Should().HaveCount(3);
        items.Should().AllSatisfy(i =>
        {
            i.Id.Should().StartWith("kv:");
            i.Properties.Should().ContainKey("Type");
            i.Properties!["Type"].Should().Be("KeyValue");
            i.Properties.Should().ContainKey("Kind");
            i.Properties.Should().ContainKey("Value");
        });
    }

    [Fact]
    public async Task ListItems_KeyValueTypes_ShowCorrectKinds()
    {
        WriteScope("types", new Dictionary<string, Primitive>
        {
            ["str"] = new Primitive("hello"),
            ["num"] = new Primitive(42L),
            ["dbl"] = new Primitive(3.14),
            ["flag"] = new Primitive(true),
            ["bytes"] = new Primitive(new byte[] { 1, 2, 3 })
        });

        var items = await _provider.ListItemsAsync("types");
        items.First(i => i.Id == "kv:str").Properties!["Kind"].Should().Be("String");
        items.First(i => i.Id == "kv:num").Properties!["Kind"].Should().Be("Integer");
        items.First(i => i.Id == "kv:dbl").Properties!["Kind"].Should().Be("Double");
        items.First(i => i.Id == "kv:flag").Properties!["Kind"].Should().Be("Boolean");
        items.First(i => i.Id == "kv:bytes").Properties!["Kind"].Should().Be("ByteArray");
    }

    [Fact]
    public async Task ListItems_ScopeWithLists_ShowsListItems()
    {
        WriteScopeWithLists("s", null, new Dictionary<string, List<Primitive>>
        {
            ["tasks"] = new() { new Primitive("a"), new Primitive("b") },
            ["queue"] = new() { new Primitive("c") }
        });

        var items = await _provider.ListItemsAsync("s");
        items.Should().HaveCount(2);
        items.Should().AllSatisfy(i =>
        {
            i.Id.Should().StartWith("list:");
            i.Properties!["Type"].Should().Be("List");
            i.Properties.Should().ContainKey("Size");
        });
        items.First(i => i.Id == "list:tasks").Properties!["Size"].Should().Be("2");
        items.First(i => i.Id == "list:queue").Properties!["Size"].Should().Be("1");
    }

    [Fact]
    public async Task ListItems_MixedKeyValuesAndLists()
    {
        WriteScope("mixed",
            new Dictionary<string, Primitive> { ["key1"] = new Primitive("val1") },
            new Dictionary<string, List<Primitive>>
            {
                ["mylist"] = new() { new Primitive("item1") }
            });

        var items = await _provider.ListItemsAsync("mixed");
        items.Should().HaveCount(2);
        items.Should().Contain(i => i.Id == "kv:key1");
        items.Should().Contain(i => i.Id == "list:mylist");
    }

    [Fact]
    public async Task ListItems_RespectsMaxItems()
    {
        var kvs = new Dictionary<string, Primitive>();
        for (var i = 0; i < 50; i++)
            kvs[$"key_{i}"] = new Primitive($"val_{i}");
        WriteScope("big", kvs);

        var items = await _provider.ListItemsAsync("big", maxItems: 10);
        items.Should().HaveCount(10);
    }

    [Fact]
    public async Task ListItems_LongStringValue_IsTruncated()
    {
        var longVal = new string('x', 200);
        WriteScope("s", new Dictionary<string, Primitive>
        {
            ["long"] = new Primitive(longVal)
        });

        var items = await _provider.ListItemsAsync("s");
        items[0].Properties!["Value"].Length.Should().BeLessOrEqualTo(104); // 100 + "..."
    }

    [Fact]
    public async Task ListItems_ByteArrayValue_ShowsLength()
    {
        WriteScope("s", new Dictionary<string, Primitive>
        {
            ["data"] = new Primitive(new byte[1024])
        });

        var items = await _provider.ListItemsAsync("s");
        items[0].Properties!["Value"].Should().Contain("1024 bytes");
    }

    [Fact]
    public async Task ListItems_ExpiredScope_ReturnsEmpty()
    {
        WriteScope("expired", new Dictionary<string, Primitive>
        {
            ["k"] = new Primitive("v")
        }, expiry: DateTime.UtcNow.AddHours(-1));

        var items = await _provider.ListItemsAsync("expired");
        items.Should().BeEmpty();
    }

    // ── GetItemDetailAsync ────────────────────────────────────────────────

    [Fact]
    public async Task GetItemDetail_KeyValue_ReturnsDetail()
    {
        WriteScope("s", new Dictionary<string, Primitive>
        {
            ["Name"] = new Primitive("Alice")
        });

        var detail = await _provider.GetItemDetailAsync("s", "kv:Name");
        detail.Should().NotBeNull();
        detail!.Id.Should().Be("kv:Name");
        detail.Summary.Should().Contain("s");
        detail.Summary.Should().Contain("Name");
        detail.Summary.Should().Contain("String");

        var content = JObject.Parse(detail.ContentJson);
        content["Key"]!.Value<string>().Should().Be("Name");
        content["Kind"]!.Value<string>().Should().Be("String");
        content["Value"]!.Value<string>().Should().Be("Alice");
    }

    [Fact]
    public async Task GetItemDetail_KeyValue_IntegerType()
    {
        WriteScope("s", new Dictionary<string, Primitive>
        {
            ["Count"] = new Primitive(42L)
        });

        var detail = await _provider.GetItemDetailAsync("s", "kv:Count");
        var content = JObject.Parse(detail!.ContentJson);
        content["Kind"]!.Value<string>().Should().Be("Integer");
        content["Value"]!.Value<long>().Should().Be(42);
    }

    [Fact]
    public async Task GetItemDetail_KeyValue_DoubleType()
    {
        WriteScope("s", new Dictionary<string, Primitive>
        {
            ["Score"] = new Primitive(3.14)
        });

        var detail = await _provider.GetItemDetailAsync("s", "kv:Score");
        var content = JObject.Parse(detail!.ContentJson);
        content["Kind"]!.Value<string>().Should().Be("Double");
        content["Value"]!.Value<double>().Should().BeApproximately(3.14, 0.001);
    }

    [Fact]
    public async Task GetItemDetail_KeyValue_BooleanType()
    {
        WriteScope("s", new Dictionary<string, Primitive>
        {
            ["Active"] = new Primitive(true)
        });

        var detail = await _provider.GetItemDetailAsync("s", "kv:Active");
        var content = JObject.Parse(detail!.ContentJson);
        content["Kind"]!.Value<string>().Should().Be("Boolean");
        content["Value"]!.Value<bool>().Should().BeTrue();
    }

    [Fact]
    public async Task GetItemDetail_KeyValue_ByteArrayType()
    {
        var bytes = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };
        WriteScope("s", new Dictionary<string, Primitive>
        {
            ["Data"] = new Primitive(bytes)
        });

        var detail = await _provider.GetItemDetailAsync("s", "kv:Data");
        var content = JObject.Parse(detail!.ContentJson);
        content["Kind"]!.Value<string>().Should().Be("ByteArray");
        var base64 = content["Value"]!.Value<string>();
        Convert.FromBase64String(base64!).Should().BeEquivalentTo(bytes);
    }

    [Fact]
    public async Task GetItemDetail_KeyValue_WithExpiry_ShowsScopeExpiry()
    {
        var expiry = DateTime.UtcNow.AddHours(2);
        WriteScope("s", new Dictionary<string, Primitive>
        {
            ["k"] = new Primitive("v")
        }, expiry: expiry);

        var detail = await _provider.GetItemDetailAsync("s", "kv:k");
        var content = JObject.Parse(detail!.ContentJson);
        content["ScopeExpiry"].Should().NotBeNull();
    }

    [Fact]
    public async Task GetItemDetail_List_ReturnsAllElements()
    {
        WriteScopeWithLists("s", null, new Dictionary<string, List<Primitive>>
        {
            ["tasks"] = new()
            {
                new Primitive("task-1"),
                new Primitive("task-2"),
                new Primitive("task-3")
            }
        });

        var detail = await _provider.GetItemDetailAsync("s", "list:tasks");
        detail.Should().NotBeNull();
        detail!.Summary.Should().Contain("3 elements");

        var content = JObject.Parse(detail.ContentJson);
        content["List"]!.Value<string>().Should().Be("tasks");
        content["Size"]!.Value<int>().Should().Be(3);
        var elements = content["Elements"] as JArray;
        elements.Should().HaveCount(3);
    }

    [Fact]
    public async Task GetItemDetail_NonExistentKey_ReturnsNull()
    {
        WriteScope("s", new Dictionary<string, Primitive>
        {
            ["exists"] = new Primitive("v")
        });

        var detail = await _provider.GetItemDetailAsync("s", "kv:missing");
        detail.Should().BeNull();
    }

    [Fact]
    public async Task GetItemDetail_NonExistentList_ReturnsNull()
    {
        WriteScopeWithLists("s", null, new Dictionary<string, List<Primitive>>
        {
            ["exists"] = new() { new Primitive("v") }
        });

        var detail = await _provider.GetItemDetailAsync("s", "list:missing");
        detail.Should().BeNull();
    }

    [Fact]
    public async Task GetItemDetail_NonExistentScope_ReturnsNull()
    {
        var detail = await _provider.GetItemDetailAsync("nope", "kv:key");
        detail.Should().BeNull();
    }

    [Fact]
    public async Task GetItemDetail_InvalidPrefix_ReturnsNull()
    {
        WriteScope("s", new Dictionary<string, Primitive>
        {
            ["k"] = new Primitive("v")
        });

        var detail = await _provider.GetItemDetailAsync("s", "invalid:k");
        detail.Should().BeNull();
    }

    [Fact]
    public async Task GetItemDetail_ExpiredScope_ReturnsNull()
    {
        WriteScope("s", new Dictionary<string, Primitive>
        {
            ["k"] = new Primitive("v")
        }, expiry: DateTime.UtcNow.AddHours(-1));

        var detail = await _provider.GetItemDetailAsync("s", "kv:k");
        detail.Should().BeNull();
    }

    // ── Concurrent access ─────────────────────────────────────────────────

    [Fact]
    public async Task ConcurrentListContainers_DoesNotThrow()
    {
        for (var i = 0; i < 5; i++)
            WriteScope($"scope-{i}", new Dictionary<string, Primitive>
            {
                ["k"] = new Primitive("v")
            });

        var tasks = Enumerable.Range(0, 20).Select(_ =>
            _provider.ListContainersAsync()).ToArray();

        var results = await Task.WhenAll(tasks);
        results.Should().AllSatisfy(r => r.Should().HaveCount(5));
    }

    [Fact]
    public async Task ConcurrentGetItemDetail_SameKey_DoesNotThrow()
    {
        WriteScope("s", new Dictionary<string, Primitive>
        {
            ["shared"] = new Primitive("concurrent")
        });

        var tasks = Enumerable.Range(0, 20).Select(_ =>
            _provider.GetItemDetailAsync("s", "kv:shared")).ToArray();

        var results = await Task.WhenAll(tasks);
        results.Should().AllSatisfy(r =>
        {
            r.Should().NotBeNull();
            r!.Id.Should().Be("kv:shared");
        });
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    /// <summary>Writes scope data using actual Primitive serialization.</summary>
    private void WriteScope(string scopeName, Dictionary<string, Primitive> keyValues,
        Dictionary<string, List<Primitive>>? lists = null, DateTime? expiry = null)
    {
        var data = new
        {
            KeyValues = keyValues,
            Lists = lists ?? new Dictionary<string, List<Primitive>>(),
            ExpiryTime = expiry
        };
        var fileName = EncodingUtilities.Base64EncodeNoPadding(scopeName);
        System.IO.File.WriteAllText(Path.Combine(_storagePath, $"{fileName}.json"),
            JsonConvert.SerializeObject(data), Encoding.UTF8);
    }

    private void WriteScopeWithLists(string scopeName,
        Dictionary<string, Primitive>? keyValues,
        Dictionary<string, List<Primitive>> lists,
        DateTime? expiry = null)
    {
        WriteScope(scopeName, keyValues ?? new Dictionary<string, Primitive>(), lists, expiry);
    }
}
