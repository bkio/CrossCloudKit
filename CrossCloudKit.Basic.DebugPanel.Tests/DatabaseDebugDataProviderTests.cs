// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Database.Basic;
using FluentAssertions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;


namespace CrossCloudKit.Basic.DebugPanel.Tests;

/// <summary>
/// Tests for <see cref="DatabaseDebugDataProvider"/> — file-system-based database browsing.
/// Covers normal CRUD data, empty states, corrupt files, missing directories,
/// large tables, special characters, and concurrent access.
/// </summary>
public class DatabaseDebugDataProviderTests : IDisposable
{
    private readonly string _dbPath;
    private readonly IDebugDataProvider _provider;

    public DatabaseDebugDataProviderTests()
    {
        _dbPath = TestHelpers.CreateTempDir();
        _provider = new DatabaseDebugDataProvider(_dbPath);
    }

    public void Dispose()
    {
        TestHelpers.CleanupDir(_dbPath);
    }

    // ── ListContainersAsync ───────────────────────────────────────────────

    [Fact]
    public async Task ListContainers_EmptyDatabase_ReturnsEmpty()
    {
        var containers = await _provider.ListContainersAsync();
        containers.Should().BeEmpty();
    }

    [Fact]
    public async Task ListContainers_NonExistentPath_ReturnsEmpty()
    {
        var provider = new DatabaseDebugDataProvider(Path.Combine(_dbPath, "nonexistent"));
        var containers = await provider.ListContainersAsync();
        containers.Should().BeEmpty();
    }

    [Fact]
    public async Task ListContainers_SingleTable_ReturnsOneContainer()
    {
        CreateTable("Users");
        WriteItem("Users", "user1", new JObject { ["Name"] = "Alice" });

        var containers = await _provider.ListContainersAsync();
        containers.Should().ContainSingle();
        containers[0].Name.Should().Be("Users");
        containers[0].ItemCount.Should().Be(1);
    }

    [Fact]
    public async Task ListContainers_MultipleTables_ReturnsAll()
    {
        CreateTable("Users");
        CreateTable("Orders");
        CreateTable("Products");
        WriteItem("Users", "u1", new JObject { ["X"] = 1 });
        WriteItem("Users", "u2", new JObject { ["X"] = 2 });
        WriteItem("Orders", "o1", new JObject { ["Y"] = 1 });

        var containers = await _provider.ListContainersAsync();
        containers.Should().HaveCount(3);
        containers.First(c => c.Name == "Users").ItemCount.Should().Be(2);
        containers.First(c => c.Name == "Orders").ItemCount.Should().Be(1);
        containers.First(c => c.Name == "Products").ItemCount.Should().Be(0);
    }

    [Fact]
    public async Task ListContainers_SkipsDunderPrefixedDirs()
    {
        CreateTable("__internal");
        CreateTable("Users");

        var containers = await _provider.ListContainersAsync();
        containers.Should().ContainSingle();
        containers[0].Name.Should().Be("Users");
    }

    [Fact]
    public async Task ListContainers_CountsOnlyJsonFiles()
    {
        CreateTable("Mixed");
        WriteItem("Mixed", "item1", new JObject { ["A"] = 1 });
        System.IO.File.WriteAllText(Path.Combine(_dbPath, "Mixed", "readme.txt"), "not json");

        var containers = await _provider.ListContainersAsync();
        containers[0].ItemCount.Should().Be(1);
    }

    // ── ListItemsAsync ────────────────────────────────────────────────────

    [Fact]
    public async Task ListItems_EmptyTable_ReturnsEmpty()
    {
        CreateTable("Empty");

        var items = await _provider.ListItemsAsync("Empty");
        items.Should().BeEmpty();
    }

    [Fact]
    public async Task ListItems_NonExistentTable_ReturnsEmpty()
    {
        var items = await _provider.ListItemsAsync("DoesNotExist");
        items.Should().BeEmpty();
    }

    [Fact]
    public async Task ListItems_ReturnsFileNameAsId()
    {
        CreateTable("T");
        WriteItem("T", "pk_val123", new JObject { ["V"] = "x" });

        var items = await _provider.ListItemsAsync("T");
        items.Should().ContainSingle();
        items[0].Id.Should().Be("pk_val123");
        items[0].Label.Should().Be("pk_val123");
    }

    [Fact]
    public async Task ListItems_ShowsSizeAndModifiedDate()
    {
        CreateTable("T");
        WriteItem("T", "item1", new JObject { ["Data"] = "test value" });

        var items = await _provider.ListItemsAsync("T");
        items[0].Properties.Should().ContainKey("Size");
        items[0].Properties.Should().ContainKey("Modified");
        items[0].Properties!["Size"].Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task ListItems_HasDetail_IsTrue()
    {
        CreateTable("T");
        WriteItem("T", "item1", new JObject { ["X"] = 1 });

        var items = await _provider.ListItemsAsync("T");
        items[0].HasDetail.Should().BeTrue();
    }

    [Fact]
    public async Task ListItems_RespectsMaxItems()
    {
        CreateTable("Big");
        for (var i = 0; i < 50; i++)
            WriteItem("Big", $"item_{i:D3}", new JObject { ["I"] = i });

        var items = await _provider.ListItemsAsync("Big", maxItems: 10);
        items.Should().HaveCount(10);
    }

    [Fact]
    public async Task ListItems_DefaultMaxIs200()
    {
        CreateTable("Big");
        for (var i = 0; i < 250; i++)
            WriteItem("Big", $"item_{i:D4}", new JObject { ["I"] = i });

        var items = await _provider.ListItemsAsync("Big");
        items.Should().HaveCount(200);
    }

    // ── GetItemDetailAsync ────────────────────────────────────────────────

    [Fact]
    public async Task GetItemDetail_ExistingItem_ReturnsFormattedJson()
    {
        CreateTable("T");
        var obj = new JObject { ["Name"] = "Alice", ["Age"] = 30, ["Active"] = true };
        WriteItem("T", "u1", obj);

        var detail = await _provider.GetItemDetailAsync("T", "u1");
        detail.Should().NotBeNull();
        detail!.Id.Should().Be("u1");
        detail.Summary.Should().Contain("T");
        detail.Summary.Should().Contain("u1");

        // ContentJson should be pretty-printed valid JSON
        var parsed = JToken.Parse(detail.ContentJson);
        parsed["Name"]!.Value<string>().Should().Be("Alice");
        parsed["Age"]!.Value<int>().Should().Be(30);
    }

    [Fact]
    public async Task GetItemDetail_NonExistentItem_ReturnsNull()
    {
        CreateTable("T");
        var detail = await _provider.GetItemDetailAsync("T", "missing");
        detail.Should().BeNull();
    }

    [Fact]
    public async Task GetItemDetail_NonExistentTable_ReturnsNull()
    {
        var detail = await _provider.GetItemDetailAsync("NoTable", "item1");
        detail.Should().BeNull();
    }

    [Fact]
    public async Task GetItemDetail_CorruptJson_ReturnsRawContent()
    {
        CreateTable("T");
        System.IO.File.WriteAllText(Path.Combine(_dbPath, "T", "bad.json"), "this is not json {{{");

        var detail = await _provider.GetItemDetailAsync("T", "bad");
        detail.Should().NotBeNull();
        detail!.ContentJson.Should().Contain("this is not json");
    }

    [Fact]
    public async Task GetItemDetail_ComplexNestedObject_PrettyPrints()
    {
        CreateTable("T");
        var complex = new JObject
        {
            ["User"] = new JObject
            {
                ["Name"] = "Bob",
                ["Address"] = new JObject { ["City"] = "Berlin", ["Zip"] = "10115" }
            },
            ["Tags"] = new JArray("admin", "user"),
            ["Score"] = 99.5
        };
        WriteItem("T", "nested", complex);

        var detail = await _provider.GetItemDetailAsync("T", "nested");
        detail!.ContentJson.Should().Contain("Berlin");
        detail.ContentJson.Should().Contain("admin");
        // Should be multi-line (pretty-printed)
        detail.ContentJson.Should().Contain("\n");
    }

    [Fact]
    public async Task GetItemDetail_EmptyJsonObject()
    {
        CreateTable("T");
        WriteItem("T", "empty", new JObject());

        var detail = await _provider.GetItemDetailAsync("T", "empty");
        detail.Should().NotBeNull();
        detail!.ContentJson.Should().Be("{}");
    }

    [Fact]
    public async Task GetItemDetail_LargeItem_Succeeds()
    {
        CreateTable("T");
        var large = new JObject();
        for (var i = 0; i < 1000; i++)
            large[$"key_{i}"] = $"value_{i}_" + new string('x', 100);
        WriteItem("T", "large", large);

        var detail = await _provider.GetItemDetailAsync("T", "large");
        detail.Should().NotBeNull();
        var parsed = JToken.Parse(detail!.ContentJson);
        (parsed as JObject)!.Count.Should().Be(1000);
    }

    // ── Special characters in names ───────────────────────────────────────

    [Fact]
    public async Task ListItems_SpecialCharsInFileName()
    {
        CreateTable("T");
        WriteItem("T", "key_with-dashes.and_dots", new JObject { ["X"] = 1 });

        var items = await _provider.ListItemsAsync("T");
        items.Should().ContainSingle();
        items[0].Id.Should().Be("key_with-dashes.and_dots");
    }

    // ── Concurrent access ─────────────────────────────────────────────────

    [Fact]
    public async Task ConcurrentListContainers_DoesNotThrow()
    {
        for (var i = 0; i < 10; i++)
        {
            CreateTable($"Table_{i}");
            WriteItem($"Table_{i}", "item", new JObject { ["V"] = i });
        }

        var tasks = Enumerable.Range(0, 20).Select(_ =>
            _provider.ListContainersAsync()).ToArray();

        var results = await Task.WhenAll(tasks);
        results.Should().AllSatisfy(r => r.Should().HaveCount(10));
    }

    [Fact]
    public async Task ConcurrentGetItemDetail_SameItem_DoesNotThrow()
    {
        CreateTable("T");
        WriteItem("T", "shared", new JObject { ["V"] = "concurrent" });

        var tasks = Enumerable.Range(0, 20).Select(_ =>
            _provider.GetItemDetailAsync("T", "shared")).ToArray();

        var results = await Task.WhenAll(tasks);
        results.Should().AllSatisfy(r =>
        {
            r.Should().NotBeNull();
            r!.Id.Should().Be("shared");
        });
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private void CreateTable(string name)
    {
        Directory.CreateDirectory(Path.Combine(_dbPath, name));
    }

    private void WriteItem(string table, string itemId, JObject data)
    {
        var path = Path.Combine(_dbPath, table, $"{itemId}.json");
        System.IO.File.WriteAllText(path, data.ToString(Formatting.None));
    }
}
