// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces.Records;
using CrossCloudKit.Utilities.Common;
using CrossCloudKit.Vector.Basic;
using FluentAssertions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;


namespace CrossCloudKit.Basic.DebugPanel.Tests;

/// <summary>
/// Tests for <see cref="VectorDebugDataProvider"/> — file-system-based vector browsing.
/// Covers collections, points, metadata, vector previews, corrupt files, base64 encoding.
/// </summary>
public class VectorDebugDataProviderTests : IDisposable
{
    private readonly string _storagePath;
    private readonly IDebugDataProvider _provider;

    public VectorDebugDataProviderTests()
    {
        _storagePath = TestHelpers.CreateTempDir();
        _provider = new VectorDebugDataProvider(_storagePath);
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
        var provider = new VectorDebugDataProvider(Path.Combine(_storagePath, "nope"));
        var containers = await provider.ListContainersAsync();
        containers.Should().BeEmpty();
    }

    [Fact]
    public async Task ListContainers_SingleCollection_WithMeta()
    {
        CreateCollection("docs", 1536, "Cosine");
        WritePoint("docs", "doc-1", new float[] { 0.1f, 0.2f, 0.3f }, new JObject { ["title"] = "Intro" });

        var containers = await _provider.ListContainersAsync();
        containers.Should().ContainSingle();
        containers[0].Name.Should().Be("docs");
        containers[0].ItemCount.Should().Be(1);
        containers[0].Properties.Should().ContainKey("Dimensions");
        containers[0].Properties!["Dimensions"].Should().Be("1536");
        containers[0].Properties.Should().ContainKey("Metric");
        containers[0].Properties!["Metric"].Should().Be("Cosine");
    }

    [Fact]
    public async Task ListContainers_MultipleCollections_CountsPointsCorrectly()
    {
        CreateCollection("a", 128, "Euclidean");
        CreateCollection("b", 256, "DotProduct");
        WritePoint("a", "p1", new float[] { 1f }, null);
        WritePoint("a", "p2", new float[] { 2f }, null);
        WritePoint("b", "p1", new float[] { 3f }, null);

        var containers = await _provider.ListContainersAsync();
        containers.Should().HaveCount(2);
        containers.First(c => c.Name == "a").ItemCount.Should().Be(2);
        containers.First(c => c.Name == "b").ItemCount.Should().Be(1);
    }

    [Fact]
    public async Task ListContainers_NoMeta_StillShowsCollection()
    {
        var collectionDir = Path.Combine(_storagePath, "no-meta");
        Directory.CreateDirectory(collectionDir);
        WritePoint("no-meta", "p1", new float[] { 1f }, null);

        var containers = await _provider.ListContainersAsync();
        containers.Should().ContainSingle();
        containers[0].Name.Should().Be("no-meta");
        containers[0].ItemCount.Should().Be(1);
        containers[0].Properties.Should().BeNull();
    }

    [Fact]
    public async Task ListContainers_CorruptMeta_DoesNotThrow()
    {
        var collectionDir = Path.Combine(_storagePath, "corrupt");
        Directory.CreateDirectory(collectionDir);
        System.IO.File.WriteAllText(Path.Combine(collectionDir, "_meta.json"), "not json{{{");
        WritePoint("corrupt", "p1", new float[] { 1f }, null);

        var containers = await _provider.ListContainersAsync();
        containers.Should().ContainSingle();
        containers[0].Name.Should().Be("corrupt");
    }

    [Fact]
    public async Task ListContainers_ExcludesMetaFileFromPointCount()
    {
        CreateCollection("c", 128, "Cosine");
        WritePoint("c", "p1", new float[] { 1f }, null);

        var containers = await _provider.ListContainersAsync();
        // _meta.json should NOT be counted as a point
        containers[0].ItemCount.Should().Be(1);
    }

    // ── ListItemsAsync ────────────────────────────────────────────────────

    [Fact]
    public async Task ListItems_EmptyCollection_ReturnsEmpty()
    {
        CreateCollection("empty", 128, "Cosine");

        var items = await _provider.ListItemsAsync("empty");
        items.Should().BeEmpty();
    }

    [Fact]
    public async Task ListItems_NonExistentCollection_ReturnsEmpty()
    {
        var items = await _provider.ListItemsAsync("nope");
        items.Should().BeEmpty();
    }

    [Fact]
    public async Task ListItems_ReturnsPointIdAndVectorLength()
    {
        CreateCollection("c", 128, "Cosine");
        var vector = Enumerable.Range(0, 128).Select(i => (float)i * 0.01f).ToArray();
        WritePoint("c", "point-abc", vector, new JObject { ["category"] = "test" });

        var items = await _provider.ListItemsAsync("c");
        items.Should().ContainSingle();
        items[0].Id.Should().Be("point-abc");
        items[0].Properties.Should().ContainKey("Vector Length");
        items[0].Properties!["Vector Length"].Should().Be("128");
    }

    [Fact]
    public async Task ListItems_ShowsFirst3MetadataProps()
    {
        CreateCollection("c", 4, "Cosine");
        var meta = new JObject
        {
            ["first"] = "a",
            ["second"] = "b",
            ["third"] = "c",
            ["fourth"] = "d" // should NOT appear
        };
        WritePoint("c", "p1", new float[] { 1f, 2f, 3f, 4f }, meta);

        var items = await _provider.ListItemsAsync("c");
        items[0].Properties.Should().ContainKey("first");
        items[0].Properties.Should().ContainKey("second");
        items[0].Properties.Should().ContainKey("third");
        items[0].Properties.Should().NotContainKey("fourth");
    }

    [Fact]
    public async Task ListItems_NoMetadata_StillShowsVectorLength()
    {
        CreateCollection("c", 4, "Cosine");
        WritePoint("c", "p1", new float[] { 1f, 2f, 3f, 4f }, null);

        var items = await _provider.ListItemsAsync("c");
        items[0].Properties.Should().ContainKey("Vector Length");
        items[0].Properties!.Count.Should().Be(1);
    }

    [Fact]
    public async Task ListItems_LongMetadataValue_IsTruncated()
    {
        CreateCollection("c", 4, "Cosine");
        var longValue = new string('x', 200);
        WritePoint("c", "p1", new float[] { 1f, 2f, 3f, 4f },
            new JObject { ["long_field"] = longValue });

        var items = await _provider.ListItemsAsync("c");
        items[0].Properties!["long_field"].Length.Should().BeLessOrEqualTo(54); // 50 + "..."
    }

    [Fact]
    public async Task ListItems_RespectsMaxItems()
    {
        CreateCollection("c", 4, "Cosine");
        for (var i = 0; i < 30; i++)
            WritePoint("c", $"p{i}", new float[] { 1f, 2f, 3f, 4f }, null);

        var items = await _provider.ListItemsAsync("c", maxItems: 5);
        items.Should().HaveCount(5);
    }

    [Fact]
    public async Task ListItems_CorruptPointFile_IsSkipped()
    {
        CreateCollection("c", 4, "Cosine");
        WritePoint("c", "good", new float[] { 1f, 2f, 3f, 4f }, null);

        // Write a corrupt file
        var encoded = EncodingUtilities.Base64EncodeNoPadding("bad");
        System.IO.File.WriteAllText(Path.Combine(_storagePath, "c", $"{encoded}.json"), "not valid json");

        var items = await _provider.ListItemsAsync("c");
        items.Should().ContainSingle();
        items[0].Id.Should().Be("good");
    }

    [Fact]
    public async Task ListItems_HasDetail_IsTrue()
    {
        CreateCollection("c", 4, "Cosine");
        WritePoint("c", "p1", new float[] { 1f, 2f, 3f, 4f }, null);

        var items = await _provider.ListItemsAsync("c");
        items[0].HasDetail.Should().BeTrue();
    }

    // ── GetItemDetailAsync ────────────────────────────────────────────────

    [Fact]
    public async Task GetItemDetail_ExistingPoint_ReturnsDetail()
    {
        CreateCollection("c", 4, "Cosine");
        var meta = new JObject { ["title"] = "Intro", ["lang"] = "en" };
        WritePoint("c", "doc-1", new float[] { 0.1f, 0.2f, 0.3f, 0.4f }, meta);

        var detail = await _provider.GetItemDetailAsync("c", "doc-1");
        detail.Should().NotBeNull();
        detail!.Id.Should().Be("doc-1");
        detail.Summary.Should().Contain("c");
        detail.Summary.Should().Contain("4d");

        var content = JObject.Parse(detail.ContentJson);
        content["Id"]!.Value<string>().Should().Be("doc-1");
        content["VectorLength"]!.Value<int>().Should().Be(4);
        content["Metadata"]!["title"]!.Value<string>().Should().Be("Intro");
    }

    [Fact]
    public async Task GetItemDetail_LargeVector_ShowsPreview()
    {
        CreateCollection("c", 1536, "Cosine");
        var vector = Enumerable.Range(0, 1536).Select(i => (float)i * 0.001f).ToArray();
        WritePoint("c", "big", vector, null);

        var detail = await _provider.GetItemDetailAsync("c", "big");
        var content = JObject.Parse(detail!.ContentJson);
        var preview = content["VectorPreview"]!.Value<string>();
        preview.Should().Contain("1536 dims");
        preview.Should().Contain("...");
    }

    [Fact]
    public async Task GetItemDetail_SmallVector_ShowsFullVector()
    {
        CreateCollection("c", 4, "Cosine");
        WritePoint("c", "small", new float[] { 1f, 2f, 3f, 4f }, null);

        var detail = await _provider.GetItemDetailAsync("c", "small");
        var content = JObject.Parse(detail!.ContentJson);
        var preview = content["VectorPreview"]!.Value<string>();
        preview.Should().NotContain("...");
    }

    [Fact]
    public async Task GetItemDetail_NonExistentPoint_ReturnsNull()
    {
        CreateCollection("c", 4, "Cosine");
        var detail = await _provider.GetItemDetailAsync("c", "missing");
        detail.Should().BeNull();
    }

    [Fact]
    public async Task GetItemDetail_NonExistentCollection_ReturnsNull()
    {
        var detail = await _provider.GetItemDetailAsync("nope", "p1");
        detail.Should().BeNull();
    }

    [Fact]
    public async Task GetItemDetail_NullMetadata_ShowsEmptyObject()
    {
        CreateCollection("c", 4, "Cosine");
        WritePoint("c", "no-meta", new float[] { 1f, 2f, 3f, 4f }, null);

        var detail = await _provider.GetItemDetailAsync("c", "no-meta");
        var content = JObject.Parse(detail!.ContentJson);
        content["Metadata"].Should().NotBeNull();
        content["Metadata"]!.Type.Should().Be(JTokenType.Object);
    }

    [Fact]
    public async Task GetItemDetail_CorruptPointFile_ReturnsFallback()
    {
        CreateCollection("c", 4, "Cosine");
        var encoded = EncodingUtilities.Base64EncodeNoPadding("corrupted");
        var filePath = Path.Combine(_storagePath, "c", $"{encoded}.json");
        System.IO.File.WriteAllText(filePath, "invalid json content");

        var detail = await _provider.GetItemDetailAsync("c", "corrupted");
        // Should return fallback with raw content
        detail.Should().NotBeNull();
        detail!.ContentJson.Should().Contain("invalid json content");
    }

    // ── Base64 encoding for file lookup ───────────────────────────────────

    [Fact]
    public async Task GetItemDetail_UsesBase64EncodedFileName()
    {
        CreateCollection("c", 4, "Cosine");
        // Write a point with a special ID that gets base64-encoded
        var specialId = "item/with:special+chars";
        WritePoint("c", specialId, new float[] { 1f, 2f, 3f, 4f }, new JObject { ["X"] = 1 });

        var detail = await _provider.GetItemDetailAsync("c", specialId);
        detail.Should().NotBeNull();
        detail!.Id.Should().Be(specialId);
    }

    // ── Concurrent access ─────────────────────────────────────────────────

    [Fact]
    public async Task ConcurrentListContainers_DoesNotThrow()
    {
        for (var i = 0; i < 5; i++)
        {
            CreateCollection($"col-{i}", 128, "Cosine");
            WritePoint($"col-{i}", "p1", new float[] { 1f }, null);
        }

        var tasks = Enumerable.Range(0, 20).Select(_ =>
            _provider.ListContainersAsync()).ToArray();

        var results = await Task.WhenAll(tasks);
        results.Should().AllSatisfy(r => r.Should().HaveCount(5));
    }

    [Fact]
    public async Task ConcurrentGetItemDetail_SamePoint_DoesNotThrow()
    {
        CreateCollection("c", 4, "Cosine");
        WritePoint("c", "shared", new float[] { 1f, 2f, 3f, 4f }, new JObject { ["V"] = "test" });

        var tasks = Enumerable.Range(0, 20).Select(_ =>
            _provider.GetItemDetailAsync("c", "shared")).ToArray();

        var results = await Task.WhenAll(tasks);
        results.Should().AllSatisfy(r =>
        {
            r.Should().NotBeNull();
            r!.Id.Should().Be("shared");
        });
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private void CreateCollection(string name, int dimensions, string metric)
    {
        var dir = Path.Combine(_storagePath, name);
        Directory.CreateDirectory(dir);
        var meta = new JObject { ["VectorDimensions"] = dimensions, ["DistanceMetric"] = metric };
        System.IO.File.WriteAllText(Path.Combine(dir, "_meta.json"), meta.ToString());
    }

    private void WritePoint(string collection, string id, float[] vector, JObject? metadata)
    {
        var dir = Path.Combine(_storagePath, collection);
        Directory.CreateDirectory(dir);
        var point = new VectorPoint { Id = id, Vector = vector, Metadata = metadata };
        var encoded = EncodingUtilities.Base64EncodeNoPadding(id);
        System.IO.File.WriteAllText(Path.Combine(dir, $"{encoded}.json"),
            JsonConvert.SerializeObject(point));
    }
}
