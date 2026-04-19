// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.File.Basic;
using FluentAssertions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;


namespace CrossCloudKit.Basic.DebugPanel.Tests;

/// <summary>
/// Tests for <see cref="FileDebugDataProvider"/> — file-system-based file service browsing.
/// Covers buckets, nested files, metadata/tags, empty states, content types, large directories.
/// </summary>
public class FileDebugDataProviderTests : IDisposable
{
    private readonly string _basePath;
    private readonly IDebugDataProvider _provider;

    public FileDebugDataProviderTests()
    {
        _basePath = TestHelpers.CreateTempDir();
        _provider = new FileDebugDataProvider(_basePath);
    }

    public void Dispose()
    {
        TestHelpers.CleanupDir(_basePath);
    }

    // ── ListContainersAsync ───────────────────────────────────────────────

    [Fact]
    public async Task ListContainers_EmptyBasePath_ReturnsEmpty()
    {
        var containers = await _provider.ListContainersAsync();
        containers.Should().BeEmpty();
    }

    [Fact]
    public async Task ListContainers_NonExistentPath_ReturnsEmpty()
    {
        var provider = new FileDebugDataProvider(Path.Combine(_basePath, "nope"));
        var containers = await provider.ListContainersAsync();
        containers.Should().BeEmpty();
    }

    [Fact]
    public async Task ListContainers_SingleBucket_ReturnsOne()
    {
        CreateBucket("my-bucket");
        WriteFile("my-bucket", "file.txt", "hello");

        var containers = await _provider.ListContainersAsync();
        containers.Should().ContainSingle();
        containers[0].Name.Should().Be("my-bucket");
        containers[0].ItemCount.Should().Be(1);
    }

    [Fact]
    public async Task ListContainers_MultipleBuckets()
    {
        CreateBucket("alpha");
        CreateBucket("beta");
        WriteFile("alpha", "a1.txt", "a");
        WriteFile("alpha", "a2.txt", "b");
        WriteFile("beta", "b1.txt", "c");

        var containers = await _provider.ListContainersAsync();
        containers.Should().HaveCount(2);
        containers.First(c => c.Name == "alpha").ItemCount.Should().Be(2);
        containers.First(c => c.Name == "beta").ItemCount.Should().Be(1);
    }

    [Fact]
    public async Task ListContainers_SkipsDotPrefixedDirs()
    {
        CreateBucket("real-bucket");
        Directory.CreateDirectory(Path.Combine(_basePath, ".metadata"));
        Directory.CreateDirectory(Path.Combine(_basePath, ".tokens"));

        var containers = await _provider.ListContainersAsync();
        containers.Should().ContainSingle();
        containers[0].Name.Should().Be("real-bucket");
    }

    [Fact]
    public async Task ListContainers_NestedFiles_CountedRecursively()
    {
        CreateBucket("bucket");
        WriteFile("bucket", "top.txt", "a");
        var subDir = Path.Combine(_basePath, "bucket", "sub", "deep");
        Directory.CreateDirectory(subDir);
        System.IO.File.WriteAllText(Path.Combine(subDir, "nested.txt"), "b");

        var containers = await _provider.ListContainersAsync();
        containers[0].ItemCount.Should().Be(2);
    }

    // ── ListItemsAsync ────────────────────────────────────────────────────

    [Fact]
    public async Task ListItems_EmptyBucket_ReturnsEmpty()
    {
        CreateBucket("empty");
        var items = await _provider.ListItemsAsync("empty");
        items.Should().BeEmpty();
    }

    [Fact]
    public async Task ListItems_NonExistentBucket_ReturnsEmpty()
    {
        var items = await _provider.ListItemsAsync("nope");
        items.Should().BeEmpty();
    }

    [Fact]
    public async Task ListItems_ShowsRelativePath()
    {
        CreateBucket("b");
        WriteFile("b", "doc.pdf", "pdf content");

        var items = await _provider.ListItemsAsync("b");
        items.Should().ContainSingle();
        items[0].Id.Should().Be("doc.pdf");
        items[0].Label.Should().Be("doc.pdf");
    }

    [Fact]
    public async Task ListItems_NestedFile_ShowsRelativePath()
    {
        CreateBucket("b");
        var subDir = Path.Combine(_basePath, "b", "reports", "2024");
        Directory.CreateDirectory(subDir);
        System.IO.File.WriteAllText(Path.Combine(subDir, "q1.pdf"), "data");

        var items = await _provider.ListItemsAsync("b");
        items.Should().ContainSingle();
        items[0].Id.Should().Be("reports/2024/q1.pdf");
    }

    [Fact]
    public async Task ListItems_HasDetail_IsFalse()
    {
        CreateBucket("b");
        WriteFile("b", "f.txt", "x");

        var items = await _provider.ListItemsAsync("b");
        items[0].HasDetail.Should().BeFalse();
    }

    [Fact]
    public async Task ListItems_ShowsFileProperties()
    {
        CreateBucket("b");
        WriteFile("b", "data.json", "{\"x\":1}");

        var items = await _provider.ListItemsAsync("b");
        items[0].Properties.Should().ContainKey("Size");
        items[0].Properties.Should().ContainKey("Created");
        items[0].Properties.Should().ContainKey("Modified");
        items[0].Properties.Should().ContainKey("ContentType");
        items[0].Properties!["ContentType"].Should().Be("application/json");
    }

    [Theory]
    [InlineData("test.json", "application/json")]
    [InlineData("doc.pdf", "application/pdf")]
    [InlineData("image.png", "image/png")]
    [InlineData("photo.jpg", "image/jpeg")]
    [InlineData("style.html", "text/html")]
    [InlineData("data.csv", "text/csv")]
    [InlineData("archive.zip", "application/zip")]
    [InlineData("readme.txt", "text/plain")]
    [InlineData("mystery.xyz", "application/octet-stream")]
    public async Task ListItems_ContentType_MatchesExtension(string fileName, string expectedType)
    {
        CreateBucket("b");
        WriteFile("b", fileName, "content");

        var items = await _provider.ListItemsAsync("b");
        items[0].Properties!["ContentType"].Should().Be(expectedType);
    }

    [Fact]
    public async Task ListItems_RespectsMaxItems()
    {
        CreateBucket("b");
        for (var i = 0; i < 50; i++)
            WriteFile("b", $"file_{i:D3}.txt", "x");

        var items = await _provider.ListItemsAsync("b", maxItems: 5);
        items.Should().HaveCount(5);
    }

    // ── GetItemDetailAsync ────────────────────────────────────────────────

    [Fact]
    public async Task GetItemDetail_ExistingFile_ReturnsMetadata()
    {
        CreateBucket("b");
        WriteFile("b", "report.pdf", "pdf data here");

        var detail = await _provider.GetItemDetailAsync("b", "report.pdf");
        detail.Should().NotBeNull();
        detail!.Id.Should().Be("report.pdf");
        detail.Summary.Should().Contain("b");

        var content = JObject.Parse(detail.ContentJson);
        content["Path"]!.Value<string>().Should().Be("report.pdf");
        content["Size"]!.Value<string>().Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task GetItemDetail_NonExistentFile_ReturnsNull()
    {
        CreateBucket("b");
        var detail = await _provider.GetItemDetailAsync("b", "missing.txt");
        detail.Should().BeNull();
    }

    [Fact]
    public async Task GetItemDetail_WithTags_IncludesTags()
    {
        CreateBucket("b");
        WriteFile("b", "tagged.txt", "content");
        WriteTags("b", "tagged.txt", new Dictionary<string, string> { ["env"] = "prod", ["team"] = "backend" });

        var detail = await _provider.GetItemDetailAsync("b", "tagged.txt");
        var content = JObject.Parse(detail!.ContentJson);
        content["Tags"].Should().NotBeNull();
        content["Tags"]!["env"]!.Value<string>().Should().Be("prod");
        content["Tags"]!["team"]!.Value<string>().Should().Be("backend");
    }

    [Fact]
    public async Task GetItemDetail_NoTags_TagsIsNull()
    {
        CreateBucket("b");
        WriteFile("b", "no-tags.txt", "content");

        var detail = await _provider.GetItemDetailAsync("b", "no-tags.txt");
        var content = JObject.Parse(detail!.ContentJson);
        content["Tags"]!.Type.Should().Be(JTokenType.Null);
    }

    [Fact]
    public async Task GetItemDetail_CorruptTagsFile_TagsIsNull()
    {
        CreateBucket("b");
        WriteFile("b", "bad-tags.txt", "content");
        var metaDir = Path.Combine(_basePath, ".metadata", "b");
        Directory.CreateDirectory(metaDir);
        System.IO.File.WriteAllText(Path.Combine(metaDir, "bad-tags.txt.json"), "not valid json {{{");

        var detail = await _provider.GetItemDetailAsync("b", "bad-tags.txt");
        var content = JObject.Parse(detail!.ContentJson);
        content["Tags"]!.Type.Should().Be(JTokenType.Null);
    }

    // ── Concurrent access ─────────────────────────────────────────────────

    [Fact]
    public async Task ConcurrentListContainers_DoesNotThrow()
    {
        for (var i = 0; i < 5; i++)
        {
            CreateBucket($"bucket-{i}");
            WriteFile($"bucket-{i}", "f.txt", "x");
        }

        var tasks = Enumerable.Range(0, 20).Select(_ =>
            _provider.ListContainersAsync()).ToArray();

        var results = await Task.WhenAll(tasks);
        results.Should().AllSatisfy(r => r.Should().HaveCount(5));
    }

    [Fact]
    public async Task ConcurrentListItems_DoesNotThrow()
    {
        CreateBucket("b");
        for (var i = 0; i < 20; i++)
            WriteFile("b", $"file_{i}.txt", "x");

        var tasks = Enumerable.Range(0, 20).Select(_ =>
            _provider.ListItemsAsync("b")).ToArray();

        var results = await Task.WhenAll(tasks);
        results.Should().AllSatisfy(r => r.Should().HaveCount(20));
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private void CreateBucket(string name)
    {
        Directory.CreateDirectory(Path.Combine(_basePath, name));
    }

    private void WriteFile(string bucket, string key, string content)
    {
        var fullPath = Path.Combine(_basePath, bucket, key);
        Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);
        System.IO.File.WriteAllText(fullPath, content);
    }

    private void WriteTags(string bucket, string key, Dictionary<string, string> tags)
    {
        var metaDir = Path.Combine(_basePath, ".metadata", bucket);
        Directory.CreateDirectory(metaDir);
        System.IO.File.WriteAllText(Path.Combine(metaDir, $"{key}.json"), JsonConvert.SerializeObject(tags));
    }
}
