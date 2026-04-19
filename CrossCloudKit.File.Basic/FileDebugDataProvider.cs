// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Basic.DebugPanel;
using Newtonsoft.Json;

namespace CrossCloudKit.File.Basic;

/// <summary>
/// Debug data provider for <see cref="FileServiceBasic"/>.
/// Lists buckets and files with metadata (size, dates).
/// </summary>
internal sealed class FileDebugDataProvider : IDebugDataProvider
{
    private readonly string _basePath;
    private const string MetadataSubfolder = ".metadata";
    private const string TokensSubfolder = ".tokens";

    public FileDebugDataProvider(string basePath)
    {
        _basePath = basePath;
    }

    public Task<List<DebugContainer>> ListContainersAsync()
    {
        var containers = new List<DebugContainer>();

        if (!Directory.Exists(_basePath))
            return Task.FromResult(containers);

        foreach (var dir in Directory.GetDirectories(_basePath))
        {
            var name = Path.GetFileName(dir);
            if (string.IsNullOrEmpty(name) || name.StartsWith("."))
                continue; // skip .metadata, .tokens

            var fileCount = CountFiles(dir);
            containers.Add(new DebugContainer
            {
                Name = name,
                ItemCount = fileCount
            });
        }

        return Task.FromResult(containers);
    }

    public Task<List<DebugItem>> ListItemsAsync(string container, int maxItems = 200)
    {
        var items = new List<DebugItem>();
        var bucketPath = Path.Combine(_basePath, container);

        if (!Directory.Exists(bucketPath))
            return Task.FromResult(items);

        var files = Directory.GetFiles(bucketPath, "*", SearchOption.AllDirectories);
        foreach (var file in files.Take(maxItems))
        {
            var relativePath = Path.GetRelativePath(bucketPath, file).Replace('\\', '/');
            var fi = new FileInfo(file);

            items.Add(new DebugItem
            {
                Id = relativePath,
                Label = relativePath,
                HasDetail = false, // Files don't have a JSON detail popup — info is in properties
                Properties = new Dictionary<string, string>
                {
                    ["Size"] = FormatBytes(fi.Length),
                    ["Created"] = fi.CreationTimeUtc.ToString("yyyy-MM-dd HH:mm:ss"),
                    ["Modified"] = fi.LastWriteTimeUtc.ToString("yyyy-MM-dd HH:mm:ss"),
                    ["ContentType"] = GuessContentType(file)
                }
            });
        }

        return Task.FromResult(items);
    }

    public Task<DebugItemDetail?> GetItemDetailAsync(string container, string itemId)
    {
        // For file service, we show metadata rather than file content
        var bucketPath = Path.Combine(_basePath, container);
        var filePath = Path.Combine(bucketPath, itemId);

        if (!System.IO.File.Exists(filePath))
            return Task.FromResult<DebugItemDetail?>(null);

        var fi = new FileInfo(filePath);
        var detail = new
        {
            Path = itemId,
            fi.Length,
            Size = FormatBytes(fi.Length),
            CreatedUtc = fi.CreationTimeUtc,
            ModifiedUtc = fi.LastWriteTimeUtc,
            ContentType = GuessContentType(filePath),
            Tags = ReadTags(container, itemId)
        };

        return Task.FromResult<DebugItemDetail?>(new DebugItemDetail
        {
            Id = itemId,
            ContentJson = JsonConvert.SerializeObject(detail, Formatting.Indented),
            Summary = $"Bucket: {container} | {FormatBytes(fi.Length)}"
        });
    }

    private Dictionary<string, string>? ReadTags(string bucket, string key)
    {
        var metadataPath = Path.Combine(_basePath, MetadataSubfolder, bucket, $"{key}.json");
        if (!System.IO.File.Exists(metadataPath))
            return null;

        try
        {
            var json = System.IO.File.ReadAllText(metadataPath);
            return JsonConvert.DeserializeObject<Dictionary<string, string>>(json);
        }
        catch
        {
            return null;
        }
    }

    private static int CountFiles(string directory)
    {
        try { return Directory.GetFiles(directory, "*", SearchOption.AllDirectories).Length; }
        catch { return -1; }
    }

    private static string FormatBytes(long bytes)
    {
        if (bytes < 1024) return $"{bytes} B";
        if (bytes < 1024 * 1024) return $"{bytes / 1024.0:F1} KB";
        return $"{bytes / (1024.0 * 1024.0):F1} MB";
    }

    private static string GuessContentType(string filePath)
    {
        return Path.GetExtension(filePath).ToLowerInvariant() switch
        {
            ".json" => "application/json",
            ".txt" => "text/plain",
            ".html" or ".htm" => "text/html",
            ".xml" => "application/xml",
            ".csv" => "text/csv",
            ".pdf" => "application/pdf",
            ".png" => "image/png",
            ".jpg" or ".jpeg" => "image/jpeg",
            ".gif" => "image/gif",
            ".svg" => "image/svg+xml",
            ".zip" => "application/zip",
            ".gz" => "application/gzip",
            _ => "application/octet-stream"
        };
    }
}
