// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Text;
using CrossCloudKit.Basic.DebugPanel;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CrossCloudKit.Database.Basic;

/// <summary>
/// Debug data provider for <see cref="DatabaseServiceBasic"/>.
/// Lists tables and items, and returns item content as JSON.
/// </summary>
internal sealed class DatabaseDebugDataProvider : IDebugDataProvider
{
    private readonly string _databasePath;

    public DatabaseDebugDataProvider(string databasePath)
    {
        _databasePath = databasePath;
    }

    public Task<List<DebugContainer>> ListContainersAsync()
    {
        var containers = new List<DebugContainer>();

        if (!Directory.Exists(_databasePath))
            return Task.FromResult(containers);

        foreach (var dir in Directory.GetDirectories(_databasePath))
        {
            var name = Path.GetFileName(dir);
            if (string.IsNullOrEmpty(name) || name.StartsWith("__"))
                continue;

            var fileCount = Directory.GetFiles(dir, "*.json").Length;
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
        var tablePath = Path.Combine(_databasePath, container);

        if (!Directory.Exists(tablePath))
            return Task.FromResult(items);

        var files = Directory.GetFiles(tablePath, "*.json");
        foreach (var file in files.Take(maxItems))
        {
            var fileName = Path.GetFileNameWithoutExtension(file);
            var fi = new FileInfo(file);

            items.Add(new DebugItem
            {
                Id = fileName,
                Label = fileName,
                Properties = new Dictionary<string, string>
                {
                    ["Size"] = FormatBytes(fi.Length),
                    ["Modified"] = fi.LastWriteTimeUtc.ToString("yyyy-MM-dd HH:mm:ss")
                }
            });
        }

        return Task.FromResult(items);
    }

    public async Task<DebugItemDetail?> GetItemDetailAsync(string container, string itemId)
    {
        var tablePath = Path.Combine(_databasePath, container);
        var filePath = Path.Combine(tablePath, $"{itemId}.json");

        if (!File.Exists(filePath))
            return null;

        var json = await File.ReadAllTextAsync(filePath, Encoding.UTF8);

        // Pretty-print the JSON
        try
        {
            var obj = JToken.Parse(json);
            json = obj.ToString(Formatting.Indented);
        }
        catch
        {
            // Return raw if not valid JSON
        }

        return new DebugItemDetail
        {
            Id = itemId,
            ContentJson = json,
            Summary = $"Table: {container} | Item: {itemId}"
        };
    }

    private static string FormatBytes(long bytes)
    {
        if (bytes < 1024) return $"{bytes} B";
        if (bytes < 1024 * 1024) return $"{bytes / 1024.0:F1} KB";
        return $"{bytes / (1024.0 * 1024.0):F1} MB";
    }
}
