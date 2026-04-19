// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Text;
using CrossCloudKit.Basic.DebugPanel;
using CrossCloudKit.Interfaces.Records;
using CrossCloudKit.Utilities.Common;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CrossCloudKit.Vector.Basic;

/// <summary>
/// Debug data provider for <see cref="VectorServiceBasic"/>.
/// Lists collections and points, shows vector info + metadata on detail.
/// </summary>
internal sealed class VectorDebugDataProvider : IDebugDataProvider
{
    private readonly string _storageDirectory;
    private const string MetaFileName = "_meta.json";

    public VectorDebugDataProvider(string storageDirectory)
    {
        _storageDirectory = storageDirectory;
    }

    public Task<List<DebugContainer>> ListContainersAsync()
    {
        var containers = new List<DebugContainer>();

        if (!Directory.Exists(_storageDirectory))
            return Task.FromResult(containers);

        foreach (var dir in Directory.GetDirectories(_storageDirectory))
        {
            var name = Path.GetFileName(dir);
            if (string.IsNullOrEmpty(name))
                continue;

            var metaPath = Path.Combine(dir, MetaFileName);
            var props = new Dictionary<string, string>();

            if (File.Exists(metaPath))
            {
                try
                {
                    var metaJson = File.ReadAllText(metaPath, Encoding.UTF8);
                    var meta = JsonConvert.DeserializeObject<JObject>(metaJson);
                    if (meta != null)
                    {
                        if (meta["VectorDimensions"] != null)
                            props["Dimensions"] = meta["VectorDimensions"]!.ToString();
                        if (meta["DistanceMetric"] != null)
                            props["Metric"] = meta["DistanceMetric"]!.ToString();
                    }
                }
                catch { /* ignore corrupt meta */ }
            }

            // Count points (json files minus meta)
            var pointCount = Directory.GetFiles(dir, "*.json")
                .Count(f => Path.GetFileName(f) != MetaFileName);

            containers.Add(new DebugContainer
            {
                Name = name,
                ItemCount = pointCount,
                Properties = props.Count > 0 ? props : null
            });
        }

        return Task.FromResult(containers);
    }

    public Task<List<DebugItem>> ListItemsAsync(string container, int maxItems = 200)
    {
        var items = new List<DebugItem>();
        var collectionPath = Path.Combine(_storageDirectory, container);

        if (!Directory.Exists(collectionPath))
            return Task.FromResult(items);

        var files = Directory.GetFiles(collectionPath, "*.json")
            .Where(f => Path.GetFileName(f) != MetaFileName)
            .Take(maxItems);

        foreach (var file in files)
        {
            try
            {
                var json = File.ReadAllText(file, Encoding.UTF8);
                var point = JsonConvert.DeserializeObject<VectorPoint>(json);
                if (point is null) continue;

                var props = new Dictionary<string, string>
                {
                    ["Vector Length"] = point.Vector.Length.ToString()
                };

                if (point.Metadata != null)
                {
                    foreach (var prop in point.Metadata.Properties().Take(3))
                    {
                        props[prop.Name] = prop.Value.ToString().Length > 50
                            ? prop.Value.ToString()[..50] + "..."
                            : prop.Value.ToString();
                    }
                }

                items.Add(new DebugItem
                {
                    Id = point.Id,
                    Label = point.Id,
                    Properties = props
                });
            }
            catch { /* skip corrupt files */ }
        }

        return Task.FromResult(items);
    }

    public async Task<DebugItemDetail?> GetItemDetailAsync(string container, string itemId)
    {
        var collectionPath = Path.Combine(_storageDirectory, container);
        var encoded = EncodingUtilities.Base64EncodeNoPadding(itemId);
        var filePath = Path.Combine(collectionPath, $"{encoded}.json");

        if (!File.Exists(filePath))
            return null;

        var json = await File.ReadAllTextAsync(filePath, Encoding.UTF8);

        // Build a display-friendly version: truncate vector, show metadata fully
        try
        {
            var point = JsonConvert.DeserializeObject<VectorPoint>(json);
            if (point is null) return null;

            var display = new JObject
            {
                ["Id"] = point.Id,
                ["VectorLength"] = point.Vector.Length,
                ["VectorPreview"] = point.Vector.Length > 8
                    ? $"[{string.Join(", ", point.Vector.Take(8).Select(v => v.ToString("F4")))}... ({point.Vector.Length} dims)]"
                    : $"[{string.Join(", ", point.Vector.Select(v => v.ToString("F4")))}]",
                ["Metadata"] = point.Metadata ?? new JObject()
            };

            return new DebugItemDetail
            {
                Id = itemId,
                ContentJson = display.ToString(Formatting.Indented),
                Summary = $"Collection: {container} | Vector: {point.Vector.Length}d"
            };
        }
        catch
        {
            // Fallback: return raw JSON
            return new DebugItemDetail
            {
                Id = itemId,
                ContentJson = json,
                Summary = $"Collection: {container}"
            };
        }
    }
}
