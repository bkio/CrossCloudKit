// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Text;
using CrossCloudKit.Basic.DebugPanel;
using CrossCloudKit.Utilities.Common;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CrossCloudKit.Memory.Basic;

/// <summary>
/// Debug data provider for <see cref="MemoryServiceBasic"/>.
/// Lists scopes (like "tables") and their keys/values + lists.
/// </summary>
internal sealed class MemoryDebugDataProvider : IDebugDataProvider
{
    private readonly string _storageDirectory;

    public MemoryDebugDataProvider(string storageDirectory)
    {
        _storageDirectory = storageDirectory;
    }

    public Task<List<DebugContainer>> ListContainersAsync()
    {
        var containers = new List<DebugContainer>();

        if (!Directory.Exists(_storageDirectory))
            return Task.FromResult(containers);

        var files = Directory.GetFiles(_storageDirectory, "*.json");

        foreach (var file in files)
        {
            var fileName = Path.GetFileNameWithoutExtension(file);
            if (fileName.Contains("_mutex"))
                continue;

            try
            {
                var scopeName = EncodingUtilities.Base64DecodeNoPadding(fileName);
                var json = System.IO.File.ReadAllText(file, Encoding.UTF8);
                var data = JsonConvert.DeserializeObject<StoredData>(json);

                if (data?.ExpiryTime.HasValue == true && data.ExpiryTime <= DateTime.UtcNow)
                    continue;

                var keyCount = data?.KeyValues?.Count ?? 0;
                var listCount = data?.Lists?.Count ?? 0;

                var props = new Dictionary<string, string>();
                if (keyCount > 0) props["Keys"] = keyCount.ToString();
                if (listCount > 0) props["Lists"] = listCount.ToString();
                if (data?.ExpiryTime.HasValue == true)
                    props["Expires"] = data.ExpiryTime.Value.ToString("yyyy-MM-dd HH:mm:ss");

                containers.Add(new DebugContainer
                {
                    Name = scopeName,
                    ItemCount = keyCount + listCount,
                    Properties = props.Count > 0 ? props : null
                });
            }
            catch { /* skip corrupt files */ }
        }

        return Task.FromResult(containers);
    }

    public Task<List<DebugItem>> ListItemsAsync(string container, int maxItems = 200)
    {
        var items = new List<DebugItem>();
        var data = ReadScopeData(container);
        if (data is null)
            return Task.FromResult(items);

        // Key-values
        if (data.KeyValues != null)
        {
            foreach (var kv in data.KeyValues.Take(maxItems))
            {
                items.Add(new DebugItem
                {
                    Id = $"kv:{kv.Key}",
                    Label = kv.Key,
                    Properties = new Dictionary<string, string>
                    {
                        ["Type"] = "KeyValue",
                        ["Kind"] = kv.Value.Kind.ToString(),
                        ["Value"] = TruncateValue(kv.Value)
                    }
                });
            }
        }

        // Lists
        if (data.Lists != null)
        {
            foreach (var list in data.Lists.Take(Math.Max(0, maxItems - items.Count)))
            {
                items.Add(new DebugItem
                {
                    Id = $"list:{list.Key}",
                    Label = list.Key,
                    Properties = new Dictionary<string, string>
                    {
                        ["Type"] = "List",
                        ["Size"] = list.Value.Count.ToString()
                    }
                });
            }
        }

        return Task.FromResult(items);
    }

    public Task<DebugItemDetail?> GetItemDetailAsync(string container, string itemId)
    {
        var data = ReadScopeData(container);
        if (data is null)
            return Task.FromResult<DebugItemDetail?>(null);

        if (itemId.StartsWith("kv:"))
        {
            var key = itemId["kv:".Length..];
            if (data.KeyValues == null || !data.KeyValues.TryGetValue(key, out var value))
                return Task.FromResult<DebugItemDetail?>(null);

            var detail = new JObject
            {
                ["Scope"] = container,
                ["Key"] = key,
                ["Kind"] = value.Kind.ToString(),
                ["Value"] = PrimitiveToJToken(value)
            };

            if (data.ExpiryTime.HasValue)
                detail["ScopeExpiry"] = data.ExpiryTime.Value.ToString("O");

            return Task.FromResult<DebugItemDetail?>(new DebugItemDetail
            {
                Id = itemId,
                ContentJson = detail.ToString(Formatting.Indented),
                Summary = $"Scope: {container} | Key: {key} ({value.Kind})"
            });
        }

        if (itemId.StartsWith("list:"))
        {
            var listName = itemId["list:".Length..];
            if (data.Lists == null || !data.Lists.TryGetValue(listName, out var list))
                return Task.FromResult<DebugItemDetail?>(null);

            var detail = new JObject
            {
                ["Scope"] = container,
                ["List"] = listName,
                ["Size"] = list.Count,
                ["Elements"] = new JArray(list.Select(PrimitiveToJToken))
            };

            return Task.FromResult<DebugItemDetail?>(new DebugItemDetail
            {
                Id = itemId,
                ContentJson = detail.ToString(Formatting.Indented),
                Summary = $"Scope: {container} | List: {listName} ({list.Count} elements)"
            });
        }

        return Task.FromResult<DebugItemDetail?>(null);
    }

    private StoredData? ReadScopeData(string scope)
    {
        var fileName = EncodingUtilities.Base64EncodeNoPadding(scope);
        var filePath = Path.Combine(_storageDirectory, $"{fileName}.json");

        if (!System.IO.File.Exists(filePath))
            return null;

        try
        {
            var json = System.IO.File.ReadAllText(filePath, Encoding.UTF8);
            var data = JsonConvert.DeserializeObject<StoredData>(json);

            if (data?.ExpiryTime.HasValue == true && data.ExpiryTime <= DateTime.UtcNow)
                return null;

            return data;
        }
        catch
        {
            return null;
        }
    }

    private static string TruncateValue(Primitive value)
    {
        var str = value.Kind switch
        {
            PrimitiveKind.String => value.AsString,
            PrimitiveKind.Integer => value.AsInteger.ToString(),
            PrimitiveKind.Double => value.AsDouble.ToString("G"),
            PrimitiveKind.Boolean => value.AsBoolean.ToString(),
            PrimitiveKind.ByteArray => $"[{value.AsByteArray.Length} bytes]",
            _ => value.ToString()
        };
        return str.Length > 100 ? str[..100] + "..." : str;
    }

    private static JToken PrimitiveToJToken(Primitive value)
    {
        return value.Kind switch
        {
            PrimitiveKind.String => new JValue(value.AsString),
            PrimitiveKind.Integer => new JValue(value.AsInteger),
            PrimitiveKind.Double => new JValue(value.AsDouble),
            PrimitiveKind.Boolean => new JValue(value.AsBoolean),
            PrimitiveKind.ByteArray => new JValue(Convert.ToBase64String(value.AsByteArray)),
            _ => new JValue(value.ToString())
        };
    }

    // ── Internal storage format (mirrors MemoryServiceBasic's private record) ──

    private record StoredData
    {
        public Dictionary<string, Primitive> KeyValues { get; init; } = new();
        public Dictionary<string, List<Primitive>> Lists { get; init; } = new();
        public DateTime? ExpiryTime { get; init; }
    }
}
