// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Basic.DebugPanel;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CrossCloudKit.LLM.Basic;

/// <summary>
/// Debug data provider for <see cref="LLMServiceBasic"/>.
/// Exposes model info, runtime statistics, and recent request history.
/// </summary>
internal sealed class LLMDebugDataProvider : IDebugDataProvider
{
    private readonly LLMRequestLog _log;
    private readonly LLMModelInfo _completionModel;
    private readonly LLMModelInfo _embeddingModel;

    public LLMDebugDataProvider(LLMRequestLog log, LLMModelInfo completionModel, LLMModelInfo embeddingModel)
    {
        _log = log;
        _completionModel = completionModel;
        _embeddingModel = embeddingModel;
    }

    public Task<List<DebugContainer>> ListContainersAsync()
    {
        var stats = _log.GetStats();
        var entries = _log.GetEntries();

        var containers = new List<DebugContainer>
        {
            new()
            {
                Name = "Models",
                ItemCount = (_completionModel.Available ? 1 : 0) + (_embeddingModel.Available ? 1 : 0),
                Properties = new Dictionary<string, string>
                {
                    ["Completion"] = _completionModel.Available ? "Loaded" : "Unavailable",
                    ["Embedding"] = _embeddingModel.Available ? "Loaded" : "Unavailable"
                }
            },
            new()
            {
                Name = "Statistics",
                ItemCount = -1,
                Properties = new Dictionary<string, string>
                {
                    ["Completions"] = stats.TotalCompletionRequests.ToString(),
                    ["Embeddings"] = stats.TotalEmbeddingRequests.ToString(),
                    ["Errors"] = stats.TotalErrors.ToString()
                }
            },
            new()
            {
                Name = "Recent Requests",
                ItemCount = entries.Count,
                Properties = new Dictionary<string, string>
                {
                    ["Buffer Size"] = $"{entries.Count} / {100}"
                }
            }
        };

        return Task.FromResult(containers);
    }

    public Task<List<DebugItem>> ListItemsAsync(string container, int maxItems = 200)
    {
        var items = new List<DebugItem>();

        switch (container)
        {
            case "Models":
                if (_completionModel.Available)
                {
                    items.Add(new DebugItem
                    {
                        Id = "completion",
                        Label = "Completion Model",
                        Properties = new Dictionary<string, string>
                        {
                            ["File"] = Path.GetFileName(_completionModel.Path ?? "N/A"),
                            ["Size"] = FormatBytes(_completionModel.FileSizeBytes),
                            ["Context"] = _completionModel.ContextSize?.ToString() ?? "N/A"
                        }
                    });
                }
                if (_embeddingModel.Available)
                {
                    items.Add(new DebugItem
                    {
                        Id = "embedding",
                        Label = "Embedding Model",
                        Properties = new Dictionary<string, string>
                        {
                            ["File"] = Path.GetFileName(_embeddingModel.Path ?? "N/A"),
                            ["Size"] = FormatBytes(_embeddingModel.FileSizeBytes)
                        }
                    });
                }
                break;

            case "Statistics":
            {
                var stats = _log.GetStats();
                items.Add(new DebugItem
                {
                    Id = "completion-stats",
                    Label = "Completion Stats",
                    HasDetail = true,
                    Properties = new Dictionary<string, string>
                    {
                        ["Requests"] = stats.TotalCompletionRequests.ToString(),
                        ["Prompt Tokens"] = stats.TotalPromptTokens.ToString(),
                        ["Completion Tokens"] = stats.TotalCompletionTokens.ToString(),
                        ["Avg Latency"] = $"{stats.AvgCompletionLatencyMs:F0}ms"
                    }
                });
                items.Add(new DebugItem
                {
                    Id = "embedding-stats",
                    Label = "Embedding Stats",
                    HasDetail = true,
                    Properties = new Dictionary<string, string>
                    {
                        ["Requests"] = stats.TotalEmbeddingRequests.ToString(),
                        ["Avg Latency"] = $"{stats.AvgEmbeddingLatencyMs:F0}ms"
                    }
                });
                if (stats.TotalErrors > 0)
                {
                    items.Add(new DebugItem
                    {
                        Id = "error-stats",
                        Label = "Errors",
                        HasDetail = false,
                        Properties = new Dictionary<string, string>
                        {
                            ["Total Errors"] = stats.TotalErrors.ToString()
                        }
                    });
                }
                break;
            }

            case "Recent Requests":
            {
                var entries = _log.GetEntries();
                // Newest first
                for (var i = entries.Count - 1; i >= 0 && items.Count < maxItems; i--)
                {
                    var entry = entries[i];
                    var props = new Dictionary<string, string>
                    {
                        ["Type"] = entry.RequestType.ToString(),
                        ["Duration"] = $"{entry.Duration.TotalMilliseconds:F0}ms",
                        ["Status"] = entry.Success ? "OK" : "Error"
                    };

                    if (entry.RequestType is LLMRequestType.Completion or LLMRequestType.CompletionStreaming)
                    {
                        if (entry.CompletionTokenEstimate.HasValue)
                            props["Tokens"] = $"{entry.PromptTokenEstimate ?? 0}+{entry.CompletionTokenEstimate.Value}";
                        if (entry.FinishReason is not null)
                            props["Finish"] = entry.FinishReason;
                    }
                    else
                    {
                        if (entry.TextCount.HasValue)
                            props["Texts"] = entry.TextCount.Value.ToString();
                        if (entry.Dimensions.HasValue)
                            props["Dims"] = entry.Dimensions.Value.ToString();
                    }

                    var label = entry.RequestType switch
                    {
                        LLMRequestType.Completion => Truncate(entry.PromptPreview, 60),
                        LLMRequestType.CompletionStreaming => Truncate(entry.PromptPreview, 60),
                        LLMRequestType.Embedding => Truncate(entry.FirstTextPreview, 60),
                        LLMRequestType.EmbeddingBatch => $"Batch ({entry.TextCount} texts)",
                        _ => "Request"
                    };

                    items.Add(new DebugItem
                    {
                        Id = $"req-{entries.Count - 1 - i}",
                        Label = label,
                        Properties = props
                    });
                }
                break;
            }
        }

        return Task.FromResult(items);
    }

    public Task<DebugItemDetail?> GetItemDetailAsync(string container, string itemId)
    {
        switch (container)
        {
            case "Models":
            {
                var model = itemId == "completion" ? _completionModel : _embeddingModel;
                var detail = new JObject
                {
                    ["Available"] = model.Available,
                    ["Path"] = model.Path ?? "N/A",
                    ["FileSize"] = FormatBytes(model.FileSizeBytes),
                    ["FileSizeBytes"] = model.FileSizeBytes
                };
                if (model.ContextSize.HasValue)
                    detail["ContextSize"] = model.ContextSize.Value;

                return Task.FromResult<DebugItemDetail?>(new DebugItemDetail
                {
                    Id = itemId,
                    ContentJson = detail.ToString(Formatting.Indented),
                    Summary = model.Available ? $"{Path.GetFileName(model.Path)} ({FormatBytes(model.FileSizeBytes)})" : "Model not loaded"
                });
            }

            case "Statistics":
            {
                var stats = _log.GetStats();
                var detail = JObject.FromObject(stats);
                return Task.FromResult<DebugItemDetail?>(new DebugItemDetail
                {
                    Id = itemId,
                    ContentJson = detail.ToString(Formatting.Indented),
                    Summary = $"{stats.TotalCompletionRequests} completions, {stats.TotalEmbeddingRequests} embeddings, {stats.TotalErrors} errors"
                });
            }

            case "Recent Requests":
            {
                if (!itemId.StartsWith("req-") || !int.TryParse(itemId.AsSpan(4), out var reverseIdx))
                    return Task.FromResult<DebugItemDetail?>(null);

                var entries = _log.GetEntries();
                var idx = entries.Count - 1 - reverseIdx;
                if (idx < 0 || idx >= entries.Count)
                    return Task.FromResult<DebugItemDetail?>(null);

                var entry = entries[idx];
                var detail = new JObject
                {
                    ["RequestType"] = entry.RequestType.ToString(),
                    ["Timestamp"] = entry.TimestampUtc.ToString("O"),
                    ["Duration"] = $"{entry.Duration.TotalMilliseconds:F1}ms",
                    ["Success"] = entry.Success
                };

                if (entry.ErrorMessage is not null)
                    detail["Error"] = entry.ErrorMessage;

                if (entry.RequestType is LLMRequestType.Completion or LLMRequestType.CompletionStreaming)
                {
                    detail["Prompt"] = entry.PromptPreview;
                    detail["Response"] = entry.ResponsePreview;
                    detail["FinishReason"] = entry.FinishReason;
                    detail["PromptTokenEstimate"] = entry.PromptTokenEstimate;
                    detail["CompletionTokenEstimate"] = entry.CompletionTokenEstimate;
                }
                else
                {
                    detail["TextCount"] = entry.TextCount;
                    detail["Dimensions"] = entry.Dimensions;
                    detail["FirstText"] = entry.FirstTextPreview;
                }

                var summary = entry.Success
                    ? $"{entry.RequestType} — {entry.Duration.TotalMilliseconds:F0}ms"
                    : $"{entry.RequestType} — FAILED: {Truncate(entry.ErrorMessage, 80)}";

                return Task.FromResult<DebugItemDetail?>(new DebugItemDetail
                {
                    Id = itemId,
                    ContentJson = detail.ToString(Formatting.Indented),
                    Summary = summary
                });
            }
        }

        return Task.FromResult<DebugItemDetail?>(null);
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private static string Truncate(string? value, int maxLen)
    {
        if (string.IsNullOrEmpty(value)) return "(empty)";
        return value.Length <= maxLen ? value : value[..maxLen] + "…";
    }

    private static string FormatBytes(long bytes)
    {
        if (bytes < 0) return "N/A";
        if (bytes < 1024) return $"{bytes} B";
        if (bytes < 1024 * 1024) return $"{bytes / 1024.0:F1} KB";
        if (bytes < 1024 * 1024 * 1024) return $"{bytes / (1024.0 * 1024.0):F1} MB";
        return $"{bytes / (1024.0 * 1024.0 * 1024.0):F2} GB";
    }
}

/// <summary>
/// Metadata about a loaded (or not-loaded) LLM model.
/// </summary>
internal sealed class LLMModelInfo
{
    public bool Available { get; init; }
    public string? Path { get; init; }
    public long FileSizeBytes { get; init; } = -1;
    public int? ContextSize { get; init; }
}
