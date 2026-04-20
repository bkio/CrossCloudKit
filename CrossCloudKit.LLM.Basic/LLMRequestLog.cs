// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Collections.Concurrent;

namespace CrossCloudKit.LLM.Basic;

/// <summary>
/// The type of LLM request that was logged.
/// </summary>
internal enum LLMRequestType
{
    Completion,
    CompletionStreaming,
    Embedding,
    EmbeddingBatch
}

/// <summary>
/// A single logged LLM request with timing, token counts, and truncated I/O.
/// </summary>
internal sealed class LLMRequestLogEntry
{
    public required LLMRequestType RequestType { get; init; }
    public required DateTime TimestampUtc { get; init; }
    public required TimeSpan Duration { get; init; }
    public required bool Success { get; init; }
    public string? ErrorMessage { get; init; }

    // ── Completion-specific ───────────────────────────────────────────────
    public string? PromptPreview { get; init; }
    public string? ResponsePreview { get; init; }
    public string? FinishReason { get; init; }
    public int? PromptTokenEstimate { get; init; }
    public int? CompletionTokenEstimate { get; init; }

    // ── Embedding-specific ────────────────────────────────────────────────
    public int? TextCount { get; init; }
    public int? Dimensions { get; init; }
    public string? FirstTextPreview { get; init; }
}

/// <summary>
/// Thread-safe ring buffer that stores the last N LLM requests for debug panel inspection.
/// Also accumulates lifetime statistics (total requests, tokens, errors, etc.).
/// </summary>
internal sealed class LLMRequestLog
{
    private readonly LLMRequestLogEntry[] _buffer;
    private int _head;
    private int _count;
    private readonly object _lock = new();

    // ── Lifetime statistics ───────────────────────────────────────────────
    private long _totalCompletionRequests;
    private long _totalEmbeddingRequests;
    private long _totalPromptTokens;
    private long _totalCompletionTokens;
    private long _totalErrors;
    private double _totalCompletionDurationMs;
    private double _totalEmbeddingDurationMs;

    public LLMRequestLog(int capacity = 100)
    {
        _buffer = new LLMRequestLogEntry[capacity];
    }

    /// <summary>Adds an entry to the ring buffer and updates statistics.</summary>
    public void Add(LLMRequestLogEntry entry)
    {
        lock (_lock)
        {
            _buffer[_head] = entry;
            _head = (_head + 1) % _buffer.Length;
            if (_count < _buffer.Length) _count++;

            // Update stats
            if (entry.RequestType is LLMRequestType.Completion or LLMRequestType.CompletionStreaming)
            {
                Interlocked.Increment(ref _totalCompletionRequests);
                _totalCompletionDurationMs += entry.Duration.TotalMilliseconds;
                if (entry.PromptTokenEstimate.HasValue)
                    Interlocked.Add(ref _totalPromptTokens, entry.PromptTokenEstimate.Value);
                if (entry.CompletionTokenEstimate.HasValue)
                    Interlocked.Add(ref _totalCompletionTokens, entry.CompletionTokenEstimate.Value);
            }
            else
            {
                Interlocked.Add(ref _totalEmbeddingRequests, entry.TextCount ?? 1);
                _totalEmbeddingDurationMs += entry.Duration.TotalMilliseconds;
            }

            if (!entry.Success)
                Interlocked.Increment(ref _totalErrors);
        }
    }

    /// <summary>Returns all entries in chronological order (oldest first).</summary>
    public List<LLMRequestLogEntry> GetEntries()
    {
        lock (_lock)
        {
            var list = new List<LLMRequestLogEntry>(_count);
            if (_count == 0) return list;

            var start = _count < _buffer.Length ? 0 : _head;
            for (var i = 0; i < _count; i++)
            {
                var idx = (start + i) % _buffer.Length;
                list.Add(_buffer[idx]);
            }
            return list;
        }
    }

    /// <summary>Returns accumulated statistics snapshot.</summary>
    public LLMRequestLogStats GetStats()
    {
        lock (_lock)
        {
            return new LLMRequestLogStats
            {
                TotalCompletionRequests = _totalCompletionRequests,
                TotalEmbeddingRequests = _totalEmbeddingRequests,
                TotalPromptTokens = _totalPromptTokens,
                TotalCompletionTokens = _totalCompletionTokens,
                TotalErrors = _totalErrors,
                AvgCompletionLatencyMs = _totalCompletionRequests > 0
                    ? _totalCompletionDurationMs / _totalCompletionRequests
                    : 0,
                AvgEmbeddingLatencyMs = _totalEmbeddingRequests > 0
                    ? _totalEmbeddingDurationMs / _totalEmbeddingRequests
                    : 0,
                EntryCount = _count
            };
        }
    }
}

/// <summary>Accumulated statistics from the request log.</summary>
internal sealed class LLMRequestLogStats
{
    public long TotalCompletionRequests { get; init; }
    public long TotalEmbeddingRequests { get; init; }
    public long TotalPromptTokens { get; init; }
    public long TotalCompletionTokens { get; init; }
    public long TotalErrors { get; init; }
    public double AvgCompletionLatencyMs { get; init; }
    public double AvgEmbeddingLatencyMs { get; init; }
    public int EntryCount { get; init; }
}
