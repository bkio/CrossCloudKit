// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Basic.DebugPanel;
using FluentAssertions;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CrossCloudKit.LLM.Basic.Tests;

/// <summary>
/// Tests for <see cref="LLMRequestLog"/>, <see cref="LLMDebugDataProvider"/>,
/// and <see cref="LLMModelInfo"/>.
/// These are pure unit tests — no models loaded, no Kestrel server.
/// </summary>
public class LLMDebugDataProviderTests
{
    // ── LLMRequestLog ─────────────────────────────────────────────────────

    [Fact]
    public void RequestLog_Add_ShouldStoreEntry()
    {
        var log = new LLMRequestLog(10);
        log.Add(MakeCompletionEntry(success: true));

        var entries = log.GetEntries();
        entries.Should().HaveCount(1);
        entries[0].RequestType.Should().Be(LLMRequestType.Completion);
    }

    [Fact]
    public void RequestLog_RingBuffer_ShouldEvictOldEntries()
    {
        var log = new LLMRequestLog(3);
        for (var i = 0; i < 5; i++)
            log.Add(MakeCompletionEntry(success: true, prompt: $"prompt-{i}"));

        var entries = log.GetEntries();
        entries.Should().HaveCount(3);
        // Oldest first — should have prompts 2, 3, 4
        entries[0].PromptPreview.Should().Be("prompt-2");
        entries[2].PromptPreview.Should().Be("prompt-4");
    }

    [Fact]
    public void RequestLog_Stats_ShouldAccumulate()
    {
        var log = new LLMRequestLog();
        log.Add(MakeCompletionEntry(success: true, promptTokens: 10, completionTokens: 20));
        log.Add(MakeCompletionEntry(success: true, promptTokens: 30, completionTokens: 40));
        log.Add(MakeCompletionEntry(success: false));
        log.Add(MakeEmbeddingEntry(success: true, textCount: 5));
        log.Add(MakeEmbeddingEntry(success: true, textCount: 3));

        var stats = log.GetStats();
        stats.TotalCompletionRequests.Should().Be(3);
        stats.TotalEmbeddingRequests.Should().Be(8); // 5 + 3
        stats.TotalPromptTokens.Should().Be(40); // 10 + 30
        stats.TotalCompletionTokens.Should().Be(60); // 20 + 40
        stats.TotalErrors.Should().Be(1);
    }

    [Fact]
    public void RequestLog_Stats_AverageLatency()
    {
        var log = new LLMRequestLog();
        log.Add(MakeCompletionEntry(success: true, durationMs: 100));
        log.Add(MakeCompletionEntry(success: true, durationMs: 200));

        var stats = log.GetStats();
        stats.AvgCompletionLatencyMs.Should().BeApproximately(150, 1);
    }

    [Fact]
    public void RequestLog_Empty_StatsAreZero()
    {
        var log = new LLMRequestLog();
        var stats = log.GetStats();
        stats.TotalCompletionRequests.Should().Be(0);
        stats.TotalEmbeddingRequests.Should().Be(0);
        stats.AvgCompletionLatencyMs.Should().Be(0);
        stats.EntryCount.Should().Be(0);
    }

    [Fact]
    public async Task RequestLog_ThreadSafety()
    {
        var log = new LLMRequestLog(50);
        var tasks = Enumerable.Range(0, 20).Select(i => Task.Run(() =>
        {
            for (var j = 0; j < 10; j++)
                log.Add(MakeCompletionEntry(success: true, prompt: $"t{i}-{j}"));
        })).ToArray();

        await Task.WhenAll(tasks);

        var entries = log.GetEntries();
        entries.Should().HaveCount(50); // ring buffer capped at 50
        var stats = log.GetStats();
        stats.TotalCompletionRequests.Should().Be(200); // 20 * 10
    }

    // ── LLMDebugDataProvider — Containers ─────────────────────────────────

    [Fact]
    public async Task ListContainers_ShouldReturnThreeContainers()
    {
        var provider = CreateProvider();
        var containers = await provider.ListContainersAsync();

        containers.Should().HaveCount(3);
        containers.Select(c => c.Name).Should().Contain("Models", "Statistics", "Recent Requests");
    }

    [Fact]
    public async Task ListContainers_ModelStatus_ShouldReflectAvailability()
    {
        var provider = CreateProvider(completionAvailable: true, embeddingAvailable: false);
        var containers = await provider.ListContainersAsync();

        var modelsContainer = containers.First(c => c.Name == "Models");
        modelsContainer.ItemCount.Should().Be(1); // only completion loaded
        modelsContainer.Properties!["Completion"].Should().Be("Loaded");
        modelsContainer.Properties["Embedding"].Should().Be("Unavailable");
    }

    [Fact]
    public async Task ListContainers_Statistics_ShouldShowCounts()
    {
        var (log, provider) = CreateProviderWithLog();
        log.Add(MakeCompletionEntry(success: true));
        log.Add(MakeEmbeddingEntry(success: true, textCount: 3));

        var containers = await provider.ListContainersAsync();
        var statsContainer = containers.First(c => c.Name == "Statistics");
        statsContainer.Properties!["Completions"].Should().Be("1");
        statsContainer.Properties["Embeddings"].Should().Be("3");
    }

    // ── LLMDebugDataProvider — ListItems ──────────────────────────────────

    [Fact]
    public async Task ListItems_Models_ShouldShowAvailableModels()
    {
        var provider = CreateProvider(completionAvailable: true, embeddingAvailable: true);
        var items = await provider.ListItemsAsync("Models");

        items.Should().HaveCount(2);
        items.Select(i => i.Id).Should().Contain("completion", "embedding");
    }

    [Fact]
    public async Task ListItems_Models_OnlyCompletion()
    {
        var provider = CreateProvider(completionAvailable: true, embeddingAvailable: false);
        var items = await provider.ListItemsAsync("Models");

        items.Should().HaveCount(1);
        items[0].Id.Should().Be("completion");
        items[0].Properties!["Context"].Should().Be("2048");
    }

    [Fact]
    public async Task ListItems_Statistics_ShouldShowStatEntries()
    {
        var (log, provider) = CreateProviderWithLog();
        log.Add(MakeCompletionEntry(success: true, promptTokens: 50, completionTokens: 100));

        var items = await provider.ListItemsAsync("Statistics");
        items.Should().Contain(i => i.Id == "completion-stats");
        items.Should().Contain(i => i.Id == "embedding-stats");

        var completionStats = items.First(i => i.Id == "completion-stats");
        completionStats.Properties!["Prompt Tokens"].Should().Be("50");
        completionStats.Properties["Completion Tokens"].Should().Be("100");
    }

    [Fact]
    public async Task ListItems_Statistics_ShouldShowErrors_WhenPresent()
    {
        var (log, provider) = CreateProviderWithLog();
        log.Add(MakeCompletionEntry(success: false));

        var items = await provider.ListItemsAsync("Statistics");
        items.Should().Contain(i => i.Id == "error-stats");
    }

    [Fact]
    public async Task ListItems_Statistics_ShouldNotShowErrors_WhenNone()
    {
        var (log, provider) = CreateProviderWithLog();
        log.Add(MakeCompletionEntry(success: true));

        var items = await provider.ListItemsAsync("Statistics");
        items.Should().NotContain(i => i.Id == "error-stats");
    }

    [Fact]
    public async Task ListItems_RecentRequests_ShouldReturnNewestFirst()
    {
        var (log, provider) = CreateProviderWithLog();
        log.Add(MakeCompletionEntry(success: true, prompt: "first"));
        log.Add(MakeEmbeddingEntry(success: true, firstText: "second"));
        log.Add(MakeCompletionEntry(success: true, prompt: "third"));

        var items = await provider.ListItemsAsync("Recent Requests");
        items.Should().HaveCount(3);
        // Newest first
        items[0].Properties!["Type"].Should().Be("Completion");
        items[0].Label.Should().Contain("third");
        items[2].Label.Should().Contain("first");
    }

    [Fact]
    public async Task ListItems_RecentRequests_ShouldShowEmbeddingBatchLabel()
    {
        var (log, provider) = CreateProviderWithLog();
        log.Add(MakeEmbeddingEntry(success: true, textCount: 7, type: LLMRequestType.EmbeddingBatch));

        var items = await provider.ListItemsAsync("Recent Requests");
        items.Should().HaveCount(1);
        items[0].Label.Should().Contain("7 texts");
    }

    [Fact]
    public async Task ListItems_UnknownContainer_ReturnsEmpty()
    {
        var provider = CreateProvider();
        var items = await provider.ListItemsAsync("NonExistent");
        items.Should().BeEmpty();
    }

    // ── LLMDebugDataProvider — GetItemDetail ──────────────────────────────

    [Fact]
    public async Task GetItemDetail_CompletionModel_ShouldReturnJson()
    {
        var provider = CreateProvider(completionAvailable: true, completionPath: "/models/test.gguf");
        var detail = await provider.GetItemDetailAsync("Models", "completion");

        detail.Should().NotBeNull();
        detail!.Id.Should().Be("completion");
        detail.ContentJson.Should().Contain("test.gguf");

        var json = JObject.Parse(detail.ContentJson);
        json["Available"]!.Value<bool>().Should().BeTrue();
        json["ContextSize"]!.Value<int>().Should().Be(2048);
    }

    [Fact]
    public async Task GetItemDetail_EmbeddingModel_ShouldReturnJson()
    {
        var provider = CreateProvider(embeddingAvailable: true, embeddingPath: "/models/embed.gguf");
        var detail = await provider.GetItemDetailAsync("Models", "embedding");

        detail.Should().NotBeNull();
        detail!.ContentJson.Should().Contain("embed.gguf");
    }

    [Fact]
    public async Task GetItemDetail_Statistics_ShouldReturnAggregates()
    {
        var (log, provider) = CreateProviderWithLog();
        log.Add(MakeCompletionEntry(success: true, promptTokens: 10, completionTokens: 20));

        var detail = await provider.GetItemDetailAsync("Statistics", "completion-stats");
        detail.Should().NotBeNull();
        detail!.Summary.Should().Contain("1 completions");

        var json = JObject.Parse(detail.ContentJson);
        json["TotalPromptTokens"]!.Value<long>().Should().Be(10);
    }

    [Fact]
    public async Task GetItemDetail_RecentRequest_Completion()
    {
        var (log, provider) = CreateProviderWithLog();
        log.Add(MakeCompletionEntry(success: true, prompt: "Hello AI", response: "Hi there!"));

        var detail = await provider.GetItemDetailAsync("Recent Requests", "req-0");
        detail.Should().NotBeNull();
        detail!.Id.Should().Be("req-0");

        var json = JObject.Parse(detail.ContentJson);
        json["Prompt"]!.Value<string>().Should().Be("Hello AI");
        json["Response"]!.Value<string>().Should().Be("Hi there!");
        json["RequestType"]!.Value<string>().Should().Be("Completion");
    }

    [Fact]
    public async Task GetItemDetail_RecentRequest_Embedding()
    {
        var (log, provider) = CreateProviderWithLog();
        log.Add(MakeEmbeddingEntry(success: true, firstText: "embed this", dimensions: 768));

        var detail = await provider.GetItemDetailAsync("Recent Requests", "req-0");
        detail.Should().NotBeNull();

        var json = JObject.Parse(detail.ContentJson);
        json["FirstText"]!.Value<string>().Should().Be("embed this");
        json["Dimensions"]!.Value<int>().Should().Be(768);
    }

    [Fact]
    public async Task GetItemDetail_RecentRequest_FailedRequest()
    {
        var (log, provider) = CreateProviderWithLog();
        log.Add(MakeCompletionEntry(success: false, errorMessage: "Model unavailable"));

        var detail = await provider.GetItemDetailAsync("Recent Requests", "req-0");
        detail.Should().NotBeNull();
        detail!.Summary.Should().Contain("FAILED");

        var json = JObject.Parse(detail.ContentJson);
        json["Success"]!.Value<bool>().Should().BeFalse();
        json["Error"]!.Value<string>().Should().Be("Model unavailable");
    }

    [Fact]
    public async Task GetItemDetail_InvalidRequestId_ReturnsNull()
    {
        var (log, provider) = CreateProviderWithLog();
        log.Add(MakeCompletionEntry(success: true));

        var detail = await provider.GetItemDetailAsync("Recent Requests", "req-99");
        detail.Should().BeNull();
    }

    [Fact]
    public async Task GetItemDetail_MalformedRequestId_ReturnsNull()
    {
        var provider = CreateProvider();
        var detail = await provider.GetItemDetailAsync("Recent Requests", "not-a-valid-id");
        detail.Should().BeNull();
    }

    [Fact]
    public async Task GetItemDetail_UnknownContainer_ReturnsNull()
    {
        var provider = CreateProvider();
        var detail = await provider.GetItemDetailAsync("NonExistent", "any");
        detail.Should().BeNull();
    }

    // ── LLMModelInfo ──────────────────────────────────────────────────────

    [Fact]
    public void ModelInfo_Defaults()
    {
        var info = new LLMModelInfo();
        info.Available.Should().BeFalse();
        info.Path.Should().BeNull();
        info.FileSizeBytes.Should().Be(-1);
        info.ContextSize.Should().BeNull();
    }

    // ── RequestLog — Additional ───────────────────────────────────────────

    [Fact]
    public void RequestLog_Stats_AvgEmbeddingLatency()
    {
        var log = new LLMRequestLog();
        log.Add(new LLMRequestLogEntry
        {
            RequestType = LLMRequestType.Embedding,
            TimestampUtc = DateTime.UtcNow,
            Duration = TimeSpan.FromMilliseconds(60),
            Success = true,
            TextCount = 1,
            Dimensions = 768,
            FirstTextPreview = "a"
        });
        log.Add(new LLMRequestLogEntry
        {
            RequestType = LLMRequestType.Embedding,
            TimestampUtc = DateTime.UtcNow,
            Duration = TimeSpan.FromMilliseconds(40),
            Success = true,
            TextCount = 1,
            Dimensions = 768,
            FirstTextPreview = "b"
        });

        var stats = log.GetStats();
        stats.AvgEmbeddingLatencyMs.Should().BeApproximately(50, 1);
    }

    [Fact]
    public void RequestLog_StreamingCompletion_CountedAsCompletion()
    {
        var log = new LLMRequestLog();
        log.Add(new LLMRequestLogEntry
        {
            RequestType = LLMRequestType.CompletionStreaming,
            TimestampUtc = DateTime.UtcNow,
            Duration = TimeSpan.FromMilliseconds(100),
            Success = true,
            PromptPreview = "streaming prompt",
            PromptTokenEstimate = 15,
            CompletionTokenEstimate = 25
        });

        var stats = log.GetStats();
        stats.TotalCompletionRequests.Should().Be(1);
        stats.TotalPromptTokens.Should().Be(15);
        stats.TotalCompletionTokens.Should().Be(25);
    }

    [Fact]
    public void RequestLog_NullTokenEstimates_ShouldNotAccumulate()
    {
        var log = new LLMRequestLog();
        log.Add(new LLMRequestLogEntry
        {
            RequestType = LLMRequestType.Completion,
            TimestampUtc = DateTime.UtcNow,
            Duration = TimeSpan.FromMilliseconds(50),
            Success = true,
            PromptTokenEstimate = null,
            CompletionTokenEstimate = null
        });

        var stats = log.GetStats();
        stats.TotalPromptTokens.Should().Be(0);
        stats.TotalCompletionTokens.Should().Be(0);
    }

    [Fact]
    public void RequestLog_EmbeddingBatch_CountsTextCount()
    {
        var log = new LLMRequestLog();
        log.Add(new LLMRequestLogEntry
        {
            RequestType = LLMRequestType.EmbeddingBatch,
            TimestampUtc = DateTime.UtcNow,
            Duration = TimeSpan.FromMilliseconds(200),
            Success = true,
            TextCount = 10,
            Dimensions = 768,
            FirstTextPreview = "batch"
        });

        var stats = log.GetStats();
        stats.TotalEmbeddingRequests.Should().Be(10);
    }

    [Fact]
    public void RequestLog_EmbeddingWithNullTextCount_DefaultsToOne()
    {
        var log = new LLMRequestLog();
        log.Add(new LLMRequestLogEntry
        {
            RequestType = LLMRequestType.Embedding,
            TimestampUtc = DateTime.UtcNow,
            Duration = TimeSpan.FromMilliseconds(30),
            Success = true,
            TextCount = null
        });

        var stats = log.GetStats();
        stats.TotalEmbeddingRequests.Should().Be(1);
    }

    [Fact]
    public void RequestLog_Capacity1_ShouldOnlyKeepLatest()
    {
        var log = new LLMRequestLog(1);
        log.Add(MakeCompletionEntry(success: true, prompt: "old"));
        log.Add(MakeCompletionEntry(success: true, prompt: "new"));

        var entries = log.GetEntries();
        entries.Should().HaveCount(1);
        entries[0].PromptPreview.Should().Be("new");
    }

    // ── Containers — Additional ───────────────────────────────────────────

    [Fact]
    public async Task ListContainers_BothModelsUnavailable_ShouldShowZeroItemCount()
    {
        var provider = CreateProvider(completionAvailable: false, embeddingAvailable: false);
        var containers = await provider.ListContainersAsync();

        var modelsContainer = containers.First(c => c.Name == "Models");
        modelsContainer.ItemCount.Should().Be(0);
        modelsContainer.Properties!["Completion"].Should().Be("Unavailable");
        modelsContainer.Properties["Embedding"].Should().Be("Unavailable");
    }

    [Fact]
    public async Task ListContainers_BothModelsAvailable_ShouldShowTwoItemCount()
    {
        var provider = CreateProvider(completionAvailable: true, embeddingAvailable: true);
        var containers = await provider.ListContainersAsync();

        var modelsContainer = containers.First(c => c.Name == "Models");
        modelsContainer.ItemCount.Should().Be(2);
    }

    [Fact]
    public async Task ListContainers_RecentRequests_ShouldShowBufferSize()
    {
        var (log, provider) = CreateProviderWithLog();
        log.Add(MakeCompletionEntry(success: true));
        log.Add(MakeEmbeddingEntry(success: true));

        var containers = await provider.ListContainersAsync();
        var recentContainer = containers.First(c => c.Name == "Recent Requests");
        recentContainer.ItemCount.Should().Be(2);
        recentContainer.Properties!["Buffer Size"].Should().Contain("2");
    }

    [Fact]
    public async Task ListContainers_Statistics_ShouldShowErrorCount()
    {
        var (log, provider) = CreateProviderWithLog();
        log.Add(MakeCompletionEntry(success: false));
        log.Add(MakeCompletionEntry(success: false));

        var containers = await provider.ListContainersAsync();
        var stats = containers.First(c => c.Name == "Statistics");
        stats.Properties!["Errors"].Should().Be("2");
    }

    // ── ListItems — Additional ────────────────────────────────────────────

    [Fact]
    public async Task ListItems_Models_NoneAvailable_ShouldReturnEmpty()
    {
        var provider = CreateProvider(completionAvailable: false, embeddingAvailable: false);
        var items = await provider.ListItemsAsync("Models");
        items.Should().BeEmpty();
    }

    [Fact]
    public async Task ListItems_Models_OnlyEmbedding()
    {
        var provider = CreateProvider(completionAvailable: false, embeddingAvailable: true);
        var items = await provider.ListItemsAsync("Models");
        items.Should().HaveCount(1);
        items[0].Id.Should().Be("embedding");
    }

    [Fact]
    public async Task ListItems_RecentRequests_StreamingLabel()
    {
        var (log, provider) = CreateProviderWithLog();
        log.Add(new LLMRequestLogEntry
        {
            RequestType = LLMRequestType.CompletionStreaming,
            TimestampUtc = DateTime.UtcNow,
            Duration = TimeSpan.FromMilliseconds(100),
            Success = true,
            PromptPreview = "streaming prompt text",
            FinishReason = "Stop"
        });

        var items = await provider.ListItemsAsync("Recent Requests");
        items.Should().HaveCount(1);
        items[0].Label.Should().Contain("streaming prompt text");
        items[0].Properties!["Type"].Should().Be("CompletionStreaming");
        items[0].Properties["Finish"].Should().Be("Stop");
    }

    [Fact]
    public async Task ListItems_RecentRequests_CompletionProperties_ShowTokens()
    {
        var (log, provider) = CreateProviderWithLog();
        log.Add(MakeCompletionEntry(success: true, promptTokens: 50, completionTokens: 100));

        var items = await provider.ListItemsAsync("Recent Requests");
        items[0].Properties!["Tokens"].Should().Be("50+100");
    }

    [Fact]
    public async Task ListItems_RecentRequests_EmbeddingProperties_ShowDimsAndTexts()
    {
        var (log, provider) = CreateProviderWithLog();
        log.Add(MakeEmbeddingEntry(success: true, textCount: 3, dimensions: 384));

        var items = await provider.ListItemsAsync("Recent Requests");
        items[0].Properties!["Texts"].Should().Be("3");
        items[0].Properties["Dims"].Should().Be("384");
    }

    [Fact]
    public async Task ListItems_RecentRequests_SingleEmbedding_ShowsTextPreview()
    {
        var (log, provider) = CreateProviderWithLog();
        log.Add(MakeEmbeddingEntry(success: true, textCount: 1, firstText: "hello world"));

        var items = await provider.ListItemsAsync("Recent Requests");
        items[0].Label.Should().Contain("hello world");
    }

    [Fact]
    public async Task ListItems_RecentRequests_FailedEntry_ShowsErrorStatus()
    {
        var (log, provider) = CreateProviderWithLog();
        log.Add(MakeCompletionEntry(success: false, errorMessage: "model crashed"));

        var items = await provider.ListItemsAsync("Recent Requests");
        items[0].Properties!["Status"].Should().Be("Error");
    }

    [Fact]
    public async Task ListItems_RecentRequests_MaxItemsLimit()
    {
        var (log, provider) = CreateProviderWithLog();
        for (var i = 0; i < 10; i++)
            log.Add(MakeCompletionEntry(success: true, prompt: $"entry-{i}"));

        var items = await provider.ListItemsAsync("Recent Requests", maxItems: 3);
        items.Should().HaveCount(3);
        // Newest first — entry-9, entry-8, entry-7
        items[0].Label.Should().Contain("entry-9");
        items[2].Label.Should().Contain("entry-7");
    }

    [Fact]
    public async Task ListItems_RecentRequests_EmptyLog_ShouldReturnEmpty()
    {
        var (_, provider) = CreateProviderWithLog();
        var items = await provider.ListItemsAsync("Recent Requests");
        items.Should().BeEmpty();
    }

    // ── GetItemDetail — Additional ────────────────────────────────────────

    [Fact]
    public async Task GetItemDetail_UnavailableModel_ShouldShowNotLoaded()
    {
        var provider = CreateProvider(completionAvailable: false);
        var detail = await provider.GetItemDetailAsync("Models", "completion");

        detail.Should().NotBeNull();
        detail!.Summary.Should().Contain("not loaded");
        var json = JObject.Parse(detail.ContentJson);
        json["Available"]!.Value<bool>().Should().BeFalse();
    }

    [Fact]
    public async Task GetItemDetail_EmbeddingStats_ShouldReturnJson()
    {
        var (log, provider) = CreateProviderWithLog();
        log.Add(MakeEmbeddingEntry(success: true, textCount: 5));

        var detail = await provider.GetItemDetailAsync("Statistics", "embedding-stats");
        detail.Should().NotBeNull();
        detail!.Summary.Should().Contain("5 embeddings");
    }

    [Fact]
    public async Task GetItemDetail_StreamingCompletion_ShouldReturnJson()
    {
        var (log, provider) = CreateProviderWithLog();
        log.Add(new LLMRequestLogEntry
        {
            RequestType = LLMRequestType.CompletionStreaming,
            TimestampUtc = DateTime.UtcNow,
            Duration = TimeSpan.FromMilliseconds(250),
            Success = true,
            PromptPreview = "streaming prompt",
            ResponsePreview = "streamed response",
            FinishReason = "Stop",
            PromptTokenEstimate = 20,
            CompletionTokenEstimate = 50
        });

        var detail = await provider.GetItemDetailAsync("Recent Requests", "req-0");
        detail.Should().NotBeNull();

        var json = JObject.Parse(detail!.ContentJson);
        json["RequestType"]!.Value<string>().Should().Be("CompletionStreaming");
        json["Prompt"]!.Value<string>().Should().Be("streaming prompt");
        json["Response"]!.Value<string>().Should().Be("streamed response");
        json["PromptTokenEstimate"]!.Value<int>().Should().Be(20);
        json["CompletionTokenEstimate"]!.Value<int>().Should().Be(50);
    }

    [Fact]
    public async Task GetItemDetail_RecentRequest_ReverseIndex_WithManyEntries()
    {
        var (log, provider) = CreateProviderWithLog();
        for (var i = 0; i < 5; i++)
            log.Add(MakeCompletionEntry(success: true, prompt: $"msg-{i}"));

        // req-0 = newest = msg-4
        var detail0 = await provider.GetItemDetailAsync("Recent Requests", "req-0");
        detail0.Should().NotBeNull();
        var json0 = JObject.Parse(detail0!.ContentJson);
        json0["Prompt"]!.Value<string>().Should().Be("msg-4");

        // req-4 = oldest = msg-0
        var detail4 = await provider.GetItemDetailAsync("Recent Requests", "req-4");
        detail4.Should().NotBeNull();
        var json4 = JObject.Parse(detail4!.ContentJson);
        json4["Prompt"]!.Value<string>().Should().Be("msg-0");
    }

    [Fact]
    public async Task GetItemDetail_RecentRequest_EmbeddingBatch_ShouldShowTextCount()
    {
        var (log, provider) = CreateProviderWithLog();
        log.Add(MakeEmbeddingEntry(success: true, textCount: 15, type: LLMRequestType.EmbeddingBatch));

        var detail = await provider.GetItemDetailAsync("Recent Requests", "req-0");
        detail.Should().NotBeNull();

        var json = JObject.Parse(detail!.ContentJson);
        json["TextCount"]!.Value<int>().Should().Be(15);
    }

    [Fact]
    public async Task GetItemDetail_RecentRequest_SuccessSummaryFormat()
    {
        var (log, provider) = CreateProviderWithLog();
        log.Add(MakeCompletionEntry(success: true, durationMs: 123));

        var detail = await provider.GetItemDetailAsync("Recent Requests", "req-0");
        detail.Should().NotBeNull();
        detail!.Summary.Should().Contain("Completion");
        detail.Summary.Should().Contain("123ms");
        detail.Summary.Should().NotContain("FAILED");
    }

    [Fact]
    public async Task GetItemDetail_RecentRequest_NegativeIndex_ReturnsNull()
    {
        var (log, provider) = CreateProviderWithLog();
        log.Add(MakeCompletionEntry(success: true));

        var detail = await provider.GetItemDetailAsync("Recent Requests", "req--1");
        detail.Should().BeNull();
    }

    [Fact]
    public async Task GetItemDetail_Models_UnknownItemId_ReturnsEmbeddingModel()
    {
        // When itemId is not "completion", it falls through to _embeddingModel
        var provider = CreateProvider(embeddingAvailable: true, embeddingPath: "/models/embed.gguf");
        var detail = await provider.GetItemDetailAsync("Models", "something-else");
        detail.Should().NotBeNull();
        detail!.ContentJson.Should().Contain("embed.gguf");
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static LLMDebugDataProvider CreateProvider(
        bool completionAvailable = false,
        bool embeddingAvailable = false,
        string? completionPath = "/models/completion.gguf",
        string? embeddingPath = "/models/embedding.gguf")
    {
        var log = new LLMRequestLog();
        return new LLMDebugDataProvider(log,
            new LLMModelInfo
            {
                Available = completionAvailable,
                Path = completionPath,
                FileSizeBytes = completionAvailable ? 139_000_000 : -1,
                ContextSize = 2048
            },
            new LLMModelInfo
            {
                Available = embeddingAvailable,
                Path = embeddingPath,
                FileSizeBytes = embeddingAvailable ? 146_000_000 : -1
            });
    }

    private static (LLMRequestLog log, LLMDebugDataProvider provider) CreateProviderWithLog()
    {
        var log = new LLMRequestLog();
        var provider = new LLMDebugDataProvider(log,
            new LLMModelInfo { Available = true, Path = "/models/completion.gguf", FileSizeBytes = 139_000_000, ContextSize = 2048 },
            new LLMModelInfo { Available = true, Path = "/models/embedding.gguf", FileSizeBytes = 146_000_000 });
        return (log, provider);
    }

    private static LLMRequestLogEntry MakeCompletionEntry(
        bool success,
        string? prompt = null,
        string? response = null,
        string? errorMessage = null,
        int? promptTokens = null,
        int? completionTokens = null,
        double durationMs = 50)
    {
        return new LLMRequestLogEntry
        {
            RequestType = LLMRequestType.Completion,
            TimestampUtc = DateTime.UtcNow,
            Duration = TimeSpan.FromMilliseconds(durationMs),
            Success = success,
            ErrorMessage = errorMessage,
            PromptPreview = prompt,
            ResponsePreview = response,
            FinishReason = success ? "Stop" : null,
            PromptTokenEstimate = promptTokens,
            CompletionTokenEstimate = completionTokens
        };
    }

    private static LLMRequestLogEntry MakeEmbeddingEntry(
        bool success,
        int textCount = 1,
        string? firstText = null,
        int? dimensions = 768,
        LLMRequestType type = LLMRequestType.Embedding)
    {
        return new LLMRequestLogEntry
        {
            RequestType = type,
            TimestampUtc = DateTime.UtcNow,
            Duration = TimeSpan.FromMilliseconds(25),
            Success = success,
            TextCount = textCount,
            Dimensions = dimensions,
            FirstTextPreview = firstText ?? "sample text"
        };
    }
}
