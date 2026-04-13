// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Interfaces.Records;
using CrossCloudKit.Utilities.Common;
using FluentAssertions;
using Moq;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CrossCloudKit.Interfaces.Tests;

public class LLMVectorExtensionsTests
{
    private readonly Mock<ILLMService>    _llm = new();
    private readonly Mock<IVectorService> _vec = new();

    // ── EmbedAndUpsertAsync ──────────────────────────────────────────────────

    [Fact]
    public async Task EmbedAndUpsertAsync_Success_ShouldEmbedThenUpsertWithCorrectPoint()
    {
        var embedding = new float[] { 0.1f, 0.2f, 0.3f };
        var meta = new JObject { ["source"] = "test" };

        _llm.Setup(l => l.CreateEmbeddingAsync("hello", It.IsAny<CancellationToken>()))
            .ReturnsAsync(OperationResult<float[]>.Success(embedding));
        _vec.Setup(v => v.UpsertAsync("col", It.IsAny<VectorPoint>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(OperationResult<bool>.Success(true));

        var result = await _vec.Object.EmbedAndUpsertAsync(_llm.Object, "col", "id1", "hello", meta);

        result.IsSuccessful.Should().BeTrue();
        _vec.Verify(v => v.UpsertAsync("col",
            It.Is<VectorPoint>(p =>
                p.Id == "id1" &&
                p.Vector == embedding &&
                p.Metadata == meta),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task EmbedAndUpsertAsync_NullMetadata_ShouldPassNullMetadataToUpsert()
    {
        _llm.Setup(l => l.CreateEmbeddingAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(OperationResult<float[]>.Success(new float[] { 1f }));
        _vec.Setup(v => v.UpsertAsync(It.IsAny<string>(), It.IsAny<VectorPoint>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(OperationResult<bool>.Success(true));

        var result = await _vec.Object.EmbedAndUpsertAsync(_llm.Object, "col", "id1", "text");

        result.IsSuccessful.Should().BeTrue();
        _vec.Verify(v => v.UpsertAsync("col",
            It.Is<VectorPoint>(p => p.Metadata == null),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task EmbedAndUpsertAsync_EmbeddingFails_ShouldReturnFailureWithoutUpsert()
    {
        _llm.Setup(l => l.CreateEmbeddingAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(OperationResult<float[]>.Failure("embed error", HttpStatusCode.BadGateway));

        var result = await _vec.Object.EmbedAndUpsertAsync(_llm.Object, "col", "id1", "hello");

        result.IsSuccessful.Should().BeFalse();
        result.ErrorMessage.Should().Contain("embed error");
        _vec.Verify(v => v.UpsertAsync(It.IsAny<string>(), It.IsAny<VectorPoint>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task EmbedAndUpsertAsync_UpsertFails_ShouldReturnFailure()
    {
        _llm.Setup(l => l.CreateEmbeddingAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(OperationResult<float[]>.Success(new float[] { 1f }));
        _vec.Setup(v => v.UpsertAsync(It.IsAny<string>(), It.IsAny<VectorPoint>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(OperationResult<bool>.Failure("upsert error", HttpStatusCode.InternalServerError));

        var result = await _vec.Object.EmbedAndUpsertAsync(_llm.Object, "col", "id1", "text");

        result.IsSuccessful.Should().BeFalse();
        result.ErrorMessage.Should().Contain("upsert error");
    }

    [Fact]
    public async Task EmbedAndUpsertAsync_EmbeddingStatusCodePreservedOnFailure()
    {
        _llm.Setup(l => l.CreateEmbeddingAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(OperationResult<float[]>.Failure("gone", HttpStatusCode.ServiceUnavailable));

        var result = await _vec.Object.EmbedAndUpsertAsync(_llm.Object, "col", "id1", "text");

        result.IsSuccessful.Should().BeFalse();
        result.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
    }

    // ── EmbedAndUpsertBatchAsync ─────────────────────────────────────────────

    [Fact]
    public async Task EmbedAndUpsertBatchAsync_Success_ShouldEmbedAllThenUpsertBatchWithCorrectPoints()
    {
        var embeddings = new float[][] { [1f, 2f], [3f, 4f] };
        IReadOnlyList<float[]> embList = embeddings;

        _llm.Setup(l => l.CreateEmbeddingsAsync(It.IsAny<IReadOnlyList<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(OperationResult<IReadOnlyList<float[]>>.Success(embList));
        _vec.Setup(v => v.UpsertBatchAsync("col", It.IsAny<IReadOnlyList<VectorPoint>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(OperationResult<bool>.Success(true));

        var meta0 = new JObject { ["k"] = 1 };
        var items = new List<(string, string, JObject?)>
        {
            ("id1", "text one", meta0),
            ("id2", "text two", null)
        };

        var result = await _vec.Object.EmbedAndUpsertBatchAsync(_llm.Object, "col", items);

        result.IsSuccessful.Should().BeTrue();
        _vec.Verify(v => v.UpsertBatchAsync("col",
            It.Is<IReadOnlyList<VectorPoint>>(pts =>
                pts.Count == 2 &&
                pts[0].Id == "id1" && pts[0].Vector == embeddings[0] && pts[0].Metadata == meta0 &&
                pts[1].Id == "id2" && pts[1].Vector == embeddings[1] && pts[1].Metadata == null),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task EmbedAndUpsertBatchAsync_EmptyItems_ShouldReturnSuccessWithoutCallingServices()
    {
        var items = new List<(string, string, JObject?)>();
        var result = await _vec.Object.EmbedAndUpsertBatchAsync(_llm.Object, "col", items);

        result.IsSuccessful.Should().BeTrue();
        _llm.Verify(l => l.CreateEmbeddingsAsync(It.IsAny<IReadOnlyList<string>>(),
            It.IsAny<CancellationToken>()), Times.Never);
        _vec.Verify(v => v.UpsertBatchAsync(It.IsAny<string>(), It.IsAny<IReadOnlyList<VectorPoint>>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task EmbedAndUpsertBatchAsync_EmbeddingFails_ShouldReturnFailureWithoutUpsert()
    {
        _llm.Setup(l => l.CreateEmbeddingsAsync(It.IsAny<IReadOnlyList<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(OperationResult<IReadOnlyList<float[]>>.Failure("batch fail", HttpStatusCode.BadGateway));

        var items = new List<(string, string, JObject?)> { ("id1", "text1", null) };
        var result = await _vec.Object.EmbedAndUpsertBatchAsync(_llm.Object, "col", items);

        result.IsSuccessful.Should().BeFalse();
        result.ErrorMessage.Should().Contain("batch fail");
        _vec.Verify(v => v.UpsertBatchAsync(It.IsAny<string>(), It.IsAny<IReadOnlyList<VectorPoint>>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task EmbedAndUpsertBatchAsync_SendsCorrectTextsToEmbedder()
    {
        IReadOnlyList<float[]> embeddings = [new float[] { 1f }, new float[] { 2f }];
        _llm.Setup(l => l.CreateEmbeddingsAsync(It.IsAny<IReadOnlyList<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(OperationResult<IReadOnlyList<float[]>>.Success(embeddings));
        _vec.Setup(v => v.UpsertBatchAsync(It.IsAny<string>(), It.IsAny<IReadOnlyList<VectorPoint>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(OperationResult<bool>.Success(true));

        var items = new List<(string, string, JObject?)>
        {
            ("a", "first text",  null),
            ("b", "second text", null)
        };
        await _vec.Object.EmbedAndUpsertBatchAsync(_llm.Object, "col", items);

        _llm.Verify(l => l.CreateEmbeddingsAsync(
            It.Is<IReadOnlyList<string>>(texts =>
                texts.Count == 2 && texts[0] == "first text" && texts[1] == "second text"),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task EmbedAndUpsertBatchAsync_FewerEmbeddingsThanItems_ShouldReturnFailureNotException()
    {
        // Simulate an API bug where only 1 embedding is returned for a 2-item batch.
        IReadOnlyList<float[]> tooFew = [new float[] { 1f }];
        _llm.Setup(l => l.CreateEmbeddingsAsync(It.IsAny<IReadOnlyList<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(OperationResult<IReadOnlyList<float[]>>.Success(tooFew));

        var items = new List<(string, string, JObject?)>
        {
            ("id1", "text1", null),
            ("id2", "text2", null)   // second item has no corresponding embedding
        };

        var result = await _vec.Object.EmbedAndUpsertBatchAsync(_llm.Object, "col", items);

        result.IsSuccessful.Should().BeFalse(
            because: "receiving fewer embeddings than requested items is a protocol error and must not be swallowed silently");
        result.ErrorMessage.Should().Contain("mismatch",
            because: "the error message should describe the count mismatch");
        _vec.Verify(v => v.UpsertBatchAsync(It.IsAny<string>(), It.IsAny<IReadOnlyList<VectorPoint>>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task EmbedAndUpsertBatchAsync_MoreEmbeddingsThanItems_ShouldReturnFailureNotException()
    {
        // Simulate an API returning more embeddings than inputs (deduplication/other anomaly).
        IReadOnlyList<float[]> tooMany = [new float[] { 1f }, new float[] { 2f }, new float[] { 3f }];
        _llm.Setup(l => l.CreateEmbeddingsAsync(It.IsAny<IReadOnlyList<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(OperationResult<IReadOnlyList<float[]>>.Success(tooMany));

        var items = new List<(string, string, JObject?)>
        {
            ("id1", "text1", null),
            ("id2", "text2", null)
        };

        var result = await _vec.Object.EmbedAndUpsertBatchAsync(_llm.Object, "col", items);

        result.IsSuccessful.Should().BeFalse(
            because: "receiving more embeddings than requested items is equally unexpected and must be treated as a protocol error");
    }

    // ── SemanticSearchAsync ──────────────────────────────────────────────────

    [Fact]
    public async Task SemanticSearchAsync_Success_ShouldEmbedQueryThenReturnResults()
    {
        var queryVec = new float[] { 0.5f, 0.5f };
        _llm.Setup(l => l.CreateEmbeddingAsync("find me", It.IsAny<CancellationToken>()))
            .ReturnsAsync(OperationResult<float[]>.Success(queryVec));

        IReadOnlyList<VectorSearchResult> searchResults =
        [
            new VectorSearchResult { Id = "r1", Score = 0.9f },
            new VectorSearchResult { Id = "r2", Score = 0.7f }
        ];
        _vec.Setup(v => v.QueryAsync("col", queryVec, 5, null, true, It.IsAny<CancellationToken>()))
            .ReturnsAsync(OperationResult<IReadOnlyList<VectorSearchResult>>.Success(searchResults));

        var result = await _vec.Object.SemanticSearchAsync(_llm.Object, "col", "find me", 5);

        result.IsSuccessful.Should().BeTrue();
        result.Data.Should().HaveCount(2);
        result.Data[0].Id.Should().Be("r1");
        result.Data[1].Id.Should().Be("r2");
    }

    [Fact]
    public async Task SemanticSearchAsync_EmbeddingFails_ShouldReturnFailureWithoutQuerying()
    {
        _llm.Setup(l => l.CreateEmbeddingAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(OperationResult<float[]>.Failure("no embed", HttpStatusCode.ServiceUnavailable));

        var result = await _vec.Object.SemanticSearchAsync(_llm.Object, "col", "query", 5);

        result.IsSuccessful.Should().BeFalse();
        result.ErrorMessage.Should().Contain("no embed");
        result.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
        _vec.Verify(v => v.QueryAsync(It.IsAny<string>(), It.IsAny<float[]>(), It.IsAny<int>(),
            It.IsAny<ConditionCoupling?>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task SemanticSearchAsync_WithFilter_ShouldPassFilterAndFlagsToQuery()
    {
        var queryVec = new float[] { 1f };
        _llm.Setup(l => l.CreateEmbeddingAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(OperationResult<float[]>.Success(queryVec));
        _vec.Setup(v => v.QueryAsync(It.IsAny<string>(), It.IsAny<float[]>(), It.IsAny<int>(),
                It.IsAny<ConditionCoupling?>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(OperationResult<IReadOnlyList<VectorSearchResult>>.Success(
                Array.Empty<VectorSearchResult>()));

        var filter = new ConditionCoupling(); // Empty filter
        var result = await _vec.Object.SemanticSearchAsync(
            _llm.Object, "col", "q", 3, filter, includeMetadata: false);

        result.IsSuccessful.Should().BeTrue();
        _vec.Verify(v => v.QueryAsync("col", queryVec, 3, filter, false,
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task SemanticSearchAsync_QueryFails_ShouldReturnFailure()
    {
        _llm.Setup(l => l.CreateEmbeddingAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(OperationResult<float[]>.Success(new float[] { 1f }));
        _vec.Setup(v => v.QueryAsync(It.IsAny<string>(), It.IsAny<float[]>(), It.IsAny<int>(),
                It.IsAny<ConditionCoupling?>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(OperationResult<IReadOnlyList<VectorSearchResult>>.Failure(
                "search fail", HttpStatusCode.InternalServerError));

        var result = await _vec.Object.SemanticSearchAsync(_llm.Object, "col", "q", 5);

        result.IsSuccessful.Should().BeFalse();
        result.ErrorMessage.Should().Contain("search fail");
    }

    [Fact]
    public async Task SemanticSearchAsync_DefaultIncludeMetadataIsTrue()
    {
        _llm.Setup(l => l.CreateEmbeddingAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(OperationResult<float[]>.Success(new float[] { 1f }));
        _vec.Setup(v => v.QueryAsync(It.IsAny<string>(), It.IsAny<float[]>(), It.IsAny<int>(),
                It.IsAny<ConditionCoupling?>(), true, It.IsAny<CancellationToken>()))
            .ReturnsAsync(OperationResult<IReadOnlyList<VectorSearchResult>>.Success(
                Array.Empty<VectorSearchResult>()));

        await _vec.Object.SemanticSearchAsync(_llm.Object, "col", "q", 5);

        // Verify includeMetadata=true was passed (only the true-variant mock will match)
        _vec.Verify(v => v.QueryAsync(It.IsAny<string>(), It.IsAny<float[]>(), It.IsAny<int>(),
            It.IsAny<ConditionCoupling?>(), true, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task SemanticSearchAsync_CancellationTokenPropagated()
    {
        var cts = new CancellationTokenSource();
        var token = cts.Token;
        cts.Cancel();

        _llm.Setup(l => l.CreateEmbeddingAsync(It.IsAny<string>(), token))
            .ReturnsAsync(OperationResult<float[]>.Failure("cancelled", HttpStatusCode.RequestTimeout));

        var result = await _vec.Object.SemanticSearchAsync(
            _llm.Object, "col", "q", 5, cancellationToken: token);

        result.IsSuccessful.Should().BeFalse();
        // QueryAsync must never be called when embedding fails (e.g. due to cancellation)
        _vec.Verify(v => v.QueryAsync(It.IsAny<string>(), It.IsAny<float[]>(), It.IsAny<int>(),
            It.IsAny<ConditionCoupling?>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
    }
}
