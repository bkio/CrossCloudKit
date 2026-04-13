// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;
using System.Text;
using CrossCloudKit.Interfaces.Records;
using CrossCloudKit.Interfaces.Enums;
using CrossCloudKit.LLM.OpenAI;
using FluentAssertions;
using Xunit;

namespace CrossCloudKit.LLM.OpenAI.Tests;

/// <summary>
/// Unit tests for <see cref="LLMServiceOpenAI"/> that do NOT require a real LLM endpoint.
/// A fake <see cref="HttpMessageHandler"/> is injected so SSE edge cases can be triggered
/// deterministically.
/// </summary>
public class LLMServiceOpenAIUnitTests
{
    // ── helpers ───────────────────────────────────────────────────────────────

    private static LLMServiceOpenAI CreateWithHandler(HttpMessageHandler handler)
    {
        var client = new HttpClient(handler) { BaseAddress = new Uri("http://localhost/v1/") };
        return new LLMServiceOpenAI(client, defaultModel: "test-model");
    }

    private static FakeHandler SseHandler(string sseBody, HttpStatusCode status = HttpStatusCode.OK)
        => new(sseBody, status, "text/event-stream");

    private static FakeHandler JsonHandler(string json, HttpStatusCode status = HttpStatusCode.OK)
        => new(json, status, "application/json");

    private static LLMRequest SimpleRequest() => new()
    {
        Messages = [new LLMMessage { Role = LLMRole.User, Content = "hi" }],
        MaxTokens = 8
    };

    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Regression test for: stream continues after parse-error failure chunk.
    /// After the fix, one failure chunk is yielded then the stream ends.
    /// </summary>
    [Fact]
    public async Task CompleteStreamingAsync_MalformedJsonChunk_ShouldYieldFailureThenEnd()
    {
        // Malformed: "NOTJSON" is not valid JSON — triggers the parse-error catch.
        const string sse = "data: NOTJSON\n\n";
        await using var service = CreateWithHandler(SseHandler(sse));

        var chunks = new List<bool>(); // true=success, false=failure
        await foreach (var cr in service.CompleteStreamingAsync(SimpleRequest()))
            chunks.Add(cr.IsSuccessful);

        chunks.Should().ContainSingle(because: "exactly one chunk should be emitted (the failure) before the stream terminates");
        chunks[0].Should().BeFalse(because: "the only chunk should be a failure wrapping the parse error");
    }

    /// <summary>
    /// Regression test for: <c>choices[]</c> empty array causes
    /// <c>ArgumentOutOfRangeException</c> in <c>ParseStreamChunk</c> before the fix.
    /// After the fix the empty-choices chunk is a no-op success (empty delta, IsFinal=false)
    /// and the stream proceeds normally to the [DONE] sentinel.
    /// </summary>
    [Fact]
    public async Task CompleteStreamingAsync_EmptyChoicesArray_ShouldNotThrowAndShouldHandleChunk()
    {
        // Some providers send {"choices":[],"usage":{...}} as a final usage-only chunk.
        const string sse = "data: {\"choices\":[]}\n\ndata: [DONE]\n\n";
        await using var service = CreateWithHandler(SseHandler(sse));

        // Must not throw — implementation must handle this without an unhandled exception.
        var chunks = new List<(bool IsSuccess, bool IsFinal, string Delta)>();
        Func<Task> act = async () =>
        {
            await foreach (var cr in service.CompleteStreamingAsync(SimpleRequest()))
            {
                chunks.Add((cr.IsSuccessful, cr.IsSuccessful && cr.Data.IsFinal,
                            cr.IsSuccessful ? cr.Data.ContentDelta : "<error>"));
            }
        };

        await act.Should().NotThrowAsync(
            because: "an empty choices array in an SSE chunk must not propagate an unhandled exception");

        // The empty-choices chunk becomes a no-op success (empty delta, not final),
        // then [DONE] produces the final success chunk — two chunks total, all successful.
        chunks.Should().HaveCount(2,
            because: "one empty-choices no-op chunk plus one [DONE] final chunk are expected");
        chunks.Should().AllSatisfy(c => c.IsSuccess.Should().BeTrue(
            because: "empty choices array must not produce a failure chunk after the fix"));
        chunks[0].IsFinal.Should().BeFalse(because: "first chunk is the no-op empty-choices chunk");
        chunks[0].Delta.Should().BeEmpty(because: "empty choices produces an empty content delta");
        chunks[1].IsFinal.Should().BeTrue(because: "second chunk is the [DONE] final sentinel");
    }

    /// <summary>
    /// Verifies that a normal streaming response delivers content chunks followed by IsFinal=true,
    /// with no failure chunk interspersed.
    /// </summary>
    [Fact]
    public async Task CompleteStreamingAsync_ValidStream_ShouldDeliverContentThenFinal()
    {
        const string sse =
            "data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"}}]}\n\n" +
            "data: {\"choices\":[{\"delta\":{\"content\":\" world\"}}]}\n\n" +
            "data: {\"choices\":[{\"delta\":{\"content\":\"\"},\"finish_reason\":\"stop\"}]}\n\n" +
            "data: [DONE]\n\n";

        await using var service = CreateWithHandler(SseHandler(sse));

        var allSuccessful = true;
        bool gotFinal = false;
        var content = new StringBuilder();

        await foreach (var cr in service.CompleteStreamingAsync(SimpleRequest()))
        {
            if (!cr.IsSuccessful) { allSuccessful = false; break; }
            if (cr.Data.IsFinal) { gotFinal = true; break; }
            content.Append(cr.Data.ContentDelta);
        }

        allSuccessful.Should().BeTrue(because: "all chunks in a valid stream should be successful");
        gotFinal.Should().BeTrue(because: "stream must terminate with an IsFinal chunk");
        content.ToString().Should().Be("Hello world",
            because: "content deltas should be concatenated in order");
    }

    /// <summary>
    /// Verifies that after a parse-error failure chunk, no subsequent successful chunks
    /// are emitted by the iterator—even if the server sends more data.
    /// </summary>
    [Fact]
    public async Task CompleteStreamingAsync_FailureChunkIsAlwaysLastChunk()
    {
        // Bad JSON first, then a valid chunk — the stream must stop after the failure.
        const string sse =
            "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n" +  // valid
            "data: INVALID\n\n" +                                              // bad JSON → failure
            "data: {\"choices\":[{\"delta\":{\"content\":\"should-not-arrive\"}}]}\n\n";  // should not be seen

        await using var service = CreateWithHandler(SseHandler(sse));

        var results = new List<(bool Success, string Delta)>();
        await foreach (var cr in service.CompleteStreamingAsync(SimpleRequest()))
        {
            results.Add((cr.IsSuccessful, cr.IsSuccessful ? cr.Data.ContentDelta : "<error>"));
        }

        results.Should().HaveCount(2,
            because: "one valid chunk then one failure chunk — the third chunk must be suppressed");
        results[0].Success.Should().BeTrue();
        results[0].Delta.Should().Be("ok");
        results[1].Success.Should().BeFalse();
    }

    /// <summary>
    /// Verifies HTTP error responses are surfaced as a single failure chunk.
    /// </summary>
    [Fact]
    public async Task CompleteStreamingAsync_HttpErrorResponse_ShouldYieldSingleFailureChunk()
    {
        await using var service = CreateWithHandler(SseHandler("{\"error\":\"bad request\"}", HttpStatusCode.BadRequest));

        var chunks = new List<bool>();
        await foreach (var cr in service.CompleteStreamingAsync(SimpleRequest()))
            chunks.Add(cr.IsSuccessful);

        chunks.Should().ContainSingle(because: "an HTTP error should produce exactly one failure chunk");
        chunks[0].Should().BeFalse();
    }

    /// <summary>
    /// Verifies that <see cref="LLMServiceOpenAI.CompleteAsync"/> surfaces HTTP errors as failures,
    /// not exceptions.
    /// </summary>
    [Fact]
    public async Task CompleteAsync_HttpErrorResponse_ShouldReturnFailureNotException()
    {
        await using var service = CreateWithHandler(
            JsonHandler("{\"error\":\"not found\"}", HttpStatusCode.NotFound));

        var result = await service.CompleteAsync(SimpleRequest());

        result.IsSuccessful.Should().BeFalse();
        result.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    /// <summary>
    /// Verifies that <see cref="LLMServiceOpenAI.CompleteAsync"/> returns a failure (not exception)
    /// when the response body cannot be parsed as valid JSON.
    /// </summary>
    [Fact]
    public async Task CompleteAsync_MalformedJsonResponse_ShouldReturnFailureNotException()
    {
        await using var service = CreateWithHandler(JsonHandler("NOT_JSON"));

        var result = await service.CompleteAsync(SimpleRequest());

        result.IsSuccessful.Should().BeFalse(
            because: "a response that is not valid JSON must be surfaced as a failure, not an exception");
    }

    /// <summary>
    /// Verifies that <see cref="LLMServiceOpenAI.CreateEmbeddingsAsync"/> returns a failure
    /// when the HTTP response is an error.
    /// </summary>
    [Fact]
    public async Task CreateEmbeddingsAsync_HttpErrorResponse_ShouldReturnFailureNotException()
    {
        await using var service = CreateWithHandler(
            JsonHandler("{\"error\":\"unauthorised\"}", HttpStatusCode.Unauthorized));

        var result = await service.CreateEmbeddingsAsync(["hello"]);

        result.IsSuccessful.Should().BeFalse();
        result.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    /// <summary>
    /// Verifies that an empty-choices response in non-streaming mode is handled as a failure.
    /// </summary>
    [Fact]
    public async Task CompleteAsync_EmptyChoicesResponse_ShouldReturnFailureNotException()
    {
        await using var service = CreateWithHandler(JsonHandler("{\"choices\":[]}"));

        var result = await service.CompleteAsync(SimpleRequest());

        result.IsSuccessful.Should().BeFalse(
            because: "a response with an empty choices array should not produce an exception");
    }

    // ── Fake handler ──────────────────────────────────────────────────────────

    private sealed class FakeHandler(string body, HttpStatusCode status, string contentType)
        : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var response = new HttpResponseMessage(status)
            {
                Content = new StringContent(body, Encoding.UTF8, contentType)
            };
            return Task.FromResult(response);
        }
    }
}
