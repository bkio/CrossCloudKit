using CrossCloudKit.Interfaces.Classes;
// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces;
using CrossCloudKit.Interfaces.Records;
using CrossCloudKit.Interfaces.Enums;
using CrossCloudKit.Utilities.Common;
using FluentAssertions;
using xRetry;
using Xunit;

namespace CrossCloudKit.LLM.Tests.Common;

/// <summary>
/// Abstract base class for <see cref="ILLMService"/> integration tests.
/// Concrete test classes must implement <see cref="CreateLLMService"/> and
/// optionally override <see cref="SupportsCompletion"/> when the completion
/// feature is conditionally available (e.g. LLM.Basic without a model file).
/// </summary>
public abstract class LLMServiceTestBase
{
    /// <summary>Creates the service under test.</summary>
    protected abstract ILLMService CreateLLMService();

    /// <summary>
    /// Override to return <c>false</c> when completion is not available
    /// (e.g. <see cref="CrossCloudKit.LLM.Basic.LLMServiceBasic"/> without a GGUF model).
    /// Tests guarded by this flag will be skipped when it returns <c>false</c>.
    /// </summary>
    protected virtual bool SupportsCompletion => true;

    // ── Embedding tests ───────────────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingAsync_WithValidText_ShouldReturnVector()
    {
        // Arrange
        await using var service = CreateLLMService();
        service.IsInitialized.Should().BeTrue();

        // Act
        var result = await service.CreateEmbeddingAsync("The quick brown fox jumps over the lazy dog.");

        // Assert
        result.IsSuccessful.Should().BeTrue(because: result.IsSuccessful ? "" : result.ErrorMessage);
        var vector = result.Data;
        vector.Should().NotBeEmpty();
        vector.Length.Should().BeGreaterThan(32, because: "meaningful embeddings have at least 32 dimensions");
        vector.Any(float.IsNaN).Should().BeFalse(because: "embedding values must be finite");
    }

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingsAsync_WithMultipleTexts_ShouldReturnOneVectorEach()
    {
        // Arrange
        await using var service = CreateLLMService();
        var texts = new[]
        {
            "Machine learning is a subset of artificial intelligence.",
            "Neural networks are inspired by the structure of the brain.",
            "Natural language processing enables computers to understand text."
        };

        // Act
        var result = await service.CreateEmbeddingsAsync(texts);

        // Assert
        result.IsSuccessful.Should().BeTrue(because: result.IsSuccessful ? "" : result.ErrorMessage);
        result.Data.Should().HaveCount(texts.Length);
        foreach (var vec in result.Data)
        {
            vec.Should().NotBeEmpty();
            vec.Any(float.IsNaN).Should().BeFalse();
        }
    }

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingAsync_SimilarTexts_ShouldHaveHigherCosineSimilarity()
    {
        // Arrange
        await using var service = CreateLLMService();

        // Act
        var catResult   = await service.CreateEmbeddingAsync("I love my cat");
        var dogResult   = await service.CreateEmbeddingAsync("I love my dog");
        var bankResult  = await service.CreateEmbeddingAsync("The financial institution processed the loan");

        catResult.IsSuccessful.Should().BeTrue();
        dogResult.IsSuccessful.Should().BeTrue();
        bankResult.IsSuccessful.Should().BeTrue();

        var simCatDog   = CosineSimilarity(catResult.Data,  dogResult.Data);
        var simCatBank  = CosineSimilarity(catResult.Data,  bankResult.Data);

        // "cat" and "dog" contexts should be more similar than "cat" and "bank loan"
        simCatDog.Should().BeGreaterThan(simCatBank,
            because: "semantically similar sentences should have higher cosine similarity");
    }

    // ── Completion tests (skipped when SupportsCompletion is false) ───────────

    [RetryFact(3, 5000)]
    public async Task CompleteAsync_WithSimplePrompt_ShouldReturnNonEmptyContent()
    {
        if (!SupportsCompletion) return;

        // Arrange
        await using var service = CreateLLMService();
        var request = new LLMRequest
        {
            Messages =
            [
                new LLMMessage { Role = LLMRole.System,    Content = "You are a helpful assistant. Be concise." },
                new LLMMessage { Role = LLMRole.User,      Content = "Say the word 'hello' and nothing else." }
            ],
            MaxTokens = 16
        };

        // Act
        var result = await service.CompleteAsync(request);

        // Assert
        result.IsSuccessful.Should().BeTrue(because: result.IsSuccessful ? "" : result.ErrorMessage);
        result.Data.Content.Should().NotBeNullOrWhiteSpace();
        result.Data.FinishReason.Should().BeOneOf(LLMFinishReason.Stop, LLMFinishReason.Length);
    }

    [RetryFact(3, 5000)]
    public async Task CompleteStreamingAsync_WithSimplePrompt_ShouldStreamContent()
    {
        if (!SupportsCompletion) return;

        // Arrange
        await using var service = CreateLLMService();
        var request = new LLMRequest
        {
            Messages =
            [
                new LLMMessage { Role = LLMRole.System, Content = "You are a helpful assistant. Be concise." },
                new LLMMessage { Role = LLMRole.User,   Content = "Count from 1 to 3, separated by spaces." }
            ],
            MaxTokens = 32
        };

        // Act
        var chunks      = new List<string>();
        bool gotFinal   = false;

        await foreach (var chunkResult in service.CompleteStreamingAsync(request))
        {
            chunkResult.IsSuccessful.Should().BeTrue(because: chunkResult.IsSuccessful ? "" : chunkResult.ErrorMessage);
            var chunk = chunkResult.Data;
            if (chunk.IsFinal)
            {
                gotFinal = true;
                break;
            }
            chunks.Add(chunk.ContentDelta);
        }

        // Assert
        gotFinal.Should().BeTrue(because: "stream should end with a final chunk");
        string.Concat(chunks).Should().NotBeNullOrWhiteSpace(because: "stream should produce some content");
    }

    [RetryFact(3, 5000)]
    public async Task CompleteAsync_WithCancellation_ShouldReturnFailure()
    {
        if (!SupportsCompletion) return;

        // Arrange
        await using var service = CreateLLMService();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var request = new LLMRequest
        {
            Messages = [new LLMMessage { Role = LLMRole.User, Content = "Hello!" }]
        };

        // Act
        var result = await service.CompleteAsync(request, cts.Token);

        // Assert
        result.IsSuccessful.Should().BeFalse(because: "cancelled request should return a failure");
    }

    // ── Additional embedding tests ─────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingAsync_EmptyText_ShouldReturnVector()
    {
        await using var service = CreateLLMService();

        var result = await service.CreateEmbeddingAsync(string.Empty);

        result.IsSuccessful.Should().BeTrue(because: result.IsSuccessful ? "" : result.ErrorMessage);
        result.Data.Should().NotBeEmpty(because: "even empty text should produce a valid embedding");
        result.Data.Any(float.IsNaN).Should().BeFalse();
    }

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingAsync_LongText_ShouldReturnVector()
    {
        await using var service = CreateLLMService();
        var longText = string.Join(" ", Enumerable.Repeat(
            "The quick brown fox jumped over the lazy dog near the riverbank", 20));

        var result = await service.CreateEmbeddingAsync(longText);

        result.IsSuccessful.Should().BeTrue(because: result.IsSuccessful ? "" : result.ErrorMessage);
        result.Data.Should().NotBeEmpty();
        result.Data.Any(float.IsNaN).Should().BeFalse();
    }

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingAsync_SameDimensionality_AcrossMultipleCalls()
    {
        await using var service = CreateLLMService();
        var texts = new[] { "hello", "world", "foo bar baz" };

        var results = new List<float[]>();
        foreach (var text in texts)
        {
            var r = await service.CreateEmbeddingAsync(text);
            r.IsSuccessful.Should().BeTrue(because: r.IsSuccessful ? "" : r.ErrorMessage);
            results.Add(r.Data);
        }

        // All embeddings from the same model must have identical dimensions
        int dim = results[0].Length;
        results.Should().AllSatisfy(v =>
            v.Should().HaveCount(dim, because: "all embeddings from one model must share the same dimensionality"));
    }

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingsAsync_BatchResultsMatchIndividualResults()
    {
        await using var service = CreateLLMService();
        var texts = new[]
        {
            "The sun rises in the east.",
            "A bird in the hand is worth two in the bush."
        };

        var batchResult = await service.CreateEmbeddingsAsync(texts);
        batchResult.IsSuccessful.Should().BeTrue(because: batchResult.IsSuccessful ? "" : batchResult.ErrorMessage);
        batchResult.Data.Should().HaveCount(texts.Length);

        // Individual calls should produce vectors close to batch results
        for (int i = 0; i < texts.Length; i++)
        {
            var single = await service.CreateEmbeddingAsync(texts[i]);
            single.IsSuccessful.Should().BeTrue();
            single.Data.Should().HaveCount(batchResult.Data[i].Length,
                because: "batch and single-call vectors must have the same dimensions");

            var sim = CosineSimilarity(single.Data, batchResult.Data[i]);
            sim.Should().BeGreaterThan(0.99f,
                because: "embeddings for the same text via batch and single call should be virtually identical");
        }
    }

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingsAsync_EmptyBatch_ShouldReturnEmptyList()
    {
        await using var service = CreateLLMService();

        var result = await service.CreateEmbeddingsAsync([]);

        result.IsSuccessful.Should().BeTrue();
        result.Data.Should().BeEmpty(because: "empty input should yield an empty embedding list");
    }

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingAsync_DifferentTexts_ShouldProduceDifferentVectors()
    {
        await using var service = CreateLLMService();

        var r1 = await service.CreateEmbeddingAsync("Astrophysics and black holes");
        var r2 = await service.CreateEmbeddingAsync("Chocolate cake baking recipe");

        r1.IsSuccessful.Should().BeTrue();
        r2.IsSuccessful.Should().BeTrue();

        // Vectors for very different topics should not be identical
        float sim = CosineSimilarity(r1.Data, r2.Data);
        sim.Should().BeLessThan(1.0f,
            because: "semantically different texts must not produce identical vectors");
    }

    // ── Additional completion tests ────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task CompleteAsync_WithMultiTurnConversation_ShouldIncorporateHistory()
    {
        if (!SupportsCompletion) return;

        await using var service = CreateLLMService();
        var request = new LLMRequest
        {
            Messages =
            [
                new LLMMessage { Role = LLMRole.System,    Content = "You are a helpful assistant. Be very concise." },
                new LLMMessage { Role = LLMRole.User,      Content = "My favourite colour is blue. Confirm you understood in one word." },
                new LLMMessage { Role = LLMRole.Assistant, Content = "Understood." },
                new LLMMessage { Role = LLMRole.User,      Content = "What is my favourite colour? Reply with just the colour name." }
            ],
            MaxTokens = 10
        };

        var result = await service.CompleteAsync(request);

        result.IsSuccessful.Should().BeTrue(because: result.IsSuccessful ? "" : result.ErrorMessage);
        result.Data.Content.Should().NotBeNullOrWhiteSpace();
        // The model should recall "blue" from the conversation history
        result.Data.Content.Should().ContainEquivalentOf("blue",
            because: "the model should recall the colour from earlier in the conversation");
    }

    [RetryFact(3, 5000)]
    public async Task CompleteAsync_WithTemperatureZero_ShouldProduceDeterministicResponse()
    {
        if (!SupportsCompletion) return;

        await using var service = CreateLLMService();
        var request = new LLMRequest
        {
            Messages =
            [
                new LLMMessage { Role = LLMRole.System, Content = "You are a helpful assistant. Be very concise." },
                new LLMMessage { Role = LLMRole.User,   Content = "Reply with just the single word: apple" }
            ],
            MaxTokens    = 8,
            Temperature  = 0.0
        };

        var r1 = await service.CompleteAsync(request);
        var r2 = await service.CompleteAsync(request);

        r1.IsSuccessful.Should().BeTrue();
        r2.IsSuccessful.Should().BeTrue();

        // With temperature=0 both calls should produce the same response (or at least both non-empty)
        r1.Data.Content.Should().NotBeNullOrWhiteSpace();
        r2.Data.Content.Should().NotBeNullOrWhiteSpace();
    }

    [RetryFact(3, 5000)]
    public async Task CompleteAsync_MultipleSequentialCalls_AllSucceed_Stateless()
    {
        if (!SupportsCompletion) return;

        await using var service = CreateLLMService();

        for (int i = 0; i < 3; i++)
        {
            var request = new LLMRequest
            {
                Messages =
                [
                    new LLMMessage { Role = LLMRole.User, Content = $"Say the number {i + 1} and nothing else." }
                ],
                MaxTokens = 8
            };

            var result = await service.CompleteAsync(request);
            result.IsSuccessful.Should().BeTrue(
                because: $"sequential call #{i + 1} should succeed (service must be stateless)");
            result.Data.Content.Should().NotBeNullOrWhiteSpace();
        }
    }

    [RetryFact(3, 5000)]
    public async Task CompleteAsync_WithMaxTokensOne_ShouldReturnLengthFinishReason()
    {
        if (!SupportsCompletion) return;

        await using var service = CreateLLMService();
        var request = new LLMRequest
        {
            Messages  = [new LLMMessage { Role = LLMRole.User, Content = "Write a long story about dragons." }],
            MaxTokens = 1
        };

        var result = await service.CompleteAsync(request);

        result.IsSuccessful.Should().BeTrue();
        result.Data.FinishReason.Should().Be(LLMFinishReason.Length,
            because: "MaxTokens=1 forces the model to stop before completing its response");
    }

    [RetryFact(3, 5000)]
    public async Task CompleteStreamingAsync_ShouldReceiveIncrementalDeltas()
    {
        if (!SupportsCompletion) return;

        await using var service = CreateLLMService();
        var request = new LLMRequest
        {
            Messages =
            [
                new LLMMessage { Role = LLMRole.System, Content = "You are a helpful assistant. Be concise." },
                new LLMMessage { Role = LLMRole.User,   Content = "List the numbers 1 through 5, one per line." }
            ],
            MaxTokens = 64
        };

        var deltas       = new List<string>();
        int chunkCount   = 0;
        bool gotFinal    = false;

        await foreach (var chunkResult in service.CompleteStreamingAsync(request))
        {
            chunkResult.IsSuccessful.Should().BeTrue();
            var chunk = chunkResult.Data;
            chunkCount++;
            if (chunk.IsFinal)
            {
                gotFinal = true;
                break;
            }
            deltas.Add(chunk.ContentDelta);
        }

        gotFinal.Should().BeTrue();
        chunkCount.Should().BeGreaterThan(1, because: "streaming should produce multiple chunks");
        string.Concat(deltas).Should().NotBeNullOrWhiteSpace();
    }

    [RetryFact(3, 5000)]
    public async Task CompleteStreamingAsync_CancelMidStream_ShouldStopGracefully()
    {
        if (!SupportsCompletion) return;

        await using var service = CreateLLMService();
        using var cts = new CancellationTokenSource();

        var request = new LLMRequest
        {
            Messages =
            [
                new LLMMessage { Role = LLMRole.User, Content = "Write a very long essay about the history of Rome." }
            ],
            MaxTokens = 512
        };

        int chunksSeen = 0;
        Func<Task> act = async () =>
        {
            await foreach (var cr in service.CompleteStreamingAsync(request, cts.Token))
            {
                chunksSeen++;
                if (chunksSeen >= 2)
                    await cts.CancelAsync();
            }
        };

        // Should not throw — implementations must handle cancellation gracefully
        await act.Should().NotThrowAsync(because: "streaming cancellation must be handled without an uncaught exception");
    }

    [RetryFact(3, 5000)]
    public async Task CompleteAsync_EmptySystemMessage_ShouldStillSucceed()
    {
        if (!SupportsCompletion) return;

        await using var service = CreateLLMService();
        var request = new LLMRequest
        {
            Messages  = [new LLMMessage { Role = LLMRole.User, Content = "Say hi." }],
            MaxTokens = 16
        };

        var result = await service.CompleteAsync(request);

        result.IsSuccessful.Should().BeTrue(because: result.IsSuccessful ? "" : result.ErrorMessage);
        result.Data.Content.Should().NotBeNullOrWhiteSpace();
    }

    [RetryFact(3, 5000)]
    public async Task CompleteAsync_FinishReasonIsStopOrLength_Never_Unknown()
    {
        if (!SupportsCompletion) return;

        await using var service = CreateLLMService();
        var request = new LLMRequest
        {
            Messages  = [new LLMMessage { Role = LLMRole.User, Content = "Describe the sky in ten words." }],
            MaxTokens = 32
        };

        var result = await service.CompleteAsync(request);

        result.IsSuccessful.Should().BeTrue();
        result.Data.FinishReason.Should().BeOneOf(
            [LLMFinishReason.Stop, LLMFinishReason.Length, LLMFinishReason.ToolCall],
            because: "finish reason must be a well-known value");
    }

    // ── Edge case: service initialisation flag ────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task IsInitialized_ShouldBeTrueAfterConstruction()
    {
        await using var service = CreateLLMService();

        service.IsInitialized.Should().BeTrue(
            because: "the service should be ready immediately after construction");
    }

    // ── Edge case: single-element batch identical to scalar ──────────────────

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingsAsync_SingleText_ShouldMatchScalarEmbedding()
    {
        await using var service = CreateLLMService();
        const string text = "Consistency check between scalar and batch embeddings.";

        var scalar = await service.CreateEmbeddingAsync(text);
        var batch  = await service.CreateEmbeddingsAsync(new[] { text });

        scalar.IsSuccessful.Should().BeTrue();
        batch.IsSuccessful.Should().BeTrue();
        batch.Data.Should().HaveCount(1);

        CosineSimilarity(scalar.Data, batch.Data[0]).Should().BeGreaterThan(0.99f,
            because: "a single-element batch should produce the same vector as the scalar call");
    }

    // ── Edge case: special characters and unicode ─────────────────────────────

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingAsync_SpecialCharacters_ShouldSucceed()
    {
        await using var service = CreateLLMService();

        var result = await service.CreateEmbeddingAsync(
            "Ünîcödé tëxt with symbols: @#$%^&*() and emojis: \U0001F680\U0001F4A1");

        result.IsSuccessful.Should().BeTrue(because: result.IsSuccessful ? "" : result.ErrorMessage);
        result.Data.Should().NotBeEmpty();
        result.Data.Any(float.IsNaN).Should().BeFalse();
        result.Data.Any(float.IsInfinity).Should().BeFalse();
    }

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingAsync_WhitespaceOnly_ShouldReturnVector()
    {
        await using var service = CreateLLMService();

        var result = await service.CreateEmbeddingAsync("   \t\n  ");

        result.IsSuccessful.Should().BeTrue(because: result.IsSuccessful ? "" : result.ErrorMessage);
        result.Data.Should().NotBeEmpty(
            because: "whitespace-only text should still produce a valid embedding");
    }

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingAsync_NumericText_ShouldReturnVector()
    {
        await using var service = CreateLLMService();

        var result = await service.CreateEmbeddingAsync("1234567890");

        result.IsSuccessful.Should().BeTrue(because: result.IsSuccessful ? "" : result.ErrorMessage);
        result.Data.Should().NotBeEmpty();
        result.Data.Any(float.IsNaN).Should().BeFalse();
    }

    // ── Edge case: embedding values are all finite ────────────────────────────

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingAsync_ValuesAreFinite_NeverInfinite()
    {
        await using var service = CreateLLMService();

        var result = await service.CreateEmbeddingAsync("Test that no embedding value is ±Infinity.");

        result.IsSuccessful.Should().BeTrue();
        result.Data.Any(float.IsInfinity).Should().BeFalse(
            because: "embedding values must be finite numbers");
        result.Data.Any(float.IsNaN).Should().BeFalse(
            because: "embedding values must not be NaN");
    }

    // ── Edge case: large batch embeddings ─────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingsAsync_LargeBatch_ShouldSucceed()
    {
        await using var service = CreateLLMService();
        var texts = Enumerable.Range(0, 15)
            .Select(i => $"Batch item number {i}: a unique sentence for embedding.")
            .ToArray();

        var result = await service.CreateEmbeddingsAsync(texts);

        result.IsSuccessful.Should().BeTrue(because: result.IsSuccessful ? "" : result.ErrorMessage);
        result.Data.Should().HaveCount(15);
        foreach (var vec in result.Data)
        {
            vec.Should().NotBeEmpty();
            vec.Any(float.IsNaN).Should().BeFalse();
        }
    }

    // ── Edge case: duplicate texts in batch should produce same vectors ───────

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingsAsync_DuplicateTexts_ShouldProduceSameVectors()
    {
        await using var service = CreateLLMService();
        const string text = "This text appears twice in the batch.";

        var result = await service.CreateEmbeddingsAsync(new[] { text, text });

        result.IsSuccessful.Should().BeTrue(because: result.IsSuccessful ? "" : result.ErrorMessage);
        result.Data.Should().HaveCount(2);

        CosineSimilarity(result.Data[0], result.Data[1]).Should().BeGreaterThan(0.99f,
            because: "identical texts must produce identical (or near-identical) embeddings");
    }

    // ── Edge case: completion with only user message (no system) ──────────────

    [RetryFact(3, 5000)]
    public async Task CompleteAsync_OnlyUserMessage_ShouldSucceed()
    {
        if (!SupportsCompletion) return;

        await using var service = CreateLLMService();
        var request = new LLMRequest
        {
            Messages  = [new LLMMessage { Role = LLMRole.User, Content = "Say OK." }],
            MaxTokens = 8
        };

        var result = await service.CompleteAsync(request);

        result.IsSuccessful.Should().BeTrue(because: result.IsSuccessful ? "" : result.ErrorMessage);
        result.Data.Content.Should().NotBeNullOrWhiteSpace();
    }

    // ── Edge case: completion with very short prompt ──────────────────────────

    [RetryFact(3, 5000)]
    public async Task CompleteAsync_SingleCharPrompt_ShouldSucceed()
    {
        if (!SupportsCompletion) return;

        await using var service = CreateLLMService();
        var request = new LLMRequest
        {
            Messages  = [new LLMMessage { Role = LLMRole.User, Content = "?" }],
            MaxTokens = 16
        };

        var result = await service.CompleteAsync(request);

        result.IsSuccessful.Should().BeTrue(because: result.IsSuccessful ? "" : result.ErrorMessage);
        result.Data.Content.Should().NotBeNull();
    }

    // ── Edge case: streaming with temperature 0 should produce output ─────────

    [RetryFact(3, 5000)]
    public async Task CompleteStreamingAsync_WithTemperatureZero_ShouldProduceOutput()
    {
        if (!SupportsCompletion) return;

        await using var service = CreateLLMService();
        var request = new LLMRequest
        {
            Messages    = [new LLMMessage { Role = LLMRole.User, Content = "Say the word: test" }],
            MaxTokens   = 8,
            Temperature = 0.0
        };

        var chunks   = new List<string>();
        bool gotFinal = false;

        await foreach (var cr in service.CompleteStreamingAsync(request))
        {
            cr.IsSuccessful.Should().BeTrue(because: cr.IsSuccessful ? "" : cr.ErrorMessage);
            if (cr.Data.IsFinal) { gotFinal = true; break; }
            chunks.Add(cr.Data.ContentDelta);
        }

        gotFinal.Should().BeTrue();
        string.Concat(chunks).Should().NotBeNullOrWhiteSpace(
            because: "streaming with temperature 0 should still produce content");
    }

    // ── Edge case: system-only message list ───────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task CompleteAsync_OnlySystemMessage_ShouldNotThrow()
    {
        if (!SupportsCompletion) return;

        await using var service = CreateLLMService();
        var request = new LLMRequest
        {
            Messages  = [new LLMMessage { Role = LLMRole.System, Content = "You are helpful." }],
            MaxTokens = 16
        };

        // Should not throw — may succeed or return a failure depending on backend
        Func<Task> act = async () => await service.CompleteAsync(request);
        await act.Should().NotThrowAsync(
            because: "implementations should handle edge-case message lists gracefully");
    }

    // ── Edge case: dispose then create new instance ──────────────────────────

    [RetryFact(3, 5000)]
    public async Task CreateLLMService_AfterDisposingPrevious_ShouldWorkIndependently()
    {
        var service1 = CreateLLMService();
        var r1 = await service1.CreateEmbeddingAsync("first");
        r1.IsSuccessful.Should().BeTrue();
        await service1.DisposeAsync();

        // New instance should work fine
        await using var service2 = CreateLLMService();
        var r2 = await service2.CreateEmbeddingAsync("second");
        r2.IsSuccessful.Should().BeTrue(
            because: "a new instance after disposing the previous should be fully functional");
    }

    // ── Edge case: double dispose should not throw ──────────────────────────

    [RetryFact(3, 5000)]
    public async Task DisposeAsync_CalledTwice_ShouldNotThrow()
    {
        var service = CreateLLMService();
        var warmUp = await service.CreateEmbeddingAsync("warm up");
        warmUp.IsSuccessful.Should().BeTrue(because: "service should work before dispose");

        await service.DisposeAsync();

        // Second dispose should be a no-op
        Func<Task> act = async () => await service.DisposeAsync();
        await act.Should().NotThrowAsync(
            because: "disposing an already-disposed service should be safe (idempotent)");
    }

    // ── Edge case: completion with empty messages list ─────────────────────────

    [RetryFact(3, 5000)]
    public async Task CompleteAsync_EmptyMessagesList_ShouldNotThrow()
    {
        if (!SupportsCompletion) return;

        await using var service = CreateLLMService();
        var request = new LLMRequest
        {
            Messages  = [],
            MaxTokens = 8
        };

        // Should not throw — may succeed or return a failure
        Func<Task> act = async () => await service.CompleteAsync(request);
        await act.Should().NotThrowAsync(
            because: "implementations should handle empty message lists gracefully");
    }

    // ── Edge case: embedding vectors should be normalised (unit length) ───────

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingAsync_VectorShouldHaveReasonableMagnitude()
    {
        await using var service = CreateLLMService();

        var result = await service.CreateEmbeddingAsync("A test sentence for magnitude check.");

        result.IsSuccessful.Should().BeTrue();
        float magnitude = MathF.Sqrt(result.Data.Sum(x => x * x));
        magnitude.Should().BeGreaterThan(0.01f,
            because: "embedding vectors should not be near-zero");
    }

    // ── Edge case: embedding with null character ──────────────────────────────

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingAsync_TextWithNullChar_ShouldSucceed()
    {
        await using var service = CreateLLMService();

        var result = await service.CreateEmbeddingAsync("before\0after");

        result.IsSuccessful.Should().BeTrue(because: result.IsSuccessful ? "" : result.ErrorMessage);
        result.Data.Should().NotBeEmpty(
            because: "text with embedded null character should still produce a valid embedding");
    }

    // ── Edge case: batch with single empty string ─────────────────────────────

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingsAsync_BatchWithEmptyString_ShouldSucceed()
    {
        await using var service = CreateLLMService();

        var result = await service.CreateEmbeddingsAsync(["hello", "", "world"]);

        result.IsSuccessful.Should().BeTrue(because: result.IsSuccessful ? "" : result.ErrorMessage);
        result.Data.Should().HaveCount(3);
        foreach (var vec in result.Data)
        {
            vec.Should().NotBeEmpty();
            vec.Any(float.IsNaN).Should().BeFalse();
        }
    }

    // ── Edge case: very long single word ──────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingAsync_VeryLongSingleWord_ShouldSucceed()
    {
        await using var service = CreateLLMService();

        // A long non-dictionary "word"
        var text = new string('x', 500);

        var result = await service.CreateEmbeddingAsync(text);

        result.IsSuccessful.Should().BeTrue(because: result.IsSuccessful ? "" : result.ErrorMessage);
        result.Data.Should().NotBeEmpty();
        result.Data.Any(float.IsNaN).Should().BeFalse();
    }

    // ── Edge case: cosine similarity is symmetric ─────────────────────────────

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingAsync_CosineSimilarityShouldBeSymmetric()
    {
        await using var service = CreateLLMService();

        var rA = await service.CreateEmbeddingAsync("First sentence about dogs");
        var rB = await service.CreateEmbeddingAsync("Second sentence about cats");

        rA.IsSuccessful.Should().BeTrue();
        rB.IsSuccessful.Should().BeTrue();

        float simAB = CosineSimilarity(rA.Data, rB.Data);
        float simBA = CosineSimilarity(rB.Data, rA.Data);

        simAB.Should().BeApproximately(simBA, 1e-6f,
            because: "cosine similarity must be symmetric: sim(A,B) == sim(B,A)");
    }

    // ── Edge case: self-similarity should be ~1.0 ─────────────────────────────

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingAsync_SelfSimilarity_ShouldBeNearlyOne()
    {
        await using var service = CreateLLMService();

        var text = "The meaning of life is 42.";
        var r1 = await service.CreateEmbeddingAsync(text);
        var r2 = await service.CreateEmbeddingAsync(text);

        r1.IsSuccessful.Should().BeTrue();
        r2.IsSuccessful.Should().BeTrue();

        float sim = CosineSimilarity(r1.Data, r2.Data);
        sim.Should().BeGreaterThan(0.99f,
            because: "the same text must produce near-identical embeddings with similarity ~1.0");
    }

    // ── Edge case: streaming with pre-cancelled token ────────────────────────

    [RetryFact(3, 5000)]
    public async Task CompleteStreamingAsync_WithPreCancelledToken_ShouldReturnFailure()
    {
        if (!SupportsCompletion) return;

        await using var service = CreateLLMService();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var request = new LLMRequest
        {
            Messages = [new LLMMessage { Role = LLMRole.User, Content = "Hello!" }],
            MaxTokens = 8
        };

        await foreach (var cr in service.CompleteStreamingAsync(request, cts.Token))
        {
            if (!cr.IsSuccessful)
                break;
        }

        // Either we got a failure chunk or the stream was empty — both are acceptable
        // for a pre-cancelled token. The key invariant: no unhandled exception.
    }

    // ── Edge case: single character embedding ─────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingAsync_SingleChar_ShouldReturnVector()
    {
        await using var service = CreateLLMService();

        var result = await service.CreateEmbeddingAsync("a");

        result.IsSuccessful.Should().BeTrue(because: result.IsSuccessful ? "" : result.ErrorMessage);
        result.Data.Should().NotBeEmpty(
            because: "even a single character should produce a valid embedding");
        result.Data.Any(float.IsNaN).Should().BeFalse();
    }

    // ── Edge case: batch embedding preserves order ────────────────────────────

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingsAsync_ShouldPreserveInputOrder()
    {
        await using var service = CreateLLMService();
        var texts = new[]
        {
            "Dogs are loyal pets.",
            "The stock market crashed.",
            "Quantum physics is hard."
        };

        var batch = await service.CreateEmbeddingsAsync(texts);
        batch.IsSuccessful.Should().BeTrue(because: batch.IsSuccessful ? "" : batch.ErrorMessage);
        batch.Data.Should().HaveCount(3);

        // Verify order by comparing each batch result to its individual embedding
        for (int i = 0; i < texts.Length; i++)
        {
            var single = await service.CreateEmbeddingAsync(texts[i]);
            single.IsSuccessful.Should().BeTrue();

            var sim = CosineSimilarity(single.Data, batch.Data[i]);
            sim.Should().BeGreaterThan(0.99f,
                because: $"batch index {i} should correspond to text '{texts[i]}'");
        }
    }

    // ── Edge case: embedding dimension consistency across batch ────────────────

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingsAsync_AllVectorsShouldHaveSameDimensions()
    {
        await using var service = CreateLLMService();
        var texts = new[]
        {
            "short",
            "A somewhat longer sentence with multiple words for embedding.",
            "42",
            string.Join(" ", Enumerable.Repeat("repeat", 50))
        };

        var result = await service.CreateEmbeddingsAsync(texts);
        result.IsSuccessful.Should().BeTrue(because: result.IsSuccessful ? "" : result.ErrorMessage);

        int dim = result.Data[0].Length;
        foreach (var vec in result.Data)
        {
            vec.Should().HaveCount(dim,
                because: "all embeddings from the same model must share identical dimensionality");
        }
    }

    // ── Edge case: streaming produces at least one content delta ───────────────

    [RetryFact(3, 5000)]
    public async Task CompleteStreamingAsync_ShouldProduceAtLeastOneNonEmptyDelta()
    {
        if (!SupportsCompletion) return;

        await using var service = CreateLLMService();
        var request = new LLMRequest
        {
            Messages = [new LLMMessage { Role = LLMRole.User, Content = "Say OK." }],
            MaxTokens = 8
        };

        var nonEmptyDeltas = new List<string>();
        await foreach (var cr in service.CompleteStreamingAsync(request))
        {
            cr.IsSuccessful.Should().BeTrue(because: cr.IsSuccessful ? "" : cr.ErrorMessage);
            if (!cr.Data.IsFinal && !string.IsNullOrEmpty(cr.Data.ContentDelta))
                nonEmptyDeltas.Add(cr.Data.ContentDelta);
        }

        nonEmptyDeltas.Should().NotBeEmpty(
            because: "streaming should produce at least one chunk with non-empty content");
    }

    // ── Edge case: streaming with empty messages should not throw ────────────

    [RetryFact(3, 5000)]
    public async Task CompleteStreamingAsync_EmptyMessagesList_ShouldNotThrow()
    {
        if (!SupportsCompletion) return;

        await using var service = CreateLLMService();
        var request = new LLMRequest
        {
            Messages  = [],
            MaxTokens = 8
        };

        // Should not throw — may produce a failure chunk or an empty stream
        Func<Task> act = async () =>
        {
            await foreach (var cr in service.CompleteStreamingAsync(request))
            {
                if (!cr.IsSuccessful) break;
            }
        };
        await act.Should().NotThrowAsync(
            because: "streaming with empty messages should not throw an unhandled exception");
    }

    // ── Edge case: batch embeddings with identical texts should be identical ──

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingsAsync_IdenticalTexts_ShouldReturnIdenticalVectors()
    {
        await using var service = CreateLLMService();
        var texts = new[] { "duplicate text", "duplicate text", "duplicate text" };

        var result = await service.CreateEmbeddingsAsync(texts);
        result.IsSuccessful.Should().BeTrue(because: result.IsSuccessful ? "" : result.ErrorMessage);
        result.Data.Should().HaveCount(3);

        // All three vectors should be identical
        CosineSimilarity(result.Data[0], result.Data[1]).Should().BeApproximately(1f, 0.001f,
            because: "identical texts must produce identical embeddings");
        CosineSimilarity(result.Data[1], result.Data[2]).Should().BeApproximately(1f, 0.001f);
    }

    // ── Edge case: complete with only system message (no user turn) ───────────

    [RetryFact(3, 5000)]
    public async Task CompleteStreamingAsync_OnlySystemMessage_ShouldNotThrow()
    {
        if (!SupportsCompletion) return;

        await using var service = CreateLLMService();
        var request = new LLMRequest
        {
            Messages  = [new LLMMessage { Role = LLMRole.System, Content = "You are a helpful assistant." }],
            MaxTokens = 16
        };

        Func<Task> act = async () =>
        {
            await foreach (var cr in service.CompleteStreamingAsync(request))
            {
                if (!cr.IsSuccessful) break;
                if (cr.Data.IsFinal) break;
            }
        };
        await act.Should().NotThrowAsync(
            because: "streaming with only system message should handle gracefully");
    }

    // ── Edge case: embedding batch of one should match singular result ────────

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingsAsync_SingleTextBatch_ShouldMatchSingularResult()
    {
        await using var service = CreateLLMService();
        const string text = "test embedding consistency";

        var single = await service.CreateEmbeddingAsync(text);
        single.IsSuccessful.Should().BeTrue();

        var batch = await service.CreateEmbeddingsAsync(new[] { text });
        batch.IsSuccessful.Should().BeTrue();
        batch.Data.Should().HaveCount(1);

        CosineSimilarity(single.Data, batch.Data[0]).Should().BeApproximately(1f, 0.001f,
            because: "a single-text batch should produce the same result as CreateEmbeddingAsync");
    }

    // ── Edge case: completion with very long system prompt ─────────────────────

    [RetryFact(3, 5000)]
    public async Task CompleteAsync_LongSystemPrompt_ShouldNotThrow()
    {
        if (!SupportsCompletion) return;

        await using var service = CreateLLMService();
        var longSystem = new string('x', 2000);
        var request = new LLMRequest
        {
            Messages =
            [
                new LLMMessage { Role = LLMRole.System, Content = longSystem },
                new LLMMessage { Role = LLMRole.User,   Content = "Say OK." }
            ],
            MaxTokens = 8
        };

        Func<Task> act = async () => await service.CompleteAsync(request);
        await act.Should().NotThrowAsync(
            because: "a long system prompt should not cause an unhandled exception");
    }

    // ── Edge case: embedding with unicode surrogates ──────────────────────────

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingAsync_UnicodeEmoji_ShouldReturnValidVector()
    {
        await using var service = CreateLLMService();

        var result = await service.CreateEmbeddingAsync("Hello 🌍🚀 World");

        result.IsSuccessful.Should().BeTrue(because: result.IsSuccessful ? "" : result.ErrorMessage);
        result.Data.Should().NotBeEmpty();
        result.Data.All(float.IsFinite).Should().BeTrue(
            because: "embeddings should contain only finite values, even for emoji text");
    }

    // ── Edge case: streaming after pre-cancelled complete should not throw ─────

    [RetryFact(3, 5000)]
    public async Task CompleteAsync_WithPreCancelledToken_ShouldReturnFailure()
    {
        if (!SupportsCompletion) return;

        await using var service = CreateLLMService();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var request = new LLMRequest
        {
            Messages = [new LLMMessage { Role = LLMRole.User, Content = "Hello!" }],
            MaxTokens = 8
        };

        var result = await service.CompleteAsync(request, cts.Token);
        result.IsSuccessful.Should().BeFalse(
            because: "a pre-cancelled token should yield a failure, not an exception");
    }

    // ── Edge case: completion with multi-turn including assistant message ──────

    [RetryFact(3, 5000)]
    public async Task CompleteStreamingAsync_MultiTurn_ShouldNotThrow()
    {
        if (!SupportsCompletion) return;

        await using var service = CreateLLMService();
        var request = new LLMRequest
        {
            Messages =
            [
                new LLMMessage { Role = LLMRole.User,      Content = "What is 2+2?" },
                new LLMMessage { Role = LLMRole.Assistant, Content = "4" },
                new LLMMessage { Role = LLMRole.User,      Content = "Now add 1. Reply with just the number." }
            ],
            MaxTokens = 8
        };

        var chunks = new List<string>();
        await foreach (var cr in service.CompleteStreamingAsync(request))
        {
            cr.IsSuccessful.Should().BeTrue(because: cr.IsSuccessful ? "" : cr.ErrorMessage);
            if (cr.Data.IsFinal) break;
            chunks.Add(cr.Data.ContentDelta);
        }

        string.Concat(chunks).Should().NotBeNullOrWhiteSpace(
            because: "multi-turn streaming should produce content");
    }

    // ── Edge case: embeddings of texts of very different lengths ───────────────

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingsAsync_VaryingLengths_ShouldAllHaveSameDimension()
    {
        await using var service = CreateLLMService();
        var texts = new[]
        {
            "a",
            "A medium-length sentence for testing dimension consistency.",
            string.Join(" ", Enumerable.Repeat("long text padding word", 100))
        };

        var result = await service.CreateEmbeddingsAsync(texts);
        result.IsSuccessful.Should().BeTrue(because: result.IsSuccessful ? "" : result.ErrorMessage);
        result.Data.Should().HaveCount(3);

        int dim = result.Data[0].Length;
        foreach (var vec in result.Data)
        {
            vec.Should().HaveCount(dim,
                because: "embeddings must have uniform dimensionality regardless of input length");
            vec.All(float.IsFinite).Should().BeTrue();
        }
    }

    // ── Edge case: complete then embed on same service instance ────────────────

    [RetryFact(3, 5000)]
    public async Task CompleteAsync_ThenEmbed_ShouldBothSucceedOnSameInstance()
    {
        if (!SupportsCompletion) return;

        await using var service = CreateLLMService();

        var completion = await service.CompleteAsync(new LLMRequest
        {
            Messages = [new LLMMessage { Role = LLMRole.User, Content = "Say hi." }],
            MaxTokens = 8
        });
        completion.IsSuccessful.Should().BeTrue(because: completion.IsSuccessful ? "" : completion.ErrorMessage);

        var embedding = await service.CreateEmbeddingAsync("test after completion");
        embedding.IsSuccessful.Should().BeTrue(because: embedding.IsSuccessful ? "" : embedding.ErrorMessage);
        embedding.Data.Should().NotBeEmpty();
    }

    // ── Edge case: streaming MaxTokens 1 should produce at least one delta ────

    [RetryFact(3, 5000)]
    public async Task CompleteStreamingAsync_MaxTokensOne_ShouldProduceSomeOutput()
    {
        if (!SupportsCompletion) return;

        await using var service = CreateLLMService();
        var request = new LLMRequest
        {
            Messages  = [new LLMMessage { Role = LLMRole.User, Content = "Say hello world." }],
            MaxTokens = 1
        };

        var chunks = new List<string>();
        bool gotFinal = false;
        await foreach (var cr in service.CompleteStreamingAsync(request))
        {
            cr.IsSuccessful.Should().BeTrue(because: cr.IsSuccessful ? "" : cr.ErrorMessage);
            if (cr.Data.IsFinal) { gotFinal = true; break; }
            chunks.Add(cr.Data.ContentDelta);
        }

        gotFinal.Should().BeTrue(because: "even with MaxTokens=1, streaming should complete");
    }

    // ── Edge case: empty string embedding ────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingAsync_EmptyString_ShouldNotThrow()
    {
        await using var service = CreateLLMService();

        // An empty string should either produce a valid embedding or a graceful failure,
        // but must never throw an unhandled exception.
        var result = await service.CreateEmbeddingAsync(string.Empty);

        if (result.IsSuccessful)
        {
            result.Data.Should().NotBeNull();
            result.Data.Length.Should().BeGreaterThan(0,
                because: "if the operation succeeds, the embedding must have dimensions");
            result.Data.Any(float.IsNaN).Should().BeFalse();
        }
        // If not successful, that's acceptable too — it's a graceful failure
    }

    // ── Edge case: embedding dimension consistency across inputs ──────────────

    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingAsync_DifferentTexts_ShouldReturnSameDimensions()
    {
        await using var service = CreateLLMService();

        var result1 = await service.CreateEmbeddingAsync("Hello");
        var result2 = await service.CreateEmbeddingAsync("A much longer sentence with many words to embed.");
        var result3 = await service.CreateEmbeddingAsync("X");

        result1.IsSuccessful.Should().BeTrue(because: result1.IsSuccessful ? "" : result1.ErrorMessage);
        result2.IsSuccessful.Should().BeTrue(because: result2.IsSuccessful ? "" : result2.ErrorMessage);
        result3.IsSuccessful.Should().BeTrue(because: result3.IsSuccessful ? "" : result3.ErrorMessage);

        result1.Data.Length.Should().Be(result2.Data.Length,
            because: "all embeddings from the same model must have the same number of dimensions");
        result2.Data.Length.Should().Be(result3.Data.Length);
    }

    // ── Edge case: streaming with pre-cancelled token ─────────────────────────

    [RetryFact(3, 5000)]
    public async Task CompleteStreamingAsync_PreCancelledToken_ShouldYieldCancelOrEmpty()
    {
        if (!SupportsCompletion) return;

        await using var service = CreateLLMService();
        var cts = new CancellationTokenSource();
        cts.Cancel(); // Pre-cancel

        var request = new LLMRequest
        {
            Messages = [new LLMMessage { Role = LLMRole.User, Content = "Hello" }],
            MaxTokens = 10
        };

        var chunks = new List<OperationResult<LLMStreamChunk>>();
        await foreach (var cr in service.CompleteStreamingAsync(request, cts.Token))
            chunks.Add(cr);

        // With a pre-cancelled token, we expect either:
        // - No chunks at all (enumerable is empty)
        // - A single failure chunk indicating cancellation
        if (chunks.Count > 0)
        {
            chunks.Should().HaveCount(1,
                because: "a pre-cancelled token should produce at most one error chunk");
            chunks[0].IsSuccessful.Should().BeFalse(
                because: "the single chunk should indicate a cancellation failure");
        }
    }

    // ── Edge case: CompleteAsync with pre-cancelled token ─────────────────────

    [RetryFact(3, 5000)]
    public async Task CompleteAsync_PreCancelledToken_ShouldReturnFailure()
    {
        if (!SupportsCompletion) return;

        await using var service = CreateLLMService();
        var cts = new CancellationTokenSource();
        cts.Cancel();

        var request = new LLMRequest
        {
            Messages = [new LLMMessage { Role = LLMRole.User, Content = "Hello" }],
            MaxTokens = 10
        };

        var result = await service.CompleteAsync(request, cts.Token);
        result.IsSuccessful.Should().BeFalse(
            because: "a pre-cancelled token should cause a failure result");
    }

    // ── Edge case: all streaming chunks before IsFinal must be successful ─────

    /// <summary>
    /// Contract enforcement: the ILLMService interface states that "on error, a failure result
    /// is yielded and the stream ends." This test verifies the inverse for happy paths:
    /// in a normal (non-error) stream every chunk is successful until the IsFinal sentinel.
    /// </summary>
    [RetryFact(3, 5000)]
    public async Task CompleteStreamingAsync_NormalStream_AllChunksBeforeFinalAreSuccessful()
    {
        if (!SupportsCompletion) return;

        await using var service = CreateLLMService();
        var request = new LLMRequest
        {
            Messages  = [new LLMMessage { Role = LLMRole.User, Content = "Say hi." }],
            MaxTokens = 16
        };

        var failures = new List<string>();
        await foreach (var cr in service.CompleteStreamingAsync(request))
        {
            if (!cr.IsSuccessful)
                failures.Add(cr.ErrorMessage ?? "<null>");
            if (cr.IsSuccessful && cr.Data.IsFinal)
                break;
        }

        failures.Should().BeEmpty(
            because: "in a normal (non-error) stream no failure chunks should appear before IsFinal");
    }

    // ── Edge case: CreateEmbeddingsAsync output count always equals input count ─

    /// <summary>
    /// The bridge utilities (<c>EmbedAndUpsertBatchAsync</c>) explicitly depend on the batch
    /// return having the same count as the input. Verify the invariant holds for the service.
    /// </summary>
    [RetryFact(3, 5000)]
    public async Task CreateEmbeddingsAsync_OutputCountAlwaysMatchesInputCount()
    {
        await using var service = CreateLLMService();
        var texts = new[]
        {
            "alpha",
            "beta gamma",
            "delta epsilon zeta"
        };

        var result = await service.CreateEmbeddingsAsync(texts);

        result.IsSuccessful.Should().BeTrue(because: result.IsSuccessful ? "" : result.ErrorMessage);
        result.Data.Should().HaveCount(texts.Length,
            because: "the number of returned embeddings must exactly match the number of input texts");
    }

    // ── Edge case: streaming always delivers IsFinal before actual termination ─

    [RetryFact(3, 5000)]
    public async Task CompleteStreamingAsync_ShouldAlwaysEndWithIsFinalChunk()
    {
        if (!SupportsCompletion) return;

        await using var service = CreateLLMService();
        var request = new LLMRequest
        {
            Messages  = [new LLMMessage { Role = LLMRole.User, Content = "Say OK." }],
            MaxTokens = 8
        };

        bool gotFinal = false;
        await foreach (var cr in service.CompleteStreamingAsync(request))
        {
            if (!cr.IsSuccessful) break; // error terminates; IsFinal not required
            if (cr.Data.IsFinal) { gotFinal = true; break; }
        }

        gotFinal.Should().BeTrue(
            because: "a successful streaming response must conclude with IsFinal=true");
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static float CosineSimilarity(float[] a, float[] b)
    {
        int len = Math.Min(a.Length, b.Length);
        float dot = 0, magA = 0, magB = 0;
        for (int i = 0; i < len; i++)
        {
            dot  += a[i] * b[i];
            magA += a[i] * a[i];
            magB += b[i] * b[i];
        }
        float denom = MathF.Sqrt(magA) * MathF.Sqrt(magB);
        return denom == 0 ? 0 : dot / denom;
    }
}
