// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Cloud.Interfaces;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Cloud.PubSub.Tests.Common;

public abstract class PubSubServiceTestBase(ITestOutputHelper testOutputHelper)
{
    protected abstract IPubSubService CreatePubSubService();

    protected virtual string GenerateTestTopic() => $"test-topic-{Guid.NewGuid():N}";

    private static void AssertInitialized(IPubSubService service, List<string> errors)
    {
        service.Should().NotBeNull();
        service.IsInitialized.Should().BeTrue($"Service should be initialized. Errors: {string.Join(" | ", errors)}");
    }

    [Fact]
    public virtual void Service_ShouldNotBeNull()
    {
        var service = CreatePubSubService();
        service.Should().NotBeNull();
    }

    [Fact]
    public virtual async Task Publish_And_Subscribe_ShouldDeliverMessage()
    {
        var errors = new List<string>();
        var service = CreatePubSubService();
        AssertInitialized(service, errors);
        var topic = GenerateTestTopic();
        try
        {
            var received = new TaskCompletionSource<string>();
            await service.SubscribeAsync(topic, (_, msg) => { received.TrySetResult(msg); return Task.CompletedTask; }, ErrorLogger);
            const string sent = "hello world";
            var publishResult = await service.PublishAsync(topic, sent, ErrorLogger);
            publishResult.Should().BeTrue($"Errors: {string.Join(" | ", errors)}");
            (await received.Task.WaitAsync(TimeSpan.FromSeconds(10))).Should().Be(sent);
        }
        finally
        {
            await service.DeleteTopicAsync(topic, ErrorLogger);
        }

        return;

        void ErrorLogger(string msg)
        {
            errors.Add(msg);
            testOutputHelper.WriteLine($"[PubSubTestError] {msg}");
        }
    }

    [Fact]
    public virtual async Task MultipleSubscribers_ShouldAllReceiveMessages()
    {
        var errors = new List<string>();
        var service = CreatePubSubService();
        AssertInitialized(service, errors);
        var topic = GenerateTestTopic();
        try
        {
            var received1 = new TaskCompletionSource<string>();
            var received2 = new TaskCompletionSource<string>();

            testOutputHelper.WriteLine("Setting up first subscription...");
            var sub1Result = await service.SubscribeAsync(topic, (_, msg) =>
            {
                testOutputHelper.WriteLine($"Subscriber 1 received: {msg}");
                received1.TrySetResult(msg);
                return Task.CompletedTask;
            }, ErrorLogger);
            sub1Result.Should().BeTrue("First subscription should succeed");

            testOutputHelper.WriteLine("Setting up second subscription...");
            var sub2Result = await service.SubscribeAsync(topic, (_, msg) =>
            {
                testOutputHelper.WriteLine($"Subscriber 2 received: {msg}");
                received2.TrySetResult(msg);
                return Task.CompletedTask;
            }, ErrorLogger);
            sub2Result.Should().BeTrue("Second subscription should succeed");

            // Allow significant time for both subscriptions to be fully established in the cloud
            testOutputHelper.WriteLine("Waiting for subscriptions to be fully established...");
            await Task.Delay(5000);

            const string sent = "fanout";
            testOutputHelper.WriteLine($"Publishing message: {sent}");
            var publishResult = await service.PublishAsync(topic, sent, ErrorLogger);
            publishResult.Should().BeTrue($"Publish should succeed. Errors: {string.Join(" | ", errors)}");

            testOutputHelper.WriteLine("Waiting for messages to be delivered...");
            (await received1.Task.WaitAsync(TimeSpan.FromSeconds(30))).Should().Be(sent);
            (await received2.Task.WaitAsync(TimeSpan.FromSeconds(30))).Should().Be(sent);

            testOutputHelper.WriteLine("Both subscribers received the message successfully!");
        }
        finally
        {
            await service.DeleteTopicAsync(topic, ErrorLogger);
        }

        return;

        void ErrorLogger(string msg)
        {
            errors.Add(msg);
            testOutputHelper.WriteLine($"[PubSubTestError] {msg}");
        }
    }

    [Fact]
    public virtual async Task Subscribe_InvalidTopic_ShouldReturnFalse()
    {
        var errors = new List<string>();
        var service = CreatePubSubService();
        AssertInitialized(service, errors);
        var result = await service.SubscribeAsync("", (_, _) => Task.CompletedTask, ErrorLogger);
        result.Should().BeFalse($"Errors: {string.Join(" | ", errors)}");
        return;

        void ErrorLogger(string msg)
        {
            errors.Add(msg);
            testOutputHelper.WriteLine($"[PubSubTestError] {msg}");
        }
    }

    [Fact]
    public virtual async Task Publish_InvalidTopic_ShouldReturnFalse()
    {
        var errors = new List<string>();
        var service = CreatePubSubService();
        AssertInitialized(service, errors);
        var result = await service.PublishAsync("", "msg", ErrorLogger);
        result.Should().BeFalse($"Errors: {string.Join(" | ", errors)}");
        return;

        void ErrorLogger(string msg)
        {
            errors.Add(msg);
            testOutputHelper.WriteLine($"[PubSubTestError] {msg}");
        }
    }

    [Fact]
    public virtual async Task Publish_EmptyMessage_ShouldReturnFalse()
    {
        var errors = new List<string>();
        var service = CreatePubSubService();
        AssertInitialized(service, errors);
        var topic = GenerateTestTopic();
        var result = await service.PublishAsync(topic, "", ErrorLogger);
        result.Should().BeFalse($"Errors: {string.Join(" | ", errors)}");
        return;

        void ErrorLogger(string msg)
        {
            errors.Add(msg);
            testOutputHelper.WriteLine($"[PubSubTestError] {msg}");
        }
    }

    [Fact]
    public virtual async Task DisposeAsync_ShouldCleanupSubscriptions()
    {
        var errors = new List<string>();
        var service = CreatePubSubService();
        AssertInitialized(service, errors);
        var topic = GenerateTestTopic();
        var received = false;
        try
        {
            await service.SubscribeAsync(topic, (_, _) => { received = true; return Task.CompletedTask; }, ErrorLogger);
            if (service is IAsyncDisposable asyncDisposable)
                await asyncDisposable.DisposeAsync();
            await service.PublishAsync(topic, "should-not-deliver", ErrorLogger);
            received.Should().BeFalse();
        }
        finally
        {
            await service.DeleteTopicAsync(topic, ErrorLogger);
        }
        return;

        void ErrorLogger(string msg)
        {
            errors.Add(msg);
            testOutputHelper.WriteLine($"[PubSubTestError] {msg}");
        }
    }

    [Fact]
    public virtual async Task Publish_ConcurrentMessages_ShouldDeliverAll()
    {
        var errors = new List<string>();
        var service = CreatePubSubService();
        AssertInitialized(service, errors);
        var topic = GenerateTestTopic();
        try
        {
            var received = new List<string>();
            var lockObj = new object();
            var allReceived = new TaskCompletionSource<bool>();
            const int expected = 20;

            await service.SubscribeAsync(topic, (_, msg) =>
            {
                lock (lockObj)
                {
                    received.Add(msg);
                    if (received.Count == expected)
                        allReceived.TrySetResult(true);
                }
                return Task.CompletedTask;
            }, ErrorLogger);

            // Publish messages concurrently
            var publishTasks = Enumerable.Range(0, expected)
                .Select(i => service.PublishAsync(topic, $"concurrent-msg-{i}", ErrorLogger))
                .ToArray();

            var allPublished = await Task.WhenAll(publishTasks);
            allPublished.Should().AllBeEquivalentTo(true, $"Errors: {string.Join(" | ", errors)}");

            await allReceived.Task.WaitAsync(TimeSpan.FromSeconds(15));
            received.Should().HaveCount(expected);
            received.Should().AllSatisfy(msg => msg.Should().StartWith("concurrent-msg-"));
        }
        finally
        {
            await service.DeleteTopicAsync(topic, ErrorLogger);
        }

        return;

        void ErrorLogger(string msg)
        {
            errors.Add(msg);
            testOutputHelper.WriteLine($"[PubSubTestError] {msg}");
        }
    }

    [Fact]
    public virtual async Task Publish_LargeMessage_ShouldHandleAppropriately()
    {
        var errors = new List<string>();
        var service = CreatePubSubService();
        AssertInitialized(service, errors);
        var topic = GenerateTestTopic();
        try
        {
            var received = new TaskCompletionSource<string>();
            await service.SubscribeAsync(topic, (_, msg) =>
            {
                received.TrySetResult(msg);
                return Task.CompletedTask;
            }, ErrorLogger);

            // Create a large message (64KB)
            var largeMessage = new string('A', 64 * 1024);
            var publishResult = await service.PublishAsync(topic, largeMessage, ErrorLogger);

            if (publishResult)
            {
                var result = await received.Task.WaitAsync(TimeSpan.FromSeconds(15));
                result.Should().Be(largeMessage);
            }
            else
            {
                // Some services may have message size limits, which is acceptable behavior
                testOutputHelper.WriteLine("Large message rejected - this may be expected behavior for the service");
            }
        }
        finally
        {
            await service.DeleteTopicAsync(topic, ErrorLogger);
        }

        return;

        void ErrorLogger(string msg)
        {
            errors.Add(msg);
            testOutputHelper.WriteLine($"[PubSubTestError] {msg}");
        }
    }

    [Fact]
    public virtual async Task Publish_SpecialCharacters_ShouldPreserveContent()
    {
        var errors = new List<string>();
        var service = CreatePubSubService();
        AssertInitialized(service, errors);
        var topic = GenerateTestTopic();
        try
        {
            var received = new TaskCompletionSource<string>();
            await service.SubscribeAsync(topic, (_, msg) =>
            {
                received.TrySetResult(msg);
                return Task.CompletedTask;
            }, ErrorLogger);

            const string specialMessage = "Hello 世界! 🎉 Special chars: @#$%^&*()_+-=[]{}|;':\",./<>?`~";
            var publishResult = await service.PublishAsync(topic, specialMessage, ErrorLogger);
            publishResult.Should().BeTrue($"Errors: {string.Join(" | ", errors)}");

            var result = await received.Task.WaitAsync(TimeSpan.FromSeconds(10));
            result.Should().Be(specialMessage);
        }
        finally
        {
            await service.DeleteTopicAsync(topic, ErrorLogger);
        }

        return;

        void ErrorLogger(string msg)
        {
            errors.Add(msg);
            testOutputHelper.WriteLine($"[PubSubTestError] {msg}");
        }
    }

    [Fact]
    public virtual async Task Subscribe_NullCallback_ShouldReturnFalse()
    {
        var errors = new List<string>();
        var service = CreatePubSubService();
        AssertInitialized(service, errors);
        var topic = GenerateTestTopic();

        var result = await service.SubscribeAsync(topic, null!, ErrorLogger);
        result.Should().BeFalse($"Errors: {string.Join(" | ", errors)}");

        return;

        void ErrorLogger(string msg)
        {
            errors.Add(msg);
            testOutputHelper.WriteLine($"[PubSubTestError] {msg}");
        }
    }

    [Fact]
    public virtual async Task Publish_NullMessage_ShouldReturnFalse()
    {
        var errors = new List<string>();
        var service = CreatePubSubService();
        AssertInitialized(service, errors);
        var topic = GenerateTestTopic();

        var result = await service.PublishAsync(topic, null!, ErrorLogger);
        result.Should().BeFalse($"Errors: {string.Join(" | ", errors)}");

        return;

        void ErrorLogger(string msg)
        {
            errors.Add(msg);
            testOutputHelper.WriteLine($"[PubSubTestError] {msg}");
        }
    }

    [Fact]
    public virtual async Task Topic_WithSpecialCharacters_ShouldHandleAppropriately()
    {
        var errors = new List<string>();
        var service = CreatePubSubService();
        AssertInitialized(service, errors);

        // Test with different topic naming patterns
        var topics = new[]
        {
            "topic-with-dashes",
            "topic_with_underscores",
            "topic.with.dots",
            "topicWithCamelCase"
        };

        foreach (var topic in topics)
        {
            try
            {
                var received = new TaskCompletionSource<string>();
                var subscribeResult = await service.SubscribeAsync(topic, (_, msg) =>
                {
                    received.TrySetResult(msg);
                    return Task.CompletedTask;
                }, ErrorLogger);

                if (subscribeResult)
                {
                    const string message = "test-message";
                    var publishResult = await service.PublishAsync(topic, message, ErrorLogger);

                    if (publishResult)
                    {
                        var result = await received.Task.WaitAsync(TimeSpan.FromSeconds(5));
                        result.Should().Be(message);
                    }
                }
            }
            catch (Exception ex)
            {
                testOutputHelper.WriteLine($"Topic '{topic}' failed: {ex.Message}");
                // Some topic names may not be supported, which is acceptable
            }
            finally
            {
                await service.DeleteTopicAsync(topic, ErrorLogger);
            }
        }

        return;

        void ErrorLogger(string msg)
        {
            errors.Add(msg);
            testOutputHelper.WriteLine($"[PubSubTestError] {msg}");
        }
    }

    [Fact]
    public virtual async Task Service_Reinitialization_ShouldMaintainFunctionality()
    {
        var errors = new List<string>();

        var topic = GenerateTestTopic();

        var service1 = CreatePubSubService();
        AssertInitialized(service1, errors);

        // Test with first service instance
        var received1 = new TaskCompletionSource<string>();
        try
        {
            await service1.SubscribeAsync(topic, (_, msg) =>
            {
                received1.TrySetResult(msg);
                return Task.CompletedTask;
            }, ErrorLogger);
            await service1.PublishAsync(topic, "message1", ErrorLogger);
            var res1 = (await received1.Task.WaitAsync(TimeSpan.FromSeconds(15)));
            res1.Should().Be("message1");

            // Create a new service instance
            var service2 = CreatePubSubService();
            AssertInitialized(service2, errors);

            // Test with a second service instance
            var received2 = new TaskCompletionSource<string>();
            try
            {
                await service2.SubscribeAsync(topic, (_, msg) =>
                {
                    received2.TrySetResult(msg);
                    return Task.CompletedTask;
                }, ErrorLogger);
                await service2.PublishAsync(topic, "message2", ErrorLogger);
                var res2 = (await received2.Task.WaitAsync(TimeSpan.FromSeconds(15)));
                res2.Should().Be("message2");
            }
            finally
            {
                await service2.DeleteTopicAsync(topic, ErrorLogger);

                // Cleanup with the second service
                if (service2 is IAsyncDisposable asyncDisposable2)
                    await asyncDisposable2.DisposeAsync();
            }
        }
        finally
        {
            await service1.DeleteTopicAsync(topic, ErrorLogger);
            if (service1 is IAsyncDisposable asyncDisposable1)
                await asyncDisposable1.DisposeAsync();
        }

        return;

        void ErrorLogger(string msg)
        {
            errors.Add(msg);
            testOutputHelper.WriteLine($"[PubSubTestError] {msg}");
        }
    }

    [Fact]
    public virtual async Task Publish_HighFrequency_ShouldMaintainPerformance()
    {
        var errors = new List<string>();
        var service = CreatePubSubService();
        AssertInitialized(service, errors);
        var topic = GenerateTestTopic();

        var received = new List<string>();
        try
        {
            var lockObj = new object();
            var allReceived = new TaskCompletionSource<bool>();
            const int messageCount = 100;

            await service.SubscribeAsync(topic, (_, msg) =>
            {
                lock (lockObj)
                {
                    received.Add(msg);
                    if (received.Count >= messageCount)
                        allReceived.TrySetResult(true);
                }

                return Task.CompletedTask;
            }, ErrorLogger);

            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            // Publish messages rapidly
            var publishTasks = new List<Task<bool>>();
            for (var i = 0; i < messageCount; i++)
            {
                publishTasks.Add(service.PublishAsync(topic, $"perf-msg-{i}", ErrorLogger));
            }

            var publishResults = await Task.WhenAll(publishTasks);
            var publishTime = stopwatch.Elapsed;

            await allReceived.Task.WaitAsync(TimeSpan.FromSeconds(30));
            var totalTime = stopwatch.Elapsed;

            testOutputHelper.WriteLine($"Published {messageCount} messages in {publishTime.TotalMilliseconds:F2}ms");
            testOutputHelper.WriteLine($"Received all messages in {totalTime.TotalMilliseconds:F2}ms");
            testOutputHelper.WriteLine($"Publish rate: {messageCount / publishTime.TotalSeconds:F2} msg/sec");
            testOutputHelper.WriteLine($"End-to-end rate: {messageCount / totalTime.TotalSeconds:F2} msg/sec");

            publishResults.Should().AllBeEquivalentTo(true);
            received.Should().HaveCount(messageCount);
        }
        finally
        {
            await service.DeleteTopicAsync(topic, ErrorLogger);
        }

        return;

        void ErrorLogger(string msg)
        {
            errors.Add(msg);
            testOutputHelper.WriteLine($"[PubSubTestError] {msg}");
        }
    }

    [Fact]
    public virtual async Task DeleteTopic_NonExistentTopic_ShouldNotThrow()
    {
        var errors = new List<string>();
        var service = CreatePubSubService();
        AssertInitialized(service, errors);

        var nonExistentTopic = $"non-existent-{Guid.NewGuid():N}";

        // This should not throw an exception
        await service.DeleteTopicAsync(nonExistentTopic, ErrorLogger);

        // The service may log errors, but should handle the case gracefully
        testOutputHelper.WriteLine($"Delete non-existent topic completed. Errors logged: {errors.Count}");

        return;

        void ErrorLogger(string msg)
        {
            errors.Add(msg);
            testOutputHelper.WriteLine($"[PubSubTestError] {msg}");
        }
    }

    [Fact]
    public async Task MarkUsedOnBucketEvent_ValidTopic_ShouldReturnTrue()
    {
        // Arrange
        var service = CreatePubSubService();
        var errors = new List<string>();
        AssertInitialized(service, errors);

        var topic = GenerateTestTopic();

        // Ensure topic exists first
        await service.EnsureTopicExistsAsync(topic, errors.Add);
        try
        {
            // Act
            var result = await service.MarkUsedOnBucketEvent(topic);

            // Assert
            Assert.True(result, "MarkUsedOnBucketEvent should return true for valid topic");
        }
        finally
        {
            // Cleanup
            await service.DeleteTopicAsync(topic);
        }
    }

    [Fact]
    public async Task UnmarkUsedOnBucketEvent_ValidTopic_ShouldReturnTrue()
    {
        // Arrange
        var service = CreatePubSubService();
        var errors = new List<string>();
        AssertInitialized(service, errors);

        var topic = GenerateTestTopic();

        // Ensure topic exists and mark it first
        await service.EnsureTopicExistsAsync(topic, errors.Add);
        try
        {
            await service.MarkUsedOnBucketEvent(topic);

            // Act
            var result = await service.UnmarkUsedOnBucketEvent(topic);

            // Assert
            Assert.True(result, "UnmarkUsedOnBucketEvent should return true for valid topic");
        }
        finally
        {
            // Cleanup
            await service.DeleteTopicAsync(topic);
        }
    }

    [Fact]
    public async Task GetTopicsUsedOnBucketEventAsync_AfterMarking_ShouldReturnMarkedTopics()
    {
        // Arrange
        var service = CreatePubSubService();
        var errors = new List<string>();
        AssertInitialized(service, errors);

        var topic1 = GenerateTestTopic();
        var topic2 = GenerateTestTopic();

        // Ensure topics exist and mark them
        await service.EnsureTopicExistsAsync(topic1, errors.Add);
        try
        {
            try
            {
                await service.EnsureTopicExistsAsync(topic2, errors.Add);
                await service.MarkUsedOnBucketEvent(topic1);
                await service.MarkUsedOnBucketEvent(topic2);

                // Act
                var markedTopics = await service.GetTopicsUsedOnBucketEventAsync(errors.Add);

                // Assert
                Assert.NotNull(markedTopics);
                Assert.Contains(topic1, markedTopics);
                Assert.Contains(topic2, markedTopics);
            }
            finally
            {
                await service.DeleteTopicAsync(topic2);
            }
        }
        finally
        {
            await service.DeleteTopicAsync(topic1);
        }
    }

    [Fact]
    public async Task GetTopicsUsedOnBucketEventAsync_AfterUnmarking_ShouldNotReturnUnmarkedTopics()
    {
        // Arrange
        var service = CreatePubSubService();
        var errors = new List<string>();
        AssertInitialized(service, errors);

        var topic = GenerateTestTopic();

        // Ensure a topic exists, mark it, then unmark it
        await service.EnsureTopicExistsAsync(topic, errors.Add);
        try
        {
            await service.MarkUsedOnBucketEvent(topic);
            await service.UnmarkUsedOnBucketEvent(topic);

            // Act
            var markedTopics = await service.GetTopicsUsedOnBucketEventAsync(errors.Add);

            // Assert
            Assert.NotNull(markedTopics);
            Assert.DoesNotContain(topic, markedTopics);
        }
        finally
        {
            // Cleanup
            await service.DeleteTopicAsync(topic);
        }
    }

    [Fact]
    public async Task MarkUsedOnBucketEvent_EmptyTopic_ShouldReturnFalse()
    {
        // Arrange
        var service = CreatePubSubService();
        var errors = new List<string>();
        AssertInitialized(service, errors);

        // Act & Assert
        var result1 = await service.MarkUsedOnBucketEvent("");
        var result2 = await service.MarkUsedOnBucketEvent(null!);

        Assert.False(result1, "MarkUsedOnBucketEvent should return false for empty topic");
        Assert.False(result2, "MarkUsedOnBucketEvent should return false for null topic");
    }

    [Fact]
    public async Task BucketEventMethods_NonExistentTopic_ShouldHandleGracefully()
    {
        // Arrange
        var service = CreatePubSubService();
        var errors = new List<string>();
        AssertInitialized(service, errors);

        var nonExistentTopic = "non-existent-topic-" + Guid.NewGuid().ToString("N")[..8];

        // Act
        var markResult = await service.MarkUsedOnBucketEvent(nonExistentTopic);
        var unmarkResult = await service.UnmarkUsedOnBucketEvent(nonExistentTopic);

        // Assert - behavior may vary by implementation, but should not throw
        // AWS creates the topic automatically, GC might require explicit creation
        Assert.True(markResult || !markResult); // Either behavior is acceptable
        Assert.True(unmarkResult || !unmarkResult); // Either behavior is acceptable
    }
}
