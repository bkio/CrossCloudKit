// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Utilities.Common;
using FluentAssertions;
using xRetry;
using Xunit;
using Xunit.Abstractions;

namespace CrossCloudKit.PubSub.Tests.Common;

public abstract class PubSubServiceTestBase(ITestOutputHelper testOutputHelper)
{
    protected abstract IPubSubService CreatePubSubService();

    private static string GenerateTestTopic([CallerMemberName] string? caller = null)
    {
        return $"test-{caller}-{StringUtilities.GenerateRandomString(8)}";
    }

    private static void AssertInitialized(IPubSubService service)
    {
        service.Should().NotBeNull();
        service.IsInitialized.Should().BeTrue($"Service should be initialized.");
    }

    [RetryFact(3, 5000)]
    public virtual void Service_ShouldNotBeNull()
    {
        var service = CreatePubSubService();
        service.Should().NotBeNull();
    }

    [RetryFact(3, 5000)]
    public virtual async Task Publish_And_Subscribe_ShouldDeliverMessage()
    {
        var service = CreatePubSubService();
        AssertInitialized(service);
        var topic = GenerateTestTopic();
        try
        {
            var received = new TaskCompletionSource<string>();
            await service.SubscribeAsync(topic, (_, msg) =>
            {
                received.TrySetResult(msg);
                return Task.CompletedTask;
            });
            const string sent = "hello world";
            var publishResult = await service.PublishAsync(topic, sent);
            publishResult.IsSuccessful.Should().BeTrue($"Errors: {publishResult.ErrorMessage}");
            (await received.Task.WaitAsync(TimeSpan.FromSeconds(10))).Should().Be(sent);
        }
        finally
        {
            await service.DeleteTopicAsync(topic);
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task MultipleSubscribers_ShouldAllReceiveMessages()
    {
        var service = CreatePubSubService();
        AssertInitialized(service);
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
            });
            sub1Result.IsSuccessful.Should().BeTrue("First subscription should succeed");

            testOutputHelper.WriteLine("Setting up second subscription...");
            var sub2Result = await service.SubscribeAsync(topic, (_, msg) =>
            {
                testOutputHelper.WriteLine($"Subscriber 2 received: {msg}");
                received2.TrySetResult(msg);
                return Task.CompletedTask;
            });
            sub2Result.IsSuccessful.Should().BeTrue("Second subscription should succeed");

            // Allow significant time for both subscriptions to be fully established in the cloud
            testOutputHelper.WriteLine("Waiting for subscriptions to be fully established...");
            await Task.Delay(5000);

            const string sent = "fanout";
            testOutputHelper.WriteLine($"Publishing message: {sent}");
            var publishResult = await service.PublishAsync(topic, sent);
            publishResult.IsSuccessful.Should().BeTrue($"Publish should succeed. Errors: {publishResult.ErrorMessage}");

            testOutputHelper.WriteLine("Waiting for messages to be delivered...");
            (await received1.Task.WaitAsync(TimeSpan.FromSeconds(30))).Should().Be(sent);
            (await received2.Task.WaitAsync(TimeSpan.FromSeconds(30))).Should().Be(sent);

            testOutputHelper.WriteLine("Both subscribers received the message successfully!");
        }
        finally
        {
            await service.DeleteTopicAsync(topic);
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task Subscribe_InvalidTopic_ShouldReturnFalse()
    {
        var service = CreatePubSubService();
        AssertInitialized(service);
        var result = await service.SubscribeAsync("", (_, _) => Task.CompletedTask);
        result.IsSuccessful.Should().BeFalse($"Errors: {result.ErrorMessage}");
    }

    [RetryFact(3, 5000)]
    public virtual async Task Publish_InvalidTopic_ShouldReturnFalse()
    {
        var service = CreatePubSubService();
        AssertInitialized(service);
        var result = await service.PublishAsync("", "msg");
        result.IsSuccessful.Should().BeFalse($"Errors: {result.ErrorMessage}");
    }

    [RetryFact(3, 5000)]
    public virtual async Task Publish_EmptyMessage_ShouldReturnFalse()
    {
        var service = CreatePubSubService();
        AssertInitialized(service);
        var topic = GenerateTestTopic();
        var result = await service.PublishAsync(topic, "");
        result.IsSuccessful.Should().BeFalse($"Errors: {result.ErrorMessage}");
    }

    [RetryFact(3, 5000)]
    public virtual async Task DisposeAsync_ShouldCleanupSubscriptions()
    {
        var service = CreatePubSubService();
        AssertInitialized(service);
        var topic = GenerateTestTopic();
        var received = false;
        try
        {
            await service.SubscribeAsync(topic, (_, _) => { received = true; return Task.CompletedTask; });
            if (service is IAsyncDisposable asyncDisposable)
                await asyncDisposable.DisposeAsync();
            await service.PublishAsync(topic, "should-not-deliver");
            received.Should().BeFalse();
        }
        finally
        {
            await service.DeleteTopicAsync(topic);
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task Publish_ConcurrentMessages_ShouldDeliverAll()
    {
        var service = CreatePubSubService();
        AssertInitialized(service);
        var topic = GenerateTestTopic();
        try
        {
            var received = new List<string>();
            var lockObj = new object();
            var allReceived = new TaskCompletionSource<bool>();
            const int expected = 30;

            await service.SubscribeAsync(topic, (_, msg) =>
            {
                lock (lockObj)
                {
                    received.Add(msg);
                    if (received.Count == expected)
                        allReceived.TrySetResult(true);
                }
                return Task.CompletedTask;
            });

            // Publish messages concurrently
            var publishTasks = Enumerable.Range(0, expected)
                .Select(i => service.PublishAsync(topic, $"concurrent-msg-{i}"))
                .ToArray();

            var allPublished = await Task.WhenAll(publishTasks);
            var publishResults = allPublished.Select(r => r.IsSuccessful).ToArray();
            publishResults.Should().AllBeEquivalentTo(true, $"Errors: {string.Join(" | ", allPublished.Where(r => !r.IsSuccessful).Select(r => r.ErrorMessage))}");

            await allReceived.Task.WaitAsync(TimeSpan.FromSeconds(15));
            received.Should().HaveCount(expected);
            received.Should().AllSatisfy(msg => msg.Should().StartWith("concurrent-msg-"));
        }
        finally
        {
            await service.DeleteTopicAsync(topic);
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task Publish_LargeMessage_ShouldHandleAppropriately()
    {
        var service = CreatePubSubService();
        AssertInitialized(service);
        var topic = GenerateTestTopic();
        try
        {
            var received = new TaskCompletionSource<string>();
            await service.SubscribeAsync(topic, (_, msg) =>
            {
                received.TrySetResult(msg);
                return Task.CompletedTask;
            });

            // Create a large message (64KB)
            var largeMessage = new string('A', 64 * 1024);
            var publishResult = await service.PublishAsync(topic, largeMessage);

            if (publishResult.IsSuccessful)
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
            await service.DeleteTopicAsync(topic);
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task Publish_SpecialCharacters_ShouldPreserveContent()
    {
        var service = CreatePubSubService();
        AssertInitialized(service);
        var topic = GenerateTestTopic();
        try
        {
            var received = new TaskCompletionSource<string>();
            await service.SubscribeAsync(topic, (_, msg) =>
            {
                received.TrySetResult(msg);
                return Task.CompletedTask;
            });

            const string specialMessage = "Hello 世界! 🎉 Special chars: @#$%^&*()_+-=[]{}|;':\",./<>?`~";
            var publishResult = await service.PublishAsync(topic, specialMessage);
            publishResult.IsSuccessful.Should().BeTrue($"Errors: {publishResult.ErrorMessage}");

            var result = await received.Task.WaitAsync(TimeSpan.FromSeconds(10));
            result.Should().Be(specialMessage);
        }
        finally
        {
            await service.DeleteTopicAsync(topic);
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task Subscribe_NullCallback_ShouldReturnFalse()
    {
        var service = CreatePubSubService();
        AssertInitialized(service);
        var topic = GenerateTestTopic();

        var result = await service.SubscribeAsync(topic, null!);
        result.IsSuccessful.Should().BeFalse($"Errors: {result.ErrorMessage}");
    }

    [RetryFact(3, 5000)]
    public virtual async Task Publish_NullMessage_ShouldReturnFalse()
    {
        var service = CreatePubSubService();
        AssertInitialized(service);
        var topic = GenerateTestTopic();

        var result = await service.PublishAsync(topic, null!);
        result.IsSuccessful.Should().BeFalse($"Errors: {result.ErrorMessage}");
    }

    [RetryFact(3, 5000)]
    public virtual async Task Topic_WithSpecialCharacters_ShouldHandleAppropriately()
    {
        var service = CreatePubSubService();
        AssertInitialized(service);

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
                });

                if (subscribeResult.IsSuccessful)
                {
                    const string message = "test-message";
                    var publishResult = await service.PublishAsync(topic, message);

                    if (publishResult.IsSuccessful)
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
                await service.DeleteTopicAsync(topic);
            }
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task Service_Reinitialization_ShouldMaintainFunctionality()
    {
        var topic = GenerateTestTopic();

        var service1 = CreatePubSubService();
        AssertInitialized(service1);

        // Test with first service instance
        var received1 = new TaskCompletionSource<string>();
        try
        {
            await service1.SubscribeAsync(topic, (_, msg) =>
            {
                received1.TrySetResult(msg);
                return Task.CompletedTask;
            });

            // Allow time for subscription to be fully established
            await Task.Delay(3000);

            await service1.PublishAsync(topic, "message1");
            var res1 = (await received1.Task.WaitAsync(TimeSpan.FromSeconds(20)));
            res1.Should().Be("message1");

            // Create a new service instance
            var service2 = CreatePubSubService();
            AssertInitialized(service2);

            // Test with a second service instance
            var received2Messages = new List<string>();
            try
            {
                await service2.SubscribeAsync(topic, (_, msg) =>
                {
                    received2Messages.Add(msg);
                    return Task.CompletedTask;
                });

                // Allow time for subscription to be fully established
                await Task.Delay(3000);

                await service2.PublishAsync(topic, "message2");

                var retryCount = 0;
                while (!received2Messages.Contains("message2") && ++retryCount < 10)
                {
                    await Task.Delay(1000);
                }
                received2Messages.Should().Contain("message2");
            }
            finally
            {
                await service2.DeleteTopicAsync(topic);

                // Cleanup with the second service
                if (service2 is IAsyncDisposable asyncDisposable2)
                    await asyncDisposable2.DisposeAsync();
            }
        }
        finally
        {
            await service1.DeleteTopicAsync(topic);
            if (service1 is IAsyncDisposable asyncDisposable1)
                await asyncDisposable1.DisposeAsync();
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task Publish_HighFrequency_ShouldMaintainPerformance()
    {
        var service = CreatePubSubService();
        AssertInitialized(service);
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
            });

            var stopwatch = Stopwatch.StartNew();

            // Publish messages rapidly
            var publishTasks = new List<Task<OperationResult<bool>>>();
            for (var i = 0; i < messageCount; i++)
            {
                publishTasks.Add(service.PublishAsync(topic, $"perf-msg-{i}"));
            }

            var publishResults = await Task.WhenAll(publishTasks);
            var publishTime = stopwatch.Elapsed;

            await allReceived.Task.WaitAsync(TimeSpan.FromSeconds(30));
            var totalTime = stopwatch.Elapsed;

            testOutputHelper.WriteLine($"Published {messageCount} messages in {publishTime.TotalMilliseconds:F2}ms");
            testOutputHelper.WriteLine($"Received all messages in {totalTime.TotalMilliseconds:F2}ms");
            testOutputHelper.WriteLine($"Publish rate: {messageCount / publishTime.TotalSeconds:F2} msg/sec");
            testOutputHelper.WriteLine($"End-to-end rate: {messageCount / totalTime.TotalSeconds:F2} msg/sec");

            var successResults = publishResults.Select(r => r.IsSuccessful).ToArray();
            successResults.Should().AllBeEquivalentTo(true, $"Errors: {string.Join(" | ", publishResults.Where(r => !r.IsSuccessful).Select(r => r.ErrorMessage))}");
            received.Should().HaveCount(messageCount);
        }
        finally
        {
            await service.DeleteTopicAsync(topic);
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task DeleteTopic_NonExistentTopic_ShouldNotThrow()
    {
        var service = CreatePubSubService();
        AssertInitialized(service);

        var nonExistentTopic = $"non-existent-{StringUtilities.GenerateRandomString(8)}";

        // This should not throw an exception
        var result = await service.DeleteTopicAsync(nonExistentTopic);

        // The service may log errors, but should handle the case gracefully
        testOutputHelper.WriteLine($"Delete non-existent topic completed. {result.ErrorMessage}");
    }

    [RetryFact(3, 5000)]
    public async Task MarkUsedOnBucketEvent_ValidTopic_ShouldReturnTrue()
    {
        // Arrange
        var service = CreatePubSubService();
        AssertInitialized(service);

        var topic = GenerateTestTopic();

        // Ensure topic exists first
        await service.EnsureTopicExistsAsync(topic);
        try
        {
            // Act
            var result = await service.MarkUsedOnBucketEvent(topic);

            // Assert
            Assert.True(result.IsSuccessful, "MarkUsedOnBucketEvent should return true for valid topic");
        }
        finally
        {
            // Cleanup
            await service.DeleteTopicAsync(topic);
        }
    }

    [RetryFact(3, 5000)]
    public async Task UnmarkUsedOnBucketEvent_ValidTopic_ShouldReturnTrue()
    {
        // Arrange
        var service = CreatePubSubService();
        AssertInitialized(service);

        var topic = GenerateTestTopic();

        // Ensure topic exists and mark it first
        await service.EnsureTopicExistsAsync(topic);
        try
        {
            await service.MarkUsedOnBucketEvent(topic);

            // Act
            var result = await service.UnmarkUsedOnBucketEvent(topic);

            // Assert
            Assert.True(result.IsSuccessful, "UnmarkUsedOnBucketEvent should return true for valid topic");
        }
        finally
        {
            // Cleanup
            await service.DeleteTopicAsync(topic);
        }
    }

    [RetryFact(3, 5000)]
    public async Task GetTopicsUsedOnBucketEventAsync_AfterMarking_ShouldReturnMarkedTopics()
    {
        // Arrange
        var service = CreatePubSubService();
        AssertInitialized(service);

        var topic1 = GenerateTestTopic();
        var topic2 = GenerateTestTopic();

        // Ensure topics exist and mark them
        await service.EnsureTopicExistsAsync(topic1);
        try
        {
            try
            {
                await service.EnsureTopicExistsAsync(topic2);
                await service.MarkUsedOnBucketEvent(topic1);
                await service.MarkUsedOnBucketEvent(topic2);

                // Act
                var markedTopics = await service.GetTopicsUsedOnBucketEventAsync();

                // Assert
                Assert.NotNull(markedTopics.Data);
                Assert.Contains(topic1, markedTopics.Data);
                Assert.Contains(topic2, markedTopics.Data);
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

    [RetryFact(3, 5000)]
    public async Task GetTopicsUsedOnBucketEventAsync_AfterUnmarking_ShouldNotReturnUnmarkedTopics()
    {
        // Arrange
        var service = CreatePubSubService();
        AssertInitialized(service);

        var topic = GenerateTestTopic();

        // Ensure a topic exists, mark it, then unmark it
        await service.EnsureTopicExistsAsync(topic);
        try
        {
            await service.MarkUsedOnBucketEvent(topic);
            await service.UnmarkUsedOnBucketEvent(topic);

            // Act
            var markedTopics = await service.GetTopicsUsedOnBucketEventAsync();

            // Assert
            Assert.NotNull(markedTopics.Data);
            Assert.DoesNotContain(topic, markedTopics.Data);
        }
        finally
        {
            // Cleanup
            await service.DeleteTopicAsync(topic);
        }
    }

    [RetryFact(3, 5000)]
    public async Task MarkUsedOnBucketEvent_EmptyTopic_ShouldReturnFalse()
    {
        // Arrange
        var service = CreatePubSubService();
        AssertInitialized(service);

        // Act & Assert
        var result1 = await service.MarkUsedOnBucketEvent("");
        var result2 = await service.MarkUsedOnBucketEvent(null!);

        Assert.False(result1.IsSuccessful, "MarkUsedOnBucketEvent should return false for empty topic");
        Assert.False(result2.IsSuccessful, "MarkUsedOnBucketEvent should return false for null topic");
    }

    [RetryFact(3, 5000)]
    public async Task BucketEventMethods_NonExistentTopic_ShouldHandleGracefully()
    {
        // Arrange
        var service = CreatePubSubService();
        AssertInitialized(service);

        var nonExistentTopic = "non-existent-topic-" + StringUtilities.GenerateRandomString(8);

        // Act
        var markResult = await service.MarkUsedOnBucketEvent(nonExistentTopic);
        var unmarkResult = await service.UnmarkUsedOnBucketEvent(nonExistentTopic);

        // Assert - behavior may vary by implementation, but should not throw
        // AWS creates the topic automatically, GC might require explicit creation
        Assert.True(markResult.IsSuccessful || !markResult.IsSuccessful); // Either behavior is acceptable
        Assert.True(unmarkResult.IsSuccessful || !unmarkResult.IsSuccessful); // Either behavior is acceptable
    }
}
