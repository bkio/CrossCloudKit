// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using CrossCloudKit.Interfaces;

namespace CrossCloudKit.PubSub.Basic;

/// <summary>
/// In-process implementation of IPubSubService using concurrent collections and event handling.
/// </summary>
public sealed class PubSubServiceBasic : IPubSubService, IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, ConcurrentBag<SubscriptionInfo>> _topicSubscriptions = new();
    private readonly ConcurrentBag<string> _bucketEventTopics = [];
    private readonly Lock _lockObject = new();
    private bool _disposed;

    private record SubscriptionInfo(Func<string, string, Task> OnMessage, Action<Exception>? OnError);

    public bool IsInitialized => !_disposed;

    /// <inheritdoc />
    public Task<OperationResult<bool>> EnsureTopicExistsAsync(string topic, CancellationToken cancellationToken = default)
    {
        if (_disposed)
            return Task.FromResult(OperationResult<bool>.Failure("Service has been disposed"));

        if (!IsInitialized)
            return Task.FromResult(OperationResult<bool>.Failure("Service is not initialized"));

        return Task.FromResult(string.IsNullOrEmpty(topic)
            ? OperationResult<bool>.Failure("Topic cannot be empty.")
            : OperationResult<bool>.Success(true));

        // For in-memory pub/sub, ensuring a topic exists simply means we're ready to handle it
        // Topics are created implicitly when the first subscription or message is published
    }

    /// <inheritdoc />
    public Task<OperationResult<bool>> SubscribeAsync(
        string topic,
        Func<string, string, Task>? onMessage,
        Action<Exception>? onError = null,
        CancellationToken cancellationToken = default)
    {
        if (_disposed)
            return Task.FromResult(OperationResult<bool>.Failure("Service has been disposed"));

        if (!IsInitialized)
            return Task.FromResult(OperationResult<bool>.Failure("Service is not initialized"));

        if (string.IsNullOrEmpty(topic))
            return Task.FromResult(OperationResult<bool>.Failure("Topic cannot be empty."));

        if (onMessage == null)
            return Task.FromResult(OperationResult<bool>.Failure("Callback cannot be null."));

        try
        {
            var subscriptionInfo = new SubscriptionInfo(onMessage, onError);
            var subscribers = _topicSubscriptions.GetOrAdd(topic, _ => []);
            subscribers.Add(subscriptionInfo);

            return Task.FromResult(OperationResult<bool>.Success(true));
        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<bool>.Failure($"Failed to subscribe to topic '{topic}': {ex.Message}"));
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> PublishAsync(string topic, string message, CancellationToken cancellationToken = default)
    {
        if (_disposed)
            return OperationResult<bool>.Failure("Service has been disposed");

        if (!IsInitialized)
            return OperationResult<bool>.Failure("Service is not initialized");

        if (string.IsNullOrEmpty(topic) || string.IsNullOrEmpty(message))
            return OperationResult<bool>.Failure("Topic and message cannot be empty.");

        try
        {
            if (!_topicSubscriptions.TryGetValue(topic, out var subscribers) || subscribers.IsEmpty)
            {
                // No subscribers for this topic - message is lost (similar to Redis pub/sub behavior)
                return OperationResult<bool>.Success(true);
            }

            // Create tasks for all message deliveries
            var deliveryTasks = subscribers.Select(subscriber => DeliverMessageAsync(subscriber, topic, message)).ToList();

            // Wait for all message deliveries to complete
            if (deliveryTasks.Count > 0)
            {
                await Task.WhenAll(deliveryTasks).ConfigureAwait(false);
            }

            return OperationResult<bool>.Success(true);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure($"Failed to publish message to topic '{topic}': {ex.Message}");
        }
    }

    /// <inheritdoc />
    public Task<OperationResult<bool>> DeleteTopicAsync(string topic, CancellationToken cancellationToken = default)
    {
        if (_disposed)
            return Task.FromResult(OperationResult<bool>.Success(true)); // Already disposed, consider topic deleted

        if (!IsInitialized)
            return Task.FromResult(OperationResult<bool>.Failure("Service is not initialized"));

        if (string.IsNullOrEmpty(topic))
            return Task.FromResult(OperationResult<bool>.Failure("Topic cannot be empty."));

        try
        {
            // Remove all subscriptions for this topic
            _topicSubscriptions.TryRemove(topic, out _);

            return Task.FromResult(OperationResult<bool>.Success(true));
        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<bool>.Failure($"Failed to delete topic '{topic}': {ex.Message}"));
        }
    }

    /// <inheritdoc />
    public Task<OperationResult<bool>> MarkUsedOnBucketEvent(string topic, CancellationToken cancellationToken = default)
    {
        if (_disposed)
            return Task.FromResult(OperationResult<bool>.Failure("Service has been disposed"));

        if (string.IsNullOrEmpty(topic))
            return Task.FromResult(OperationResult<bool>.Failure("Topic cannot be empty."));

        if (!IsInitialized)
            return Task.FromResult(OperationResult<bool>.Failure("Service is not initialized"));

        try
        {
            lock (_lockObject)
            {
                // Check if a topic is already marked
                if (_bucketEventTopics.Contains(topic))
                    return Task.FromResult(OperationResult<bool>.Success(true));

                _bucketEventTopics.Add(topic);
                return Task.FromResult(OperationResult<bool>.Success(true));
            }
        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<bool>.Failure($"Failed to mark topic '{topic}' as used on bucket event: {ex.Message}"));
        }
    }

    /// <inheritdoc />
    public Task<OperationResult<bool>> UnmarkUsedOnBucketEvent(string topic, CancellationToken cancellationToken = default)
    {
        if (_disposed)
            return Task.FromResult(OperationResult<bool>.Failure("Service has been disposed"));

        if (string.IsNullOrEmpty(topic))
            return Task.FromResult(OperationResult<bool>.Failure("Topic cannot be empty."));

        if (!IsInitialized)
            return Task.FromResult(OperationResult<bool>.Failure("Service is not initialized"));

        try
        {
            lock (_lockObject)
            {
                // Create a new bag without the topic to remove
                var newBag = new ConcurrentBag<string>();
                foreach (var existingTopic in _bucketEventTopics)
                {
                    if (existingTopic != topic)
                    {
                        newBag.Add(existingTopic);
                    }
                }

                // Replace the old bag with the new one
                _bucketEventTopics.Clear();
                foreach (var remainingTopic in newBag)
                {
                    _bucketEventTopics.Add(remainingTopic);
                }

                return Task.FromResult(OperationResult<bool>.Success(true));
            }
        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<bool>.Failure($"Failed to unmark topic '{topic}' from bucket event: {ex.Message}"));
        }
    }

    /// <inheritdoc />
    public Task<OperationResult<List<string>>> GetTopicsUsedOnBucketEventAsync(CancellationToken cancellationToken = default)
    {
        if (_disposed)
            return Task.FromResult(OperationResult<List<string>>.Failure("Service has been disposed"));

        if (!IsInitialized)
            return Task.FromResult(OperationResult<List<string>>.Failure("Service is not initialized"));

        try
        {
            lock (_lockObject)
            {
                var topics = _bucketEventTopics.ToList();
                return Task.FromResult(OperationResult<List<string>>.Success(topics));
            }
        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<List<string>>.Failure($"Failed to get bucket event topics: {ex.Message}"));
        }
    }

    /// <summary>
    /// Not relevant for Basic Pub/Sub
    /// </summary>
    public Task<OperationResult<bool>> AWSSpecific_AddSnsS3PolicyAsync(string snsTopicArn, string bucketArn, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(OperationResult<bool>.Success(true));
    }

    /// <summary>
    /// Not relevant for Basic Pub/Sub
    /// </summary>
    public Task<OperationResult<bool>> AWSSpecific_RemoveSnsS3PolicyAsync(string encodedTopic, string bucketArn,
        CancellationToken cancellationToken = default)
    {
        return Task.FromResult(OperationResult<bool>.Success(true));
    }

    private static async Task DeliverMessageAsync(
        SubscriptionInfo subscriber,
        string topic,
        string message)
    {
        try
        {
            await subscriber.OnMessage(topic, message).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            try
            {
                // Notify the subscriber's error handler if available
                subscriber.OnError?.Invoke(ex);
            }
            catch (Exception)
            {
                // ignored
            }
        }
    }

    /// <summary>
    /// Dispose the service asynchronously
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        try
        {
            // Clear all subscriptions
            _topicSubscriptions.Clear();

            // Clear bucket event topics
            lock (_lockObject)
            {
                _bucketEventTopics.Clear();
            }
        }
        catch (Exception)
        {
            // Ignore disposal errors
        }

        await Task.CompletedTask;
        // ReSharper disable once GCSuppressFinalizeForTypeWithoutDestructor
        GC.SuppressFinalize(this);
    }
}
