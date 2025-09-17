// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Text;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Utilities.Common;
using Newtonsoft.Json;
// ReSharper disable NullableWarningSuppressionIsUsed

namespace CrossCloudKit.PubSub.Basic;

/// <summary>
/// Cross-process implementation of IPubSubService using file-based storage and OS-level synchronization primitives.
/// Enables pub/sub messaging across multiple processes on the same machine.
/// </summary>
public sealed class PubSubServiceBasic : IPubSubService, IAsyncDisposable
{
    private readonly string _storageDirectory;
    private readonly ConcurrentDictionary<string, Timer> _messagePollingTimers = new();
    private readonly ConcurrentDictionary<string, List<SubscriptionInfo>> _localSubscriptions = new();
    private readonly ConcurrentDictionary<string, ConcurrentQueue<string>> _deliveredMessageIds = new();
    private readonly Timer _cleanupTimer;
    private readonly string _processId;
    private bool _disposed;

    private const string RootFolderName = "CrossCloudKit.PubSub.Basic";

    private record SubscriptionInfo(Func<string, string, Task> OnMessage, Action<Exception>? OnError);

    public PubSubServiceBasic()
    {
        _storageDirectory = Path.Combine(Path.GetTempPath(), RootFolderName);
        Directory.CreateDirectory(_storageDirectory);

        _processId = Environment.MachineName + ":" + Environment.ProcessId + ":" + Guid.NewGuid().ToString("N");

        // Start background cleanup every 5 minutes
        _cleanupTimer = new Timer(CleanupExpiredData, null, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));
    }

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
            // Register a subscription in the file system for cross-process visibility
            var regResult = RegisterSubscription(topic).Result;
            if (!regResult.IsSuccessful)
                return Task.FromResult(OperationResult<bool>.Failure($"Failed to register subscription for topic '{topic}': {regResult.ErrorMessage}"));

            var subscriptionInfo = new SubscriptionInfo(onMessage, onError);

            // Add to local subscriptions
            var localSubs = _localSubscriptions.GetOrAdd(topic, _ => []);
            lock (localSubs)
            {
                localSubs.Add(subscriptionInfo);
            }

            // Start polling for messages if this is the first subscription for this topic
            if (!_messagePollingTimers.ContainsKey(topic))
            {
                StartMessagePolling(topic);
            }

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
            // Generate message ID for deduplication
            var messageId = $"{_processId}#{DateTime.UtcNow.Ticks}#{Guid.NewGuid():N}";

            // Store message in file system for cross-process delivery
            var storeResult = await StoreMessageWithIdAsync(topic, message, messageId);
            if (!storeResult.IsSuccessful)
            {
                return OperationResult<bool>.Failure($"Failed to store message to topic '{topic}': {storeResult.ErrorMessage}");
            }

            // Deliver to local subscribers immediately with message ID
            await DeliverToLocalSubscribersAsync(topic, message, messageId);

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
            // Stop message polling for this topic
            if ( _messagePollingTimers.TryRemove(topic, out var timer))
            {
                timer.Dispose();
            }

            // Remove local subscriptions
            _localSubscriptions.TryRemove(topic, out _);

            // Clean up topic files
            CleanupTopicFiles(topic);

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
            var mutexWrapper = CreateMutex("bucket_events");
            if (!mutexWrapper.IsSuccessful)
                return Task.FromResult(OperationResult<bool>.Failure($"Failed to create mutex: {mutexWrapper.ErrorMessage}"));
            using var mutex = mutexWrapper.Data!;

            var bucketEventTopics = GetBucketEventTopics();
            if (bucketEventTopics.Contains(topic)) return Task.FromResult(OperationResult<bool>.Success(true));
            bucketEventTopics.Add(topic);
            SaveBucketEventTopics(bucketEventTopics);
            return Task.FromResult(OperationResult<bool>.Success(true));
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
            var mutexWrapper = CreateMutex("bucket_events");
            if (!mutexWrapper.IsSuccessful)
                return Task.FromResult(OperationResult<bool>.Failure($"Failed to create mutex: {mutexWrapper.ErrorMessage}"));
            using var mutex = mutexWrapper.Data!;

            var bucketEventTopics = GetBucketEventTopics();
            if (bucketEventTopics.Remove(topic))
            {
                SaveBucketEventTopics(bucketEventTopics);
            }
            return Task.FromResult(OperationResult<bool>.Success(true));
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
            var mutexWrapper = CreateMutex("bucket_events");
            if (!mutexWrapper.IsSuccessful)
                return Task.FromResult(OperationResult<List<string>>.Failure($"Failed to create mutex: {mutexWrapper.ErrorMessage}"));
            using var mutex = mutexWrapper.Data!;

            var topics = GetBucketEventTopics();
            return Task.FromResult(OperationResult<List<string>>.Success(topics));
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
            // Dispose cleanup timer
            try
            {
                await _cleanupTimer.DisposeAsync();
            }
            catch (Exception)
            {
                // ignored
            }

            // Dispose all message polling timers
            foreach (var timer in _messagePollingTimers.Values)
            {
                try
                {
                    await timer.DisposeAsync();
                }
                catch (Exception)
                {
                    // ignored
                }
            }
            _messagePollingTimers.Clear();

            // Unregister this process's subscriptions
            UnregisterAllSubscriptions();
        }
        catch (Exception)
        {
            // Ignore disposal errors
        }

        await Task.CompletedTask;
        // ReSharper disable once GCSuppressFinalizeForTypeWithoutDestructor
        GC.SuppressFinalize(this);
    }

    private record PubSubMessage(string MessageId, string Content, DateTime Timestamp, string PublisherId);

    private static OperationResult<AutoMutex> CreateMutex(string key)
    {
        // Create a safe mutex name from the key
        var mutexName = "CrossCloudKit.PubSub.Basic." + Convert.ToBase64String(Encoding.UTF8.GetBytes(key))
            .Replace('/', '_').Replace('+', '-').Replace("=", "");

        try
        {
            return OperationResult<AutoMutex>.Success(new AutoMutex(mutexName));
        }
        catch (Exception e)
        {
            return OperationResult<AutoMutex>.Failure($"Failed to create mutex: {e.Message}");
        }
    }

    private Task<OperationResult<bool>> StoreMessageWithIdAsync(string topic, string message, string messageId)
    {
        var pubSubMessage = new PubSubMessage(messageId, message, DateTime.UtcNow, _processId);

        var mutexWrapper = CreateMutex($"topic_{topic}");
        if (!mutexWrapper.IsSuccessful)
            return Task.FromResult(OperationResult<bool>.Failure($"Failed to create mutex: {mutexWrapper.ErrorMessage}"));
        using var mutex = mutexWrapper.Data!;

        var messagesFilePath = GetTopicMessagesFilePath(topic);
        var messages = LoadMessagesFromFile(messagesFilePath);
        messages.Add(pubSubMessage);

        // Keep only recent messages (last 100 or last hour)
        var cutoffTime = DateTime.UtcNow.AddHours(-1);
        messages = messages
            .Where(m => m.Timestamp > cutoffTime)
            .OrderByDescending(m => m.Timestamp)
            .Take(100)
            .ToList();

        SaveMessagesToFile(messagesFilePath, messages);

        return Task.FromResult(OperationResult<bool>.Success(true));
    }

    private async Task DeliverToLocalSubscribersAsync(string topic, string message, string? messageId = null)
    {
        if (!_localSubscriptions.TryGetValue(topic, out var subscribers))
            return;

        // Use message ID for proper deduplication
        if (!string.IsNullOrEmpty(messageId))
        {
            var deliveredQueue = _deliveredMessageIds.GetOrAdd(topic, _ => new ConcurrentQueue<string>());

            // Check if already delivered (convert to HashSet for O(1) lookup during check)
            var deliveredSet = new HashSet<string>(deliveredQueue);

            if (deliveredSet.Contains(messageId))
                return; // Already delivered this message

            // Add to queue
            deliveredQueue.Enqueue(messageId);

            // Clean up old entries if queue gets too large
            if (deliveredQueue.Count > 1000)
            {
                for (var i = 0; i < 500 && deliveredQueue.TryDequeue(out _); i++)
                {
                    // Remove oldest 500 entries
                }
            }
        }

        List<SubscriptionInfo> subscribersCopy;
        lock (subscribers)
        {
            subscribersCopy = new List<SubscriptionInfo>(subscribers);
        }

        var deliveryTasks = subscribersCopy.Select(subscriber => DeliverMessageAsync(subscriber, topic, message));
        await Task.WhenAll(deliveryTasks);
    }

    private static async Task DeliverMessageAsync(SubscriptionInfo subscriber, string topic, string message)
    {
        try
        {
            await subscriber.OnMessage(topic, message);
        }
        catch (Exception ex)
        {
            try
            {
                subscriber.OnError?.Invoke(ex);
            }
            catch (Exception)
            {
                // ignored
            }
        }
    }

    private Task<OperationResult<bool>> RegisterSubscription(string topic)
    {
        var mutexWrapper = CreateMutex($"subscriptions_{topic}");
        if (!mutexWrapper.IsSuccessful)
            return Task.FromResult(OperationResult<bool>.Failure($"Failed to create mutex: {mutexWrapper.ErrorMessage}"));
        using var mutex = mutexWrapper.Data;

        var subscriptionsFilePath = GetTopicSubscriptionsFilePath(topic);
        var subscriptions = LoadSubscriptionsFromFile(subscriptionsFilePath);

        // Remove old subscriptions for this process
        subscriptions.RemoveAll(s => s == _processId);

        // Add a new subscription
        subscriptions.Add(_processId);

        SaveSubscriptionsToFile(subscriptionsFilePath, subscriptions);
        return Task.FromResult(OperationResult<bool>.Success(true));
    }

    private void StartMessagePolling(string topic)
    {
        var timer = new Timer(async void (_) =>
            {
                try
                {
                    await PollForMessagesAsync(topic);
                }
                catch (Exception)
                {
                    // Ignore polling errors to prevent process crash
                    // Individual polling failures shouldn't stop the service
                }
            },
            null, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(2));
        _messagePollingTimers[topic] = timer;
    }

    private async Task PollForMessagesAsync(string topic)
    {
        if (_disposed || !_localSubscriptions.ContainsKey(topic))
            return;

        try
        {
            var mutexWrapper = CreateMutex($"topic_{topic}");
            if (!mutexWrapper.IsSuccessful)
                return;
            using var mutex = mutexWrapper.Data!;

            var messagesFilePath = GetTopicMessagesFilePath(topic);
            var messages = LoadMessagesFromFile(messagesFilePath);
            var undeliveredMessages = messages.Where(m => m.PublisherId != _processId).ToList();

            foreach (var message in undeliveredMessages)
            {
                await DeliverToLocalSubscribersAsync(topic, message.Content, message.MessageId);
            }

            // Mark messages as processed by removing old ones
            if (messages.Count != 0)
            {
                var cutoffTime = DateTime.UtcNow.AddMinutes(-5);
                var filteredMessages = messages.Where(m => m.Timestamp > cutoffTime).ToList();
                SaveMessagesToFile(messagesFilePath, filteredMessages);
            }
        }
        catch (Exception)
        {
            // Ignore polling errors
        }
    }

    private void CleanupTopicFiles(string topic)
    {
        try
        {
            var messagesFilePath = GetTopicMessagesFilePath(topic);
            if (File.Exists(messagesFilePath))
                File.Delete(messagesFilePath);

            var subscriptionsFilePath = GetTopicSubscriptionsFilePath(topic);
            if (File.Exists(subscriptionsFilePath))
                File.Delete(subscriptionsFilePath);
        }
        catch (Exception)
        {
            // Ignore cleanup errors
        }
    }

    private void CleanupExpiredData(object? state)
    {
        if (_disposed)
            return;

        try
        {
            if (!Directory.Exists(_storageDirectory))
                return;

            var files = Directory.GetFiles(_storageDirectory, "*.json");
            var cutoffTime = DateTime.UtcNow.AddHours(-24);

            foreach (var file in files)
            {
                try
                {
                    var fileInfo = new FileInfo(file);
                    if (fileInfo.LastWriteTime < cutoffTime)
                        File.Delete(file);
                }
                catch (Exception)
                {
                    // Ignore errors for individual files
                }
            }

            // Clean up delivered message IDs for topics that no longer have active subscriptions
            foreach (var topic in _deliveredMessageIds.Keys.ToList())
            {
                if (!_localSubscriptions.ContainsKey(topic))
                {
                    _deliveredMessageIds.TryRemove(topic, out _);
                }
                else
                {
                    // Limit the size of delivered message queues
                    var deliveredQueue = _deliveredMessageIds.GetOrAdd(topic, _ => new ConcurrentQueue<string>());

                    if (deliveredQueue.Count <= 2000) continue;

                    // Remove oldest entries from the queue
                    for (var i = 0; i < 1000 && deliveredQueue.TryDequeue(out _); i++)
                    {
                    }
                }
            }

        }
        catch (Exception)
        {
            // Ignore cleanup errors
        }
    }

    private void UnregisterAllSubscriptions()
    {
        foreach (var topic in _localSubscriptions.Keys.ToList())
        {
            try
            {
                var mutexWrapper = CreateMutex($"subscriptions_{topic}");
                if (!mutexWrapper.IsSuccessful)
                    return;
                using var mutex = mutexWrapper.Data!;

                var subscriptionsFilePath = GetTopicSubscriptionsFilePath(topic);
                var subscriptions = LoadSubscriptionsFromFile(subscriptionsFilePath);
                subscriptions.RemoveAll(s => s == _processId);
                SaveSubscriptionsToFile(subscriptionsFilePath, subscriptions);
            }
            catch (Exception)
            {
                // Ignore errors during cleanup
            }
        }

        // Clear all subscriptions
        _localSubscriptions.Clear();
    }

    private List<string> GetBucketEventTopics()
    {
        var filePath = GetBucketEventTopicsFilePath();
        if (!File.Exists(filePath))
            return [];

        try
        {
            var json = File.ReadAllText(filePath, Encoding.UTF8);
            return JsonConvert.DeserializeObject<List<string>>(json) ?? [];
        }
        catch (Exception)
        {
            return [];
        }
    }

    private void SaveBucketEventTopics(List<string> topics)
    {
        var filePath = GetBucketEventTopicsFilePath();
        if (topics.Count == 0)
        {
            if (File.Exists(filePath))
                File.Delete(filePath);
            return;
        }
        var json = JsonConvert.SerializeObject(topics, Formatting.None);
        FileSystemUtilities.WriteToFileEnsureWrittenToDisk(json, filePath);
    }

    private static List<PubSubMessage> LoadMessagesFromFile(string filePath)
    {
        if (!File.Exists(filePath))
            return [];

        try
        {
            var json = File.ReadAllText(filePath, Encoding.UTF8);
            return JsonConvert.DeserializeObject<List<PubSubMessage>>(json) ?? [];
        }
        catch (Exception)
        {
            return [];
        }
    }

    private static void SaveMessagesToFile(string filePath, List<PubSubMessage> messages)
    {
        if (messages.Count == 0)
        {
            if (File.Exists(filePath))
                File.Delete(filePath);
            return;
        }
        var json = JsonConvert.SerializeObject(messages, Formatting.None);
        FileSystemUtilities.WriteToFileEnsureWrittenToDisk(json, filePath);
    }

    private static List<string> LoadSubscriptionsFromFile(string filePath)
    {
        if (!File.Exists(filePath))
            return [];

        try
        {
            var json = File.ReadAllText(filePath, Encoding.UTF8);
            return JsonConvert.DeserializeObject<List<string>>(json) ?? [];
        }
        catch (Exception)
        {
            return [];
        }
    }

    private static void SaveSubscriptionsToFile(string filePath, List<string> subscriptions)
    {
        if (subscriptions.Count == 0)
        {
            if (File.Exists(filePath))
                File.Delete(filePath);
            return;
        }
        var json = JsonConvert.SerializeObject(subscriptions, Formatting.None);
        FileSystemUtilities.WriteToFileEnsureWrittenToDisk(json, filePath);
    }

    private string GetTopicMessagesFilePath(string topic)
    {
        var fileName = Convert.ToBase64String(Encoding.UTF8.GetBytes($"messages_{topic}"))
            .Replace('/', '_').Replace('+', '-').Replace("=", "");
        return Path.Combine(_storageDirectory, $"{fileName}.json");
    }

    private string GetTopicSubscriptionsFilePath(string topic)
    {
        var fileName = Convert.ToBase64String(Encoding.UTF8.GetBytes($"subscriptions_{topic}"))
            .Replace('/', '_').Replace('+', '-').Replace("=", "");
        return Path.Combine(_storageDirectory, $"{fileName}.json");
    }

    private string GetBucketEventTopicsFilePath()
    {
        return Path.Combine(_storageDirectory, "bucket_event_topics.json");
    }
}
