// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Amazon.SQS;
using Amazon.SQS.Model;
using CrossCloudKit.Interfaces;
using System.Collections.Concurrent;
using CrossCloudKit.Utilities.Common;

namespace CrossCloudKit.PubSub.AWS;

/// <summary>
/// Modern AWS SQS pub/sub service implementation with async/await patterns
/// Uses individual queues per subscriber to achieve fanout behavior
/// </summary>
public sealed class PubSubServiceAWS : IPubSubService, IAsyncDisposable
{
    private const string SlaveSeparator = "-slave-";
    private record GetQueueListCacheEntry(List<string> Queues, DateTime CachedAt);

    private readonly AmazonSQSClient _sqsClient;
    private readonly ConcurrentDictionary<string, List<CancellationTokenSource>> _subscriptions = new();
    private readonly Dictionary<string, HashSet<string>> _queuesNotTs = [];
    private readonly Dictionary<string, GetQueueListCacheEntry> _queueListCacheNotTs = [];
    private readonly HashSet<string> _s3EventTopicsNotTs = [];

    public bool IsInitialized { get; }

    /// <summary>
    /// Constructor for AWS SQS pub/sub service
    /// </summary>
    public PubSubServiceAWS(
        string accessKey,
        string secretKey,
        string region,
        Action<string>? errorMessageAction = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(accessKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(secretKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(region);

        try
        {
            _sqsClient = new AmazonSQSClient(
                new Amazon.Runtime.BasicAWSCredentials(accessKey, secretKey),
                Amazon.RegionEndpoint.GetBySystemName(region));

            InitializeS3EventForwardingAsyncNotSafe().Wait();

            IsInitialized = true;
        }
        catch (Exception e)
        {
            errorMessageAction?.Invoke($"PubSubServiceAWS initialization failed: {e.Message}");
            IsInitialized = false;
            _sqsClient = null!;
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> EnsureTopicExistsAsync(string topic, CancellationToken cancellationToken = default)
    {
        var result = await InternalEnsureTopicExistsAsync(topic, false, cancellationToken);
        if (!result.IsSuccessful || result.Data == null || result.Data.Count == 0)
            return OperationResult<bool>.Failure("Operation has failed.");
        return OperationResult<bool>.Success(true);
    }
    private async Task<OperationResult<List<string>>> InternalEnsureTopicExistsAsync(string topic, bool forceAddNew, CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || string.IsNullOrWhiteSpace(topic))
            return OperationResult<List<string>>.Failure("Not initialized or parameters are invalid.");

        List<string> relevantQueues;
        try
        {
            relevantQueues = await GetQueueListNotSafe(topic, cancellationToken);
            if (!forceAddNew && relevantQueues.Count > 0)
                return OperationResult<List<string>>.Success(relevantQueues);
        }
        catch (RequestThrottledException)
        {
            return await RequestThrottledRetry(() => InternalEnsureTopicExistsAsync(topic, forceAddNew, cancellationToken), cancellationToken);
        }
        catch (Exception e)
        {
            return OperationResult<List<string>>.Failure($"Failed to ensure topic exists(1): {e.Message}");
        }

        var newQueueName = relevantQueues.Count > 0 ? $"{topic}{SlaveSeparator}{StringUtilities.GenerateRandomString(8, true)}" : topic;
        try
        {
            await _sqsClient.CreateQueueAsync(new CreateQueueRequest
            {
                QueueName = newQueueName
            }, cancellationToken);
        }
        catch (RequestThrottledException)
        {
            return await RequestThrottledRetry(() => InternalEnsureTopicExistsAsync(topic, forceAddNew, cancellationToken), cancellationToken);
        }
        catch (Exception e)
        {
            return OperationResult<List<string>>.Failure($"Failed to ensure topic exists(2): {e.Message}");
        }

        var setRightsResult = await SetSqsInstanceRights(newQueueName, cancellationToken);
        if (!setRightsResult.IsSuccessful)
            return OperationResult<List<string>>.Failure($"Failed to set queue policy: {setRightsResult.ErrorMessage}");

        relevantQueues.Add(newQueueName);
        lock (_queuesNotTs)
        {
            if (_queuesNotTs.TryGetValue(topic, out var finalQueues))
            {
                finalQueues.Add(newQueueName);
            }
        }

        return OperationResult<List<string>>.Success(relevantQueues);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> PublishAsync(string topic, string message, CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || string.IsNullOrWhiteSpace(topic) || string.IsNullOrWhiteSpace(message))
            return OperationResult<bool>.Failure("Not initialized or parameters are invalid.");

        var result = await InternalEnsureTopicExistsAsync(topic, false, cancellationToken);
        if (!result.IsSuccessful || result.Data == null || result.Data.Count == 0)
            return OperationResult<bool>.Failure("Operation has failed.");
        var relevantQueues = result.Data;

        try
        {
            var publishTasks = relevantQueues.Select(queueUrl => _sqsClient.SendMessageAsync(new SendMessageRequest
            {
                QueueUrl = queueUrl, // Use queue URL from relevantQueues
                MessageBody = message
            }, cancellationToken));

            // Run in parallel
            await Task.WhenAll(publishTasks);
        }
        catch (RequestThrottledException)
        {
            return await RequestThrottledRetry(() => PublishAsync(topic, message, cancellationToken), cancellationToken);
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure($"Publish failed: {e.Message}");
        }
        return OperationResult<bool>.Success(true);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> SubscribeAsync(
        string topic,
        Func<string, string, Task>? onMessage,
        Action<Exception>? onError = null,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || string.IsNullOrWhiteSpace(topic) || onMessage == null)
            return OperationResult<bool>.Failure("Not initialized or parameters are invalid.");

        try
        {
            var result = await InternalEnsureTopicExistsAsync(topic, true, cancellationToken);
            if (!result.IsSuccessful || result.Data == null || result.Data.Count == 0)
                return OperationResult<bool>.Failure("Operation has failed.");
            var relevantQueues = result.Data;

            var queueName = relevantQueues[^1]; // Use the last queue URL from relevantQueues

            var cts = new CancellationTokenSource();
            _subscriptions.AddOrUpdate(topic,
                [cts],
                (_, existing) => { existing.Add(cts); return existing; });

            // Start message polling in the background
            _ = Task.Run(async () => await PollMessagesAsync(topic, queueName, onMessage, onError, relevantQueues.Count == 1, cts.Token), cts.Token);
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure($"Subscribe failed: {e.Message}");
        }
        return OperationResult<bool>.Success(true);
    }

    private async Task PollMessagesAsync(
        string topic,
        string queueName,
        Func<string, string, Task> onMessage,
        Action<Exception>? onError,
        bool isMasterSubscription,
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var request = new ReceiveMessageRequest
                {
                    QueueUrl = queueName,
                    MaxNumberOfMessages = 10,
                    MessageAttributeNames = ["All"],
                    WaitTimeSeconds = 20,
                    VisibilityTimeout = 30
                };

                var response = await _sqsClient.ReceiveMessageAsync(request, cancellationToken);

                if (response?.Messages == null || response.Messages.Count == 0)
                    continue;

                var acknowledgedMessages = new Dictionary<string, string>();

                var deleteTasks = new List<Task>();
                foreach (var sqsMessage in response.Messages.Where(sqsMessage => acknowledgedMessages.TryAdd(sqsMessage.MessageId, sqsMessage.Body)))
                {
                    try
                    {
                        await onMessage(topic, sqsMessage.Body);
                    }
                    catch (Exception e)
                    {
                        onError?.Invoke(e);
                    }

                    deleteTasks.Add(_sqsClient.DeleteMessageAsync(new DeleteMessageRequest
                    {
                        QueueUrl = queueName,
                        ReceiptHandle = sqsMessage.ReceiptHandle
                    }, cancellationToken));
                }

                if (isMasterSubscription)
                {
                    var forwardTasks = new List<Task>();

                    var relevantQueues = await GetQueueListNotSafe(topic, cancellationToken);

                    lock (_s3EventTopicsNotTs)
                    {
                        if (_s3EventTopicsNotTs.Contains(topic) && relevantQueues.Count > 1)
                        {
                            forwardTasks.AddRange((from message in acknowledgedMessages.Values
                                from otherSubQueueUrl in relevantQueues.Where(queue => queue != queueName).ToList()
                                select _sqsClient.SendMessageAsync(new SendMessageRequest
                                {
                                    QueueUrl = otherSubQueueUrl, // Use queue URL from relevantQueues
                                    MessageBody = message
                                }, cancellationToken)));
                        }
                    }
                    await Task.WhenAll(forwardTasks);
                }

                await Task.WhenAll(deleteTasks);
            }
            catch (QueueDoesNotExistException)
            {
                break; // Queue was deleted, stop polling
            }
            catch (ObjectDisposedException)
            {
                break; // Operation has stopped, stop polling
            }
            catch (OperationCanceledException)
            {
                // Expected on cancellation
                break;
            }
            catch (Exception e)
            {
                onError?.Invoke(e);
                // Wait before retrying to avoid tight loop on persistent errors
                try
                {
                    await Task.Delay(5000, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> DeleteTopicAsync(string topic, CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || string.IsNullOrWhiteSpace(topic))
            return OperationResult<bool>.Failure("Not initialized or parameters are invalid.");

        // Cancel all subscriptions for this topic
        if (_subscriptions.TryRemove(topic, out var ctsList))
        {
            foreach (var cts in ctsList)
            {
                try
                {
                    await cts.CancelAsync();
                    cts.Dispose();
                }
                catch (Exception)
                {
                    //Ignored
                }
            }
        }

        try
        {
            var toBeDeletedQueues = await GetQueueListNotSafe(topic, cancellationToken);

            var deleteTasks = toBeDeletedQueues.Select(queueUrl => _sqsClient.DeleteQueueAsync(new DeleteQueueRequest()
            {
                QueueUrl = queueUrl
            }, cancellationToken));

            // Run in parallel
            await Task.WhenAll(deleteTasks);

            lock (_queuesNotTs)
            {
                _queuesNotTs.Remove(topic);
            }

            lock (_queueListCacheNotTs)
            {
                _queueListCacheNotTs.Remove(topic);
            }

            lock (_s3EventTopicsNotTs)
            {
                _s3EventTopicsNotTs.Remove(topic);
            }
        }
        catch (RequestThrottledException)
        {
            return await RequestThrottledRetry(() => DeleteTopicAsync(topic, cancellationToken), cancellationToken);
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure($"Failed to delete queue(1) {topic}: {e.Message}");
        }
        return OperationResult<bool>.Success(true);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> MarkUsedOnBucketEvent(string topic, CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || string.IsNullOrWhiteSpace(topic))
            return OperationResult<bool>.Failure("Not initialized or parameters are invalid.");

        lock (_s3EventTopicsNotTs)
        {
            _s3EventTopicsNotTs.Add(topic);
        }

        try
        {
            await _sqsClient.TagQueueAsync(new TagQueueRequest
            {
                QueueUrl = topic,
                Tags = new Dictionary<string, string>
                {
                    { IPubSubService.UsedOnBucketEventFlagKey, "true" }
                }
            }, cancellationToken);
        }
        catch (RequestThrottledException)
        {
            return await RequestThrottledRetry(() => MarkUsedOnBucketEvent(topic, cancellationToken), cancellationToken);
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure($"Failed to mark topic as used with bucket events {topic}: {e.Message}");
        }
        return OperationResult<bool>.Success(true);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> UnmarkUsedOnBucketEvent(string topic, CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || string.IsNullOrWhiteSpace(topic))
            return OperationResult<bool>.Failure("Not initialized or parameters are invalid.");

        lock (_s3EventTopicsNotTs)
        {
            _s3EventTopicsNotTs.Remove(topic);
        }

        try
        {
            await _sqsClient.UntagQueueAsync(new UntagQueueRequest
            {
                QueueUrl = topic,
                TagKeys = [IPubSubService.UsedOnBucketEventFlagKey]
            }, cancellationToken);
        }
        catch (RequestThrottledException)
        {
            return await RequestThrottledRetry(() => UnmarkUsedOnBucketEvent(topic, cancellationToken), cancellationToken);
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure($"Failed to unmark topic as used with bucket events {topic}: {e.Message}");
        }
        return OperationResult<bool>.Success(true);
    }

    /// <inheritdoc />
    public Task<OperationResult<List<string>>> GetTopicsUsedOnBucketEventAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            lock (_s3EventTopicsNotTs)
            {
                return Task.FromResult(OperationResult<List<string>>.Success(_s3EventTopicsNotTs.ToList()));
            }
        }
        catch (Exception e)
        {
            return Task.FromResult(OperationResult<List<string>>.Failure($"Failed to get topics: {e.Message}"));
        }
    }

    private static async Task<T> RequestThrottledRetry<T>(Func<Task<T>> onRetry, CancellationToken cancellationToken = default)
    {
        try
        {
            await Task.Delay(5000, cancellationToken);
            return await onRetry();
        }
        catch (OperationCanceledException)
        {
            //Ignored
        }
        return default!;
    }

    private async Task<List<string>> GetQueueListNotSafe(string topic, CancellationToken cancellationToken = default)
    {
        var relevantQueues = new List<string>();

        // Check cache first
        lock (_queueListCacheNotTs)
        {
            if (_queueListCacheNotTs.TryGetValue(topic, out var cached))
            {
                if (DateTime.UtcNow - cached.CachedAt >= TimeSpan.FromSeconds(5))
                {
                    _queueListCacheNotTs.Remove(topic); // Remove expired cache entry
                }
                else
                {
                    relevantQueues.AddRange(cached.Queues);
                }
            }
        }

        if (relevantQueues.Count == 0)
        {
            var allQueues = new List<string>();
            string? nextToken = null;

            do
            {
                var relevantQueuesResult = await _sqsClient.ListQueuesAsync(new ListQueuesRequest
                {
                    QueueNamePrefix = topic,
                    NextToken = nextToken,
                    MaxResults = 1000
                }, cancellationToken);

                if (relevantQueuesResult.QueueUrls is { Count: > 0 })
                {
                    allQueues.AddRange(relevantQueuesResult.QueueUrls);
                }

                nextToken = relevantQueuesResult.NextToken;

            } while (!string.IsNullOrEmpty(nextToken));

            GetQueueListCacheEntry cacheEntry;

            if (allQueues.Count > 0)
            {
                cacheEntry = new GetQueueListCacheEntry([.. allQueues], DateTime.UtcNow);

                relevantQueues.AddRange(
                    allQueues.Select(queueUrl =>
                        queueUrl[queueUrl.IndexOf(topic, StringComparison.InvariantCulture)..])
                );
            }
            else
            {
                cacheEntry = new GetQueueListCacheEntry([], DateTime.UtcNow);
            }

            // Update cache
            lock (_queueListCacheNotTs)
            {
                _queueListCacheNotTs[topic] = cacheEntry;
            }
        }

        lock (_queuesNotTs)
        {
            if (!_queuesNotTs.TryGetValue(topic, out var finalQueues))
            {
                finalQueues = [];
                _queuesNotTs.Add(topic, finalQueues);
            }

            foreach (var queueUrl in relevantQueues)
            {
                finalQueues.Add(queueUrl);
            }
            return [.. finalQueues]; //Create copy
        }
    }

    private async Task<OperationResult<bool>> SetSqsInstanceRights(string topicName, CancellationToken cancellationToken = default)
    {
        // Get queue URL
        var queueUrlResponse = await _sqsClient.GetQueueUrlAsync(topicName, cancellationToken).ConfigureAwait(false);
        var queueUrl = queueUrlResponse.QueueUrl;
        try
        {
            var setQueueAttributesRequest = new SetQueueAttributesRequest
            {
                QueueUrl = queueUrl,
                Attributes = new Dictionary<string, string>
                {
                    {
                        "Policy", $$"""
                          {
                              "Version": "2012-10-17",
                              "Id": "S3PublishPolicy",
                              "Statement": [
                                  {
                                      "Sid": "AllowS3PublishToSQS",
                                      "Effect": "Allow",
                                      "Principal": {
                                          "Service": "s3.amazonaws.com"
                                      },
                                      "Action": [
                                          "sqs:*"
                                      ],
                                      "Resource": "*"
                                  }
                              ]
                          }
                          """
                    }
                }
            };
            await _sqsClient.SetQueueAttributesAsync(setQueueAttributesRequest, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure($"Failed to set queue policy: {e.Message}");
        }
        return OperationResult<bool>.Success(true);
    }

    private async Task<List<string>> GetMasterQueuesTaggedWithUsedOnBucketEventAsyncNotSafe(CancellationToken cancellationToken = default)
    {
        var matchingQueues = new List<string>();
        string? nextToken = null;

        do
        {
            var listQueuesResponse = await _sqsClient.ListQueuesAsync(new ListQueuesRequest
            {
                NextToken = nextToken,
                MaxResults = 1000 // maximum allowed
            }, cancellationToken);

            foreach (var queueUrl in listQueuesResponse.QueueUrls)
            {
                var tagsResponse = await _sqsClient.ListQueueTagsAsync(new ListQueueTagsRequest
                {
                    QueueUrl = queueUrl
                }, cancellationToken);

                if (tagsResponse.Tags == null ||
                    !tagsResponse.Tags.ContainsKey(IPubSubService.UsedOnBucketEventFlagKey)) continue;

                if (!queueUrl.Contains(SlaveSeparator))
                    matchingQueues.Add(new Uri(queueUrl).Segments.Last());
            }

            nextToken = listQueuesResponse.NextToken;

        } while (!string.IsNullOrEmpty(nextToken));

        return matchingQueues;
    }
    private async Task InitializeS3EventForwardingAsyncNotSafe(CancellationToken cancellationToken = default)
    {
        var masterQueues = await GetMasterQueuesTaggedWithUsedOnBucketEventAsyncNotSafe(cancellationToken);
        foreach (var queueName in masterQueues)
        {
            lock (_s3EventTopicsNotTs)
            {
                _s3EventTopicsNotTs.Add(queueName);
            }
            //await SubscribeAsync(queueUrl, (_, _) => Task.CompletedTask, null, cancellationToken);
            //Logically there should be a listener already, if not, when a new listener is added; forwarding will begin anyway.
        }
    }

    public async ValueTask DisposeAsync()
    {
        // Cancel all subscriptions
        var cancellationTasks = _subscriptions.Values.Select(async ctsList =>
        {
            foreach (var cts in ctsList)
            {
                await cts.CancelAsync();
                cts.Dispose();
            }
        });

        await Task.WhenAll(cancellationTasks);

        _subscriptions.Clear();
        lock (_queuesNotTs)
        {
            _queuesNotTs.Clear();
        }
        lock (_queueListCacheNotTs)
        {
            _queueListCacheNotTs.Clear();
        }
        lock (_s3EventTopicsNotTs)
        {
            _s3EventTopicsNotTs.Clear();
        }
        _sqsClient.Dispose();
    }
}
