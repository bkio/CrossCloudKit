// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using CrossCloudKit.Interfaces;
using System.Collections.Concurrent;
using CrossCloudKit.Utilities.Common;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CrossCloudKit.PubSub.AWS;

/// <summary>
/// AWS SNS+SQS hybrid pub/sub service implementation
/// Uses SNS for publishing and SQS for reliable subscription delivery
/// </summary>
public sealed class PubSubServiceAWS : IPubSubService, IAsyncDisposable
{
    private readonly string _region;
    private readonly Amazon.Runtime.AWSCredentials _credentials;
    private readonly ConcurrentDictionary<string, (string topicArn, AmazonSimpleNotificationServiceClient client)> _publishers = new();
    private readonly ConcurrentDictionary<string, ConcurrentBag<CancellationSiblings>> _subscriptions = new();
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, string>> _topicSubscriptions = new();

    private record CancellationSiblings(CancellationTokenSource Cts, Func<Task>? OnCancel);

    public bool IsInitialized { get; }

    /// <summary>
    /// Constructor for AWS SNS+SQS hybrid pub/sub service
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

        _region = region;

        try
        {
            _credentials = new Amazon.Runtime.BasicAWSCredentials(accessKey, secretKey);

            IsInitialized = true;
        }
        catch (Exception e)
        {
            errorMessageAction?.Invoke($"PubSubServiceAWS initialization failed: {e.Message}");
            IsInitialized = false;
            _credentials = null!;
        }
    }

    private async Task<(string topicArn, AmazonSimpleNotificationServiceClient client)> EnsureTopicExistsAndGetPublisherAsync(
        string encodedTopic,
        bool addToPublishers = true,
        CancellationToken cancellationToken = default)
    {
        if (_publishers.TryGetValue(encodedTopic, out var existing))
            return existing;

        var client = new AmazonSimpleNotificationServiceClient(
            _credentials,
            Amazon.RegionEndpoint.GetBySystemName(_region));

        var topicArn = await EnsureTopicExistsAsync(client, encodedTopic, cancellationToken);

        return addToPublishers ? _publishers.GetOrAdd(encodedTopic, (topicArn, client)) : (topicArn, client);
    }

    private static async Task<string> EnsureTopicExistsAsync(AmazonSimpleNotificationServiceClient client, string encodedTopicName, CancellationToken cancellationToken)
    {
        var listResponse = await GetAllTopics(client, cancellationToken);
        var existingTopic = listResponse.FirstOrDefault(t => t.TopicArn.EndsWith($":{encodedTopicName}"));

        if (existingTopic != null)
            return existingTopic.TopicArn;

        var createResponse = await client.CreateTopicAsync(new CreateTopicRequest
        {
            Name = encodedTopicName
        }, cancellationToken);

        return createResponse.TopicArn;
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> EnsureTopicExistsAsync(string topic, CancellationToken cancellationToken = default)
    {
        topic = EncodingUtilities.EncodeTopic(topic)!;
        try
        {
            await EnsureTopicExistsAndGetPublisherAsync(topic, false, cancellationToken);
            return OperationResult<bool>.Success(true);
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure($"EnsureTopicExistsAsync failed: {e.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> PublishAsync(string topic, string message, CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || string.IsNullOrWhiteSpace(topic) || string.IsNullOrWhiteSpace(message))
        {
            return OperationResult<bool>.Failure("Not initialized or parameters are invalid.");
        }

        topic = EncodingUtilities.EncodeTopic(topic)!;

        try
        {
            var (topicArn, client) = await EnsureTopicExistsAndGetPublisherAsync(topic, true, cancellationToken);

            await client.PublishAsync(new PublishRequest
            {
                TopicArn = topicArn,
                Message = message
            }, cancellationToken);

            return OperationResult<bool>.Success(true);
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure($"Publish failed: {e.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> SubscribeAsync(string topic, Func<string, string, Task>? onMessage, Action<Exception>? onError = null, CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || string.IsNullOrWhiteSpace(topic) || onMessage == null)
            return OperationResult<bool>.Failure("Not initialized or parameters are invalid.");

        topic = EncodingUtilities.EncodeTopic(topic)!;

        try
        {
            var cts = new CancellationTokenSource();

            // Start subscription in the background
            var ctsRegResult = await StartSubscriptionAsync(topic, onMessage, onError, cts.Token);
            if (!ctsRegResult.IsSuccessful) return OperationResult<bool>.Failure(ctsRegResult.ErrorMessage!);

            var sib = new CancellationSiblings(cts, ctsRegResult.Data!);
            _subscriptions.AddOrUpdate(topic,
                [sib],
                (_, existing) => { existing.Add(sib); return existing; });
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure($"Subscribe failed: {e.Message}");
        }
        return OperationResult<bool>.Success(true);
    }

    private async Task<OperationResult<Func<Task>?>> StartSubscriptionAsync(string encodedTopic, Func<string, string, Task> onMessage, Action<Exception>? onError, CancellationToken cancellationToken)
    {
        Func<Task>? cancelAction = null;

        try
        {
            using var snsClient = new AmazonSimpleNotificationServiceClient(_credentials,
                Amazon.RegionEndpoint.GetBySystemName(_region));
            var snsTopicArn = await EnsureTopicExistsAsync(snsClient, encodedTopic, cancellationToken);

            using var sqsClient = new AmazonSQSClient(_credentials,
                Amazon.RegionEndpoint.GetBySystemName(_region));

            // Create unique SQS queue for this subscription
            var sqsQueueName = $"{encodedTopic}-{StringUtilities.GenerateRandomString(8)}";
            var createQueueResponse = await sqsClient.CreateQueueAsync(new CreateQueueRequest
            {
                QueueName = sqsQueueName
            }, cancellationToken);

            var sqsQueueUrl = createQueueResponse.QueueUrl;

            // Get queue ARN
            var getQueueAttributesResponse = await sqsClient.GetQueueAttributesAsync(new GetQueueAttributesRequest
            {
                QueueUrl = sqsQueueUrl,
                AttributeNames = ["QueueArn"]
            }, cancellationToken);

            var sqsQueueArn = getQueueAttributesResponse.Attributes["QueueArn"];

            // Set SQS policy to allow SNS to send messages
            var policySetResult = await SetSqsPolicyAsync(
                sqsClient, sqsQueueUrl,
                cancellationToken);
            if (!policySetResult.IsSuccessful)
            {
                return OperationResult<Func<Task>?>.Failure(policySetResult.ErrorMessage!);
            }

            // Subscribe SQS queue to SNS topic
            var subscriptionResponse = await snsClient.SubscribeAsync(new SubscribeRequest
            {
                TopicArn = snsTopicArn,
                Protocol = "sqs",
                Endpoint = sqsQueueArn,

            }, cancellationToken);
            var subscriptionArn = subscriptionResponse.SubscriptionArn;

            // Track subscription queue for cleanup
            _topicSubscriptions.AddOrUpdate(encodedTopic,
                new ConcurrentDictionary<string, string>()
                {
                    [sqsQueueUrl] = subscriptionArn
                },
                (_, existing) =>
                {
                    existing.TryAdd(sqsQueueUrl, subscriptionArn);
                    return existing;
                });

            // Register cancellation to clean up resources
            cancelAction = async () =>
            {
                try
                {
                    using var localSnsClient = new AmazonSimpleNotificationServiceClient(_credentials,
                        Amazon.RegionEndpoint.GetBySystemName(_region));
                    using var localSqsClient = new AmazonSQSClient(_credentials,
                        Amazon.RegionEndpoint.GetBySystemName(_region));

                    if (!_topicSubscriptions.TryGetValue(encodedTopic, out var queueNameToSqsClient)
                        || !queueNameToSqsClient.TryRemove(sqsQueueUrl, out var localSubscriptionArn)) return;

                    try
                    {
                        await localSnsClient.UnsubscribeAsync(new UnsubscribeRequest
                        {
                            SubscriptionArn = localSubscriptionArn
                        }, CancellationToken.None);
                    }
                    catch (Exception)
                    {
                        // ignored
                    }

                    try
                    {
                        await localSqsClient.DeleteQueueAsync(new DeleteQueueRequest
                        {
                            QueueUrl = sqsQueueUrl
                        }, CancellationToken.None);
                    }
                    catch (Exception)
                    {
                        // ignored
                    }
                }
                catch
                {
                    // Ignore cleanup exceptions
                }
            };

            // Start message polling
            _ = Task.Run(
                () => PollMessagesAsync(encodedTopic, sqsQueueUrl, onMessage, onError, cancellationToken),
                cancellationToken);
        }
        catch (OperationCanceledException)
        {
            // Expected on cancellation
        }
        catch (Exception e)
        {
            if (cancelAction != null)
            {
                try
                {
                    await cancelAction();
                }
                catch (Exception)
                {
                    // Ignore cleanup exceptions
                }
            }
            return OperationResult<Func<Task>?>.Failure($"Start subscription failed: {e.Message}");
        }
        return OperationResult<Func<Task>?>.Success(cancelAction);
    }

    private async Task PollMessagesAsync(string encodedTopic, string sqsQueueUrl, Func<string, string, Task> onMessage, Action<Exception>? onError, CancellationToken cancellationToken)
    {
        using var sqsClient = new AmazonSQSClient(_credentials, Amazon.RegionEndpoint.GetBySystemName(_region));

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var receiveResponse = await sqsClient.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = sqsQueueUrl,
                    MaxNumberOfMessages = 10,
                    WaitTimeSeconds = 20,
                    VisibilityTimeout = 30,
                    MessageAttributeNames = ["All"]
                }, cancellationToken);

                if (receiveResponse?.Messages == null || receiveResponse.Messages.Count == 0)
                    continue;

                foreach (var sqsMessage in receiveResponse.Messages)
                {
                    string finalMessage;
                    try
                    {
                        var bodyObject = JObject.Parse(sqsMessage.Body);
                        var finalBody = new JObject()
                        {
                            ["body"] = bodyObject
                        };
                        if (bodyObject.TryGetValue("Type", out var typeTok) && typeTok.Type == JTokenType.String
                            && typeTok.Value<string>() == "Notification"
                            && bodyObject.TryGetValue("Message", out var messageTok) && messageTok.Type == JTokenType.String)
                        {
                            finalMessage = bodyObject["Message"]!.Value<string>()!;
                        }
                        else
                        {
                            if (sqsMessage.Attributes != null)
                            {
                                foreach (var attribute in sqsMessage.Attributes)
                                {
                                    finalBody[attribute.Key] = attribute.Value;
                                }
                            }
                            finalMessage = finalBody.ToString(Formatting.None);
                        }
                    }
                    catch (JsonReaderException)
                    {
                        finalMessage = sqsMessage.Body;
                    }

                    try
                    {
                        await onMessage(EncodingUtilities.DecodeTopic(encodedTopic)!, finalMessage);

                        // Delete message after successful processing
                        await sqsClient.DeleteMessageAsync(new DeleteMessageRequest
                        {
                            QueueUrl = sqsQueueUrl,
                            ReceiptHandle = sqsMessage.ReceiptHandle
                        }, cancellationToken);
                    }
                    catch (Exception e)
                    {
                        onError?.Invoke(e);
                        // Don't delete message on error - it will become visible again for retry
                    }
                }
            }
            catch (QueueDoesNotExistException)
            {
                break;
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception e)
            {
                onError?.Invoke(e);
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

    private static async Task<OperationResult<bool>> SetSqsPolicyAsync(
        AmazonSQSClient sqsClient,
        string sqsQueueUrl,
        CancellationToken cancellationToken)
    {
        try
        {
            await sqsClient.SetQueueAttributesAsync(new SetQueueAttributesRequest
            {
                QueueUrl = sqsQueueUrl,
                Attributes = new Dictionary<string, string>
                {
                    { "Policy", $$"""
                          {
                              "Version": "2012-10-17",
                              "Id": "SNSPublishPolicy",
                              "Statement": [
                                  {
                                      "Sid": "AllowSNSPublishToSQS",
                                      "Effect": "Allow",
                                      "Principal": {
                                          "Service": "sns.amazonaws.com"
                                      },
                                      "Action": [
                                          "sqs:*"
                                      ],
                                      "Resource": "*"
                                  }
                              ]
                          }
                      """ }
                }
            }, cancellationToken);
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure($"Set SQS policy failed: {e.Message}");
        }

        return OperationResult<bool>.Success(true);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> AWSSpecific_AddSnsS3PolicyAsync(string snsTopicArn, string bucketArn, CancellationToken cancellationToken = default)
    {
        try
        {
            using var snsClient = new AmazonSimpleNotificationServiceClient(_credentials,
                Amazon.RegionEndpoint.GetBySystemName(_region));

            await snsClient.SetTopicAttributesAsync(new SetTopicAttributesRequest()
            {
                TopicArn = snsTopicArn,
                AttributeName = "Policy",
                AttributeValue = $$"""
                                   {
                                       "Version": "2012-10-17",
                                       "Statement": [
                                           {
                                               "Sid": "AllowS3PublishToSNS{{StringUtilities.GenerateRandomString(4)}}",
                                               "Effect": "Allow",
                                               "Principal": {
                                                   "Service": "s3.amazonaws.com"
                                               },
                                               "Action": [
                                                   "sns:Publish"
                                               ],
                                               "Resource": "{{snsTopicArn}}",
                                               "Condition": {
                                                   "ArnLike": {
                                                       "aws:SourceArn": "{{bucketArn}}"
                                                   }
                                               }
                                           }
                                       ]
                                   }
                                   """
            }, cancellationToken);
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure($"Set SNS policy failed: {e.Message}");
        }
        return OperationResult<bool>.Success(true);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> AWSSpecific_RemoveSnsS3PolicyAsync(string encodedTopic, string bucketArn, CancellationToken cancellationToken = default)
    {
        try
        {
            using var snsClient = new AmazonSimpleNotificationServiceClient(_credentials,
                Amazon.RegionEndpoint.GetBySystemName(_region));

            var topics = await GetAllTopics(snsClient, cancellationToken);
            var relevantTopics = topics.Where(t => t.TopicArn.EndsWith($":{encodedTopic}"));

            foreach (var topic in relevantTopics)
            {
                var policy = await snsClient.GetTopicAttributesAsync(new GetTopicAttributesRequest
                {
                    TopicArn = topic.TopicArn
                }, cancellationToken);

                if (!policy.Attributes.TryGetValue("Policy", out var policyStr)) continue;

                var removeStatements = new List<JObject>();
                var allStatementsRemoved = false;

                if (!policyStr.Contains(bucketArn)) continue;

                var parsed = JObject.Parse(policyStr);
                if (parsed.TryGetValue("Statement", out var statements) && statements.Type == JTokenType.Array)
                {
                    var arr = (JArray)statements;
                    foreach (var statement in arr)
                    {
                        var statementObj = (JObject)statement;
                        if (!statementObj.TryGetValue("Condition", out var condition) ||
                            condition.Type != JTokenType.Object) continue;

                        var conditionObj = (JObject)condition;
                        if (!conditionObj.TryGetValue("ArnLike", out var arnLike) ||
                            arnLike.Type != JTokenType.Object) continue;

                        var arnLikeObj = (JObject)arnLike;
                        if ((string)arnLikeObj["aws:SourceArn"]! == bucketArn)
                        {
                            removeStatements.Add(statementObj);
                        }
                    }
                    foreach (var removeStatement in removeStatements.TakeWhile(_ => arr.Count != 1))
                    {
                        arr.Remove(removeStatement);
                    }
                    allStatementsRemoved = arr.Count == 0;
                }

                if (!allStatementsRemoved && removeStatements.Count > 0)
                {
                    await snsClient.SetTopicAttributesAsync(new SetTopicAttributesRequest
                    {
                        TopicArn = topic.TopicArn,
                        AttributeName = "Policy",
                        AttributeValue = parsed.ToString(Formatting.None)
                    }, cancellationToken);
                }
            }
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure($"Remove SNS policy failed: {e.Message}");
        }
        return OperationResult<bool>.Success(true);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> DeleteTopicAsync(string topic, CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || string.IsNullOrWhiteSpace(topic))
            return OperationResult<bool>.Failure("Not initialized or parameters are invalid.");

        topic = EncodingUtilities.EncodeTopic(topic)!;

        var errorMsg = new ConcurrentBag<string>();

        try
        {
            using var snsClient = new AmazonSimpleNotificationServiceClient(_credentials,
                Amazon.RegionEndpoint.GetBySystemName(_region));
            using var sqsClient = new AmazonSQSClient(_credentials,
                Amazon.RegionEndpoint.GetBySystemName(_region));

            // Cancel local subscription tracking
            if (_subscriptions.TryRemove(topic, out var cSibList))
            {
                foreach (var cSib in cSibList)
                {
                    if (cSib.OnCancel != null)
                    {
                        await cSib.OnCancel.Invoke();
                    }
                    await cSib.Cts.CancelAsync();
                    cSib.Cts.Dispose();
                }
            }

            // Delete all SQS queues for this topic
            if (_topicSubscriptions.TryRemove(topic, out var queueUrlToSqsClient))
            {
                var deleteTasks = new List<Task>();
                while (!queueUrlToSqsClient.IsEmpty)
                {
                    var sqsQueueUrl = queueUrlToSqsClient.Keys.First();
                    if (queueUrlToSqsClient.TryRemove(sqsQueueUrl, out var subscriptionArn))
                    {
                        deleteTasks.Add(Task.Run(async () =>
                        {
                            try
                            {
                                // ReSharper disable once AccessToDisposedClosure
                                await snsClient.UnsubscribeAsync(new UnsubscribeRequest
                                {
                                    SubscriptionArn = subscriptionArn
                                }, CancellationToken.None);
                            }
                            catch (NotFoundException)
                            {
                                // Subscription doesn't exist, that's fine
                            }
                            catch (Exception e)
                            {
                                errorMsg.Add(e.Message);
                            }

                            try
                            {
                                // ReSharper disable once AccessToDisposedClosure
                                await sqsClient.DeleteQueueAsync(new DeleteQueueRequest
                                {
                                    QueueUrl = sqsQueueUrl
                                }, cancellationToken);
                            }
                            catch (QueueDoesNotExistException)
                            {
                                // Queue doesn't exist, that's fine
                            }
                            catch (Exception e)
                            {
                                errorMsg.Add(e.Message);
                            }
                        }, cancellationToken));
                    }
                }

                if (deleteTasks.Count > 0)
                {
                    await Task.WhenAll(deleteTasks);
                }
            }

            // Remove publisher from cache and delete the topic
            if (_publishers.TryRemove(topic, out var topicArnAndSnsClient))
            {
                try
                {
                    await topicArnAndSnsClient.client.DeleteTopicAsync(new DeleteTopicRequest
                    {
                        TopicArn = topicArnAndSnsClient.topicArn
                    }, cancellationToken);
                }
                catch (NotFoundException)
                {
                    // Topic doesn't exist, that's fine
                }
                catch (Exception e)
                {
                    errorMsg.Add(e.Message);
                }

                try { topicArnAndSnsClient.client.Dispose(); }
                catch (Exception)
                {
                    // Ignore cleanup exceptions
                }
            }

            //Generic topic remove in case there is no publish/subscribe called before

            try
            {
                var listResponse = await GetAllTopics(snsClient, cancellationToken);
                var existingTopic = listResponse.FirstOrDefault(t => t.TopicArn.EndsWith($":{topic}"));
                if (existingTopic != null)
                {
                    await snsClient.DeleteTopicAsync(existingTopic.TopicArn, cancellationToken);
                }
            }
            catch (Exception e)
            {
                errorMsg.Add(e.Message);
            }
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure($"Delete topic failed: {e.Message}");
        }
        return !errorMsg.IsEmpty ? OperationResult<bool>.Failure($"Delete subscription failed: {string.Join(Environment.NewLine, errorMsg)}") : OperationResult<bool>.Success(true);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> MarkUsedOnBucketEvent(string topicName, CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || string.IsNullOrWhiteSpace(topicName))
            return OperationResult<bool>.Failure("Not initialized or parameters are invalid.");

        topicName = EncodingUtilities.EncodeTopic(topicName)!;

        try
        {
            using var snsClient = new AmazonSimpleNotificationServiceClient(_credentials,
                Amazon.RegionEndpoint.GetBySystemName(_region));

            var listResponse = await GetAllTopics(snsClient, cancellationToken);
            var existingTopic = listResponse.FirstOrDefault(t => t.TopicArn.EndsWith($":{topicName}"));
            if (existingTopic == null)
                return OperationResult<bool>.Failure($"Topic {topicName} does not exist.");

            await snsClient.TagResourceAsync(new TagResourceRequest
            {
                ResourceArn = existingTopic.TopicArn,
                Tags = [new Tag { Key = IPubSubService.UsedOnBucketEventFlagKey, Value = "true" }]
            }, cancellationToken);

            return OperationResult<bool>.Success(true);
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure(
                $"Failed to mark topic as used with bucket events {topicName}: {e.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> UnmarkUsedOnBucketEvent(string topicName, CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || string.IsNullOrWhiteSpace(topicName))
            return OperationResult<bool>.Failure("Not initialized or parameters are invalid.");

        topicName = EncodingUtilities.EncodeTopic(topicName)!;

        try
        {
            using var snsClient = new AmazonSimpleNotificationServiceClient(_credentials,
                Amazon.RegionEndpoint.GetBySystemName(_region));

            var listResponse = await GetAllTopics(snsClient, cancellationToken);
            var existingTopic = listResponse.FirstOrDefault(t => t.TopicArn.EndsWith($":{topicName}"));
            if (existingTopic == null)
                return OperationResult<bool>.Failure($"Topic {topicName} does not exist.");

            await snsClient.UntagResourceAsync(new UntagResourceRequest
            {
                ResourceArn = existingTopic.TopicArn,
                TagKeys = [IPubSubService.UsedOnBucketEventFlagKey]
            }, cancellationToken);

            return OperationResult<bool>.Success(true);
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure(
                $"Failed to unmark topic as used with bucket events {topicName}: {e.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<List<string>>> GetTopicsUsedOnBucketEventAsync(CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<List<string>>.Failure("Not initialized.");

        try
        {
            using var snsClient = new AmazonSimpleNotificationServiceClient(_credentials, Amazon.RegionEndpoint.GetBySystemName(_region));

            var listResponse = await GetAllTopics(snsClient, cancellationToken);

            var result = new List<string>();

            foreach (var topicSummary in listResponse)
            {
                try
                {
                    var tagsResponse = await snsClient.ListTagsForResourceAsync(new ListTagsForResourceRequest
                    {
                        ResourceArn = topicSummary.TopicArn
                    }, cancellationToken);

                    if (tagsResponse.Tags?.Any(tag => tag.Key == IPubSubService.UsedOnBucketEventFlagKey) !=
                        true) continue;

                    var topicName = topicSummary.TopicArn.Split(':').Last();
                    result.Add(EncodingUtilities.DecodeTopic(topicName)!);

                }
                catch (Exception)
                {
                    // Ignore individual topic tag retrieval failures
                }
            }

            return OperationResult<List<string>>.Success(result);
        }
        catch (Exception e)
        {
            return OperationResult<List<string>>.Failure($"Failed to get topics: {e.Message}");
        }
    }

    private static async Task<List<Topic>> GetAllTopics(AmazonSimpleNotificationServiceClient snsClient, CancellationToken cancellationToken)
    {
        var result = new List<Topic>();
        string? nextToken = null;
        do
        {
            var response = await snsClient.ListTopicsAsync(new ListTopicsRequest { NextToken = nextToken }, cancellationToken);
            if (response.Topics is { Count: > 0 })
            {
                result.AddRange(response.Topics);
            }
            nextToken = response.NextToken;
        } while (!string.IsNullOrEmpty(nextToken));

        return result;
    }

    public async ValueTask DisposeAsync()
    {
        // Cancel all subscription tokens
        var cancellationTasks = _subscriptions.Values.Select(async cSibList =>
        {
            foreach (var cSib in cSibList)
            {
                if (cSib.OnCancel != null)
                {
                    await cSib.OnCancel.Invoke();
                }
                await cSib.Cts.CancelAsync();
                cSib.Cts.Dispose();
            }
        });
        await Task.WhenAll(cancellationTasks);

        _subscriptions.Clear();

        // Dispose all publisher clients
        foreach (var publisher in _publishers.Values)
        {
            try { publisher.client.Dispose(); }
            catch (Exception)
            {
                // Ignore cleanup exceptions
            }
        }
        _publishers.Clear();

        using var snsClient = new AmazonSimpleNotificationServiceClient(_credentials, Amazon.RegionEndpoint.GetBySystemName(_region));

        foreach (var subscriptions in _topicSubscriptions.Values)
        {
            foreach (var subscriptionArn in subscriptions.Values)
            {
                await snsClient.UnsubscribeAsync(
                    subscriptionArn,
                    CancellationToken.None);
            }
        }

        _topicSubscriptions.Clear();
    }
}
