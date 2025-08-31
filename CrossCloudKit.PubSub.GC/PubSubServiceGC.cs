// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Grpc.Core;
using CrossCloudKit.Utilities.Common;
using System.Collections.Concurrent;
using System.Net;
using Google.Api.Gax.ResourceNames;
using Google.Protobuf.WellKnownTypes;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

// ReSharper disable MemberCanBePrivate.Global

namespace CrossCloudKit.PubSub.GC;

public enum CredentialType
{
    ServiceAccountFile,
    ServiceAccountJson,
    ApplicationDefault
}

/// <summary>
/// Simplified Google Cloud Pub/Sub implementation
/// </summary>
public sealed class PubSubServiceGC : IPubSubService, IAsyncDisposable
{
    private readonly string _projectId;
    private readonly GoogleCredential _credential = null!;
    private readonly ConcurrentDictionary<string, PublisherServiceApiClient> _publishers = new();
    private readonly ConcurrentDictionary<string, List<CancellationTokenSource>> _subscriptions = new();
    private readonly ConcurrentDictionary<string, List<SubscriptionName>> _topicSubscriptions = new();

    public bool IsInitialized { get; }

    /// <summary>
    /// Unified constructor for all credential types
    /// </summary>
    public PubSubServiceGC(
        string projectId,
        CredentialType credentialType,
        string? credentialData = null,
        bool isBase64Encoded = false,
        Action<string>? errorMessageAction = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(projectId);
        _projectId = projectId;

        try
        {
            _credential = credentialType switch
            {
                CredentialType.ServiceAccountFile => LoadFromFile(credentialData!),
                CredentialType.ServiceAccountJson => LoadFromJson(credentialData!, isBase64Encoded),
                CredentialType.ApplicationDefault => GoogleCredential.GetApplicationDefault(),
                _ => throw new ArgumentException("Invalid credential type", nameof(credentialType))
            };

            IsInitialized = true;
        }
        catch (Exception e)
        {
            errorMessageAction?.Invoke($"PubSubServiceGC initialization failed: {e.Message}");
            IsInitialized = false;
        }
    }

    /// <summary>
    /// Constructor using service account JSON content
    /// </summary>
    public PubSubServiceGC(
        string projectId,
        string serviceAccountJsonContent,
        bool isBase64Encoded = false,
        Action<string>? errorMessageAction = null)
        : this(projectId, CredentialType.ServiceAccountJson, serviceAccountJsonContent, isBase64Encoded, errorMessageAction)
    {
    }

    private static GoogleCredential LoadFromFile(string filePath)
    {
        using var stream = File.OpenRead(filePath);
        return GoogleCredential.FromStream(stream);
    }

    private static GoogleCredential LoadFromJson(string json, bool isBase64)
    {
        var content = isBase64 ? EncodingUtilities.Base64Decode(json) : json;
        return GoogleCredential.FromJson(content);
    }

    private static string EncodeTopic(string topic) => WebUtility.UrlEncode(topic);

    private async Task<PublisherServiceApiClient> GetPublisherAsync(string topic, CancellationToken cancellationToken = default, bool addToPublishers = true)
    {
        if (_publishers.TryGetValue(topic, out var existing))
            return existing;

        var client = await new PublisherServiceApiClientBuilder
        {
            Credential = _credential.CreateScoped(PublisherServiceApiClient.DefaultScopes)
        }.BuildAsync(cancellationToken);

        var topicName = new TopicName(_projectId, topic);
        await EnsureTopicExistsAsync(client, topicName, cancellationToken);

        return addToPublishers ? _publishers.GetOrAdd(topic, client) : client;
    }

    private static async Task EnsureTopicExistsAsync(PublisherServiceApiClient client, TopicName topicName, CancellationToken cancellationToken)
    {
        try
        {
            await client.CreateTopicAsync(topicName, cancellationToken);
        }
        catch (RpcException e) when (e.Status.StatusCode == StatusCode.AlreadyExists)
        {
            // Topic exists, that's fine
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> EnsureTopicExistsAsync(string topic, CancellationToken cancellationToken = default)
    {
        try
        {
            await GetPublisherAsync(topic, cancellationToken, false);
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

        try
        {
            var encodedTopic = EncodeTopic(topic);
            var publisher = await GetPublisherAsync(encodedTopic, cancellationToken);
            var topicName = new TopicName(_projectId, encodedTopic);
            var pubsubMessage = new PubsubMessage { Data = ByteString.CopyFromUtf8(message) };

            await publisher.PublishAsync(topicName, [pubsubMessage], cancellationToken: cancellationToken);
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

        try
        {
            var encodedTopic = EncodeTopic(topic);

            var cts = new CancellationTokenSource();
            _subscriptions.AddOrUpdate(topic,
                [cts],
                (_, existing) => { existing.Add(cts); return existing; });

            // Start subscription in the background
            await StartSubscriptionAsync(topic, encodedTopic, onMessage, onError, cts.Token);

            return OperationResult<bool>.Success(true);
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure($"Subscribe failed: {e.Message}");
        }
    }

    private static readonly HashSet<string> SupportedFileEventTypes =
        ["OBJECT_FINALIZE", "OBJECT_METADATA_UPDATE", "OBJECT_DELETE", "OBJECT_ARCHIVE"];

    private async Task StartSubscriptionAsync(string originalTopic, string encodedTopic, Func<string, string, Task> onMessage, Action<Exception>? onError, CancellationToken cancellationToken)
    {
        SubscriberClient? subscriber;
        try
        {
            var topicName = new TopicName(_projectId, encodedTopic);
            var subscriptionName = new SubscriptionName(_projectId, $"{encodedTopic}-{Guid.NewGuid():N}");

            // Ensure the topic exists first
            var ensureResult = await EnsureTopicExistsAsync(encodedTopic, cancellationToken);
            if (!ensureResult.IsSuccessful)
            {
                throw new Exception($"Ensure topic failed: {ensureResult.ErrorMessage}");
            }

            // Create subscription
            var subscriberApi = await new SubscriberServiceApiClientBuilder
            {
                Credential = _credential.CreateScoped(SubscriberServiceApiClient.DefaultScopes)
            }.BuildAsync(cancellationToken);

            await subscriberApi.CreateSubscriptionAsync(subscriptionName, topicName, null, 600, cancellationToken);

            // Track subscription for cleanup
            _topicSubscriptions.AddOrUpdate(originalTopic,
                [subscriptionName],
                (_, existing) => { existing.Add(subscriptionName); return existing; });

            // Create and track subscriber client
            subscriber = await new SubscriberClientBuilder
            {
                SubscriptionName = subscriptionName,
                Credential = _credential.CreateScoped(SubscriberServiceApiClient.DefaultScopes)
            }.BuildAsync(cancellationToken);

            // Register cancellation to stop subscriber
            await using var registration = cancellationToken.Register(async void () =>
            {
                try
                {
                    if (subscriber == null) return;
                    await subscriber.StopAsync(CancellationToken.None);
                }
                catch
                {
                    // Ignore stop exceptions
                }
            });

            // Start subscriber
            _ = subscriber.StartAsync(async (msg, _) =>
            {
                try
                {
                    var dataRaw = msg.Data.ToStringUtf8();

                    if (msg.Attributes != null && msg.Attributes.TryGetValue("eventType", out var eventType)
                        && SupportedFileEventTypes.Contains(eventType))
                    {
                        //File event

                        var finalMsgObj = new JObject();

                        var attributesObj = new JObject();
                        finalMsgObj["attributes"] = attributesObj;

                        if (msg.Attributes != null)
                        {
                            foreach (var attr in msg.Attributes)
                            {
                                attributesObj[attr.Key] = attr.Value;
                            }
                        }

                        try
                        {
                            var messageParsed = JObject.Parse(dataRaw);
                            finalMsgObj["data"] = messageParsed;
                        }
                        catch (JsonReaderException)
                        {
                            finalMsgObj["data"] = dataRaw;
                        }
                        await onMessage(originalTopic, finalMsgObj.ToString(Formatting.None));
                    }
                    else
                    {
                        await onMessage(originalTopic, dataRaw);
                    }
                    return SubscriberClient.Reply.Ack;
                }
                catch (Exception e)
                {
                    onError?.Invoke(e);
                    return SubscriberClient.Reply.Nack;
                }
            });
        }
        catch (OperationCanceledException)
        {
            // Expected on cancellation
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> DeleteTopicAsync(string topic, CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || string.IsNullOrWhiteSpace(topic))
            return OperationResult<bool>.Failure("Not initialized or parameters are invalid.");

        try
        {
            var encodedTopic = EncodeTopic(topic);

            // Cancel local subscription tracking
            if (_subscriptions.TryRemove(topic, out var ctsList))
            {
                foreach (var cts in ctsList)
                {
                    await cts.CancelAsync();
                    cts.Dispose();
                }
            }

            // Delete all subscriptions for this topic from Google Cloud
            if (_topicSubscriptions.TryRemove(topic, out var subscriptions))
            {
                var subscriberApi = await new SubscriberServiceApiClientBuilder
                {
                    Credential = _credential.CreateScoped(SubscriberServiceApiClient.DefaultScopes)
                }.BuildAsync(cancellationToken);

                var errorMsg = new ConcurrentBag<string>();
                await Task.WhenAll(subscriptions.Select(async sub =>
                {
                    try
                    {
                        await subscriberApi.DeleteSubscriptionAsync(sub, cancellationToken: cancellationToken);
                    }
                    catch (RpcException e) when (e.Status.StatusCode == StatusCode.NotFound)
                    {
                        // Subscription doesn't exist, that's fine
                    }
                    catch (Exception e)
                    {
                        lock (errorMsg)
                        {
                            errorMsg.Add(e.Message);
                        }
                    }
                }));
                if (!errorMsg.IsEmpty)
                {
                    return OperationResult<bool>.Failure($"Delete subscription failed: {string.Join(Environment.NewLine, errorMsg)}");
                }
            }

            // Remove publisher from cache and delete the topic
            _publishers.TryRemove(encodedTopic, out _);

            var publisherClient = await new PublisherServiceApiClientBuilder
            {
                Credential = _credential.CreateScoped(PublisherServiceApiClient.DefaultScopes)
            }.BuildAsync(cancellationToken);

            var topicName = new TopicName(_projectId, encodedTopic);
            await publisherClient.DeleteTopicAsync(topicName, cancellationToken);
        }
        catch (RpcException e) when (e.Status.StatusCode == StatusCode.NotFound)
        {
            // Topic doesn't exist, that's fine
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure($"Delete topic failed: {e.Message}");
        }

        return OperationResult<bool>.Success(true);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> MarkUsedOnBucketEvent(string topicName, CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || string.IsNullOrWhiteSpace(topicName))
            return OperationResult<bool>.Failure("Not initialized or parameters are invalid.");

        var publisherClient = await new PublisherServiceApiClientBuilder
        {
            Credential = _credential.CreateScoped(PublisherServiceApiClient.DefaultScopes)
        }.BuildAsync(cancellationToken);

        var encodedTopic = EncodeTopic(topicName);
        var topicNameObj = new TopicName(_projectId, encodedTopic);

        try
        {
            var existingTopic = await publisherClient.GetTopicAsync(topicNameObj, cancellationToken);
            if (existingTopic == null) return OperationResult<bool>.Failure("Topic does not exist.");

            var labels = new Dictionary<string, string>(existingTopic.Labels)
            {
                [IPubSubService.UsedOnBucketEventFlagKey] = "true"
            };

            var updatedTopic = new Topic
            {
                TopicName = topicNameObj,
                Labels = { labels }
            };

            await publisherClient.UpdateTopicAsync(updatedTopic, new FieldMask { Paths = { "labels" } }, cancellationToken);

            return OperationResult<bool>.Success(true);
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure($"Failed to mark topic as used with bucket events {topicName}: {e.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> UnmarkUsedOnBucketEvent(string topicName, CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || string.IsNullOrWhiteSpace(topicName))
            return OperationResult<bool>.Failure("Not initialized or parameters are invalid.");

        var publisherClient = await new PublisherServiceApiClientBuilder
        {
            Credential = _credential.CreateScoped(PublisherServiceApiClient.DefaultScopes)
        }.BuildAsync(cancellationToken);

        var encodedTopic = EncodeTopic(topicName);
        var topicNameObj = new TopicName(_projectId, encodedTopic);

        try
        {
            var existingTopic = await publisherClient.GetTopicAsync(topicNameObj, cancellationToken);

            var labels = new Dictionary<string, string>(existingTopic.Labels);
            if (labels.Remove(IPubSubService.UsedOnBucketEventFlagKey))
            {
                var updatedTopic = new Topic
                {
                    TopicName = topicNameObj,
                    Labels = { labels }
                };

                await publisherClient.UpdateTopicAsync(
                    updatedTopic,
                    new FieldMask { Paths = { "labels" } },
                    cancellationToken
                );
            }

            return OperationResult<bool>.Success(true);
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure($"Failed to unmark topic as used with bucket events {topicName}: {e.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<List<string>>> GetTopicsUsedOnBucketEventAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var publisherClient = await new PublisherServiceApiClientBuilder
            {
                Credential = _credential.CreateScoped(PublisherServiceApiClient.DefaultScopes)
            }.BuildAsync(cancellationToken);

            var topics = new List<string>();

            var projectName = ProjectName.FromProject(_projectId);
            var response = publisherClient.ListTopicsAsync(projectName);

            await foreach (var topic in response.WithCancellation(cancellationToken))
            {
                if (topic.Labels.TryGetValue(IPubSubService.UsedOnBucketEventFlagKey, out var flagValue) &&
                    string.Equals(flagValue, "true", StringComparison.OrdinalIgnoreCase))
                {
                    topics.Add(TopicName.Parse(topic.Name).TopicId);
                }
            }

            return OperationResult<List<string>>.Success(topics);
        }
        catch (Exception e)
        {
            return OperationResult<List<string>>.Failure($"Failed to get topics: {e.Message}");
        }
    }

    public async ValueTask DisposeAsync()
    {
        // Cancel all subscription tokens
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
        _publishers.Clear();
        _topicSubscriptions.Clear();
    }
}
