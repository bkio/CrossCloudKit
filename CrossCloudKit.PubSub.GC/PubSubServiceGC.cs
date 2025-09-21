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
using CrossCloudKit.Interfaces.Classes;
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
    private readonly GoogleCredential? _credential;
    private readonly ConcurrentDictionary<string, PublisherServiceApiClient> _publishers = new();
    private readonly ConcurrentDictionary<string, ConcurrentBag<CancellationSiblings>> _subscriptions = new();
    private readonly ConcurrentDictionary<string, ConcurrentBag<SubscriptionName>> _topicSubscriptions = new();

    private record CancellationSiblings(CancellationTokenSource Cts, Func<Task>? OnCancel);

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
                CredentialType.ServiceAccountFile => LoadFromFile(credentialData.NotNull()),
                CredentialType.ServiceAccountJson => LoadFromJson(credentialData.NotNull(), isBase64Encoded),
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

    private async Task<PublisherServiceApiClient> EnsureTopicExistsAndGetPublisherAsync(
        string encodedTopic,
        bool addToPublishers = true,
        CancellationToken cancellationToken = default)
    {
        if (_publishers.TryGetValue(encodedTopic, out var existing))
            return existing;

        var client = await new PublisherServiceApiClientBuilder
        {
            Credential = _credential.NotNull().CreateScoped(PublisherServiceApiClient.DefaultScopes)
        }.BuildAsync(cancellationToken);

        var encodedTopicName = new TopicName(_projectId, encodedTopic);
        await EnsureTopicExistsAsync(client, encodedTopicName, cancellationToken);

        return addToPublishers ? _publishers.GetOrAdd(encodedTopic, client) : client;
    }

    private static async Task EnsureTopicExistsAsync(PublisherServiceApiClient client, TopicName encodedTopicName, CancellationToken cancellationToken)
    {
        try
        {
            await client.CreateTopicAsync(encodedTopicName, cancellationToken);
        }
        catch (RpcException e) when (e.Status.StatusCode == StatusCode.AlreadyExists)
        {
            // Topic exists, that's fine
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> EnsureTopicExistsAsync(string topic, CancellationToken cancellationToken = default)
    {
        topic = EncodingUtilities.EncodeTopic(topic).NotNull();
        try
        {
            await EnsureTopicExistsAndGetPublisherAsync(topic, false, cancellationToken);
            return OperationResult<bool>.Success(true);
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure($"EnsureTopicExistsAsync failed: {e.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> PublishAsync(string topic, string message, CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
        {
            return OperationResult<bool>.Failure("Not initialized.", HttpStatusCode.ServiceUnavailable);
        }

        if (string.IsNullOrWhiteSpace(topic) || string.IsNullOrWhiteSpace(message))
        {
            return OperationResult<bool>.Failure("Parameters are invalid.", HttpStatusCode.BadRequest);
        }

        topic = EncodingUtilities.EncodeTopic(topic).NotNull();

        try
        {
            var publisher = await EnsureTopicExistsAndGetPublisherAsync(topic, true, cancellationToken);
            var topicName = new TopicName(_projectId, topic);
            var pubsubMessage = new PubsubMessage { Data = ByteString.CopyFromUtf8(message) };

            await publisher.PublishAsync(topicName, [pubsubMessage], cancellationToken: cancellationToken);
            return OperationResult<bool>.Success(true);
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure($"Publish failed: {e.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> SubscribeAsync(string topic, Func<string, string, Task>? onMessage, Action<Exception>? onError = null, CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
        {
            return OperationResult<bool>.Failure("Not initialized.", HttpStatusCode.ServiceUnavailable);
        }

        if (string.IsNullOrWhiteSpace(topic) || onMessage == null)
        {
            return OperationResult<bool>.Failure("Parameters are invalid.", HttpStatusCode.BadRequest);
        }

        topic = EncodingUtilities.EncodeTopic(topic).NotNull();

        try
        {
            var cts = new CancellationTokenSource();

            // Start subscription in the background
            var onCancel = await StartSubscriptionAsync(topic, onMessage, onError, cts.Token);

            var sib = new CancellationSiblings(cts, onCancel);

            _subscriptions.AddOrUpdate(topic,
                [sib],
                (_, existing) => { existing.Add(sib); return existing; });

            return OperationResult<bool>.Success(true);
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure($"Subscribe failed: {e.Message}", HttpStatusCode.InternalServerError);
        }
    }

    private static readonly HashSet<string> SupportedFileEventTypes =
        ["OBJECT_FINALIZE", "OBJECT_METADATA_UPDATE", "OBJECT_DELETE", "OBJECT_ARCHIVE"];

    private async Task<Func<Task>?> StartSubscriptionAsync(string encodedTopic, Func<string, string, Task> onMessage, Action<Exception>? onError, CancellationToken cancellationToken)
    {
        Func<Task>? cancelAction = null;
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
                Credential = _credential.NotNull().CreateScoped(SubscriberServiceApiClient.DefaultScopes)
            }.BuildAsync(cancellationToken);

            await subscriberApi.CreateSubscriptionAsync(subscriptionName, topicName, null, 600, cancellationToken);

            // Track subscription for cleanup
            _topicSubscriptions.AddOrUpdate(encodedTopic,
                [subscriptionName],
                (_, existing) => { existing.Add(subscriptionName); return existing; });

            // Create and track subscriber client
            subscriber = await new SubscriberClientBuilder
            {
                SubscriptionName = subscriptionName,
                Credential = _credential.NotNull().CreateScoped(SubscriberServiceApiClient.DefaultScopes)
            }.BuildAsync(cancellationToken);

            // Register cancellation to stop subscriber
            cancelAction = async () =>
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
            };

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
                        await onMessage(EncodingUtilities.DecodeTopic(encodedTopic).NotNull(), finalMsgObj.ToString(Formatting.None));
                    }
                    else
                    {
                        await onMessage(EncodingUtilities.DecodeTopic(encodedTopic).NotNull(), dataRaw);
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

        return cancelAction;
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> DeleteTopicAsync(string topic, CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
        {
            return OperationResult<bool>.Failure("Not initialized.", HttpStatusCode.ServiceUnavailable);
        }

        if (string.IsNullOrWhiteSpace(topic))
        {
            return OperationResult<bool>.Failure("Parameters are invalid.", HttpStatusCode.BadRequest);
        }

        topic = EncodingUtilities.EncodeTopic(topic).NotNull();

        try
        {
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

            // Delete all subscriptions for this topic from Google Cloud
            if (_topicSubscriptions.TryRemove(topic, out var subscriptions))
            {
                var subscriberApi = await new SubscriberServiceApiClientBuilder
                {
                    Credential = _credential.NotNull().CreateScoped(SubscriberServiceApiClient.DefaultScopes)
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
                    return OperationResult<bool>.Failure($"Delete subscription failed: {string.Join(Environment.NewLine, errorMsg)}", HttpStatusCode.InternalServerError);
                }
            }

            // Remove publisher from cache and delete the topic
            _publishers.TryRemove(topic, out _);

            var publisherClient = await new PublisherServiceApiClientBuilder
            {
                Credential = _credential.NotNull().CreateScoped(PublisherServiceApiClient.DefaultScopes)
            }.BuildAsync(cancellationToken);

            var topicName = new TopicName(_projectId, topic);
            await publisherClient.DeleteTopicAsync(topicName, cancellationToken);
        }
        catch (RpcException e) when (e.Status.StatusCode == StatusCode.NotFound)
        {
            // Topic doesn't exist, that's fine
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure($"Delete topic failed: {e.Message}", HttpStatusCode.InternalServerError);
        }

        return OperationResult<bool>.Success(true);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> MarkUsedOnBucketEvent(string topicName, CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
        {
            return OperationResult<bool>.Failure("Not initialized.", HttpStatusCode.ServiceUnavailable);
        }

        if (string.IsNullOrWhiteSpace(topicName))
        {
            return OperationResult<bool>.Failure("Parameters are invalid.", HttpStatusCode.BadRequest);
        }

        topicName = EncodingUtilities.EncodeTopic(topicName).NotNull();

        var publisherClient = await new PublisherServiceApiClientBuilder
        {
            Credential = _credential.NotNull().CreateScoped(PublisherServiceApiClient.DefaultScopes)
        }.BuildAsync(cancellationToken);

        var topicNameObj = new TopicName(_projectId, topicName);

        try
        {
            var existingTopic = await publisherClient.GetTopicAsync(topicNameObj, cancellationToken);
            if (existingTopic == null) return OperationResult<bool>.Failure("Topic does not exist.", HttpStatusCode.NotFound);

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
            return OperationResult<bool>.Failure($"Failed to mark topic as used with bucket events {topicName}: {e.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> UnmarkUsedOnBucketEvent(string topicName, CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
        {
            return OperationResult<bool>.Failure("Not initialized.", HttpStatusCode.ServiceUnavailable);
        }

        if (string.IsNullOrWhiteSpace(topicName))
        {
            return OperationResult<bool>.Failure("Parameters are invalid.", HttpStatusCode.BadRequest);
        }

        topicName = EncodingUtilities.EncodeTopic(topicName).NotNull();

        var publisherClient = await new PublisherServiceApiClientBuilder
        {
            Credential = _credential.NotNull().CreateScoped(PublisherServiceApiClient.DefaultScopes)
        }.BuildAsync(cancellationToken);

        var topicNameObj = new TopicName(_projectId, topicName);

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
            return OperationResult<bool>.Failure($"Failed to unmark topic as used with bucket events {topicName}: {e.Message}", HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<List<string>>> GetTopicsUsedOnBucketEventAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var publisherClient = await new PublisherServiceApiClientBuilder
            {
                Credential = _credential.NotNull().CreateScoped(PublisherServiceApiClient.DefaultScopes)
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
            return OperationResult<List<string>>.Failure($"Failed to get topics: {e.Message}", HttpStatusCode.InternalServerError);
        }
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

        _publishers.Clear();

        while (!_topicSubscriptions.IsEmpty)
        {
            var topic = _topicSubscriptions.First().Key;
            if (!_topicSubscriptions.TryRemove(topic, out var subscriptions)) continue;

            var subscriberApi = await new SubscriberServiceApiClientBuilder
            {
                Credential = _credential.NotNull().CreateScoped(SubscriberServiceApiClient.DefaultScopes)
            }.BuildAsync(CancellationToken.None);

            await Task.WhenAll(subscriptions.Select(async sub =>
            {
                try
                {
                    await subscriberApi.DeleteSubscriptionAsync(sub, cancellationToken: CancellationToken.None);
                }
                catch (Exception)
                {
                    // Ignore
                }
            }));
        }
        _topicSubscriptions.Clear();
    }

    /// <summary>
    /// Not relevant for Google Cloud Pub/Sub
    /// </summary>
    public Task<OperationResult<bool>> AWSSpecific_AddSnsS3PolicyAsync(string snsTopicArn, string bucketArn, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(OperationResult<bool>.Success(true));
    }

    /// <summary>
    /// Not relevant for Google Cloud Pub/Sub
    /// </summary>
    public Task<OperationResult<bool>> AWSSpecific_RemoveSnsS3PolicyAsync(string encodedTopic, string bucketArn,
        CancellationToken cancellationToken = default)
    {
        return Task.FromResult(OperationResult<bool>.Success(true));
    }
}
