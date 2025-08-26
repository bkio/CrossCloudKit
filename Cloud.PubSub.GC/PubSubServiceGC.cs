// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Cloud.Interfaces;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Grpc.Core;
using Utilities.Common;
using System.Collections.Concurrent;
using System.Net;
using Google.Api.Gax.ResourceNames;
using Google.Protobuf.WellKnownTypes;

// ReSharper disable MemberCanBePrivate.Global

namespace Cloud.PubSub.GC;

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

    public async Task<bool> EnsureTopicExistsAsync(string topic, Action<string>? errorMessageAction, CancellationToken cancellationToken = default)
    {
        try
        {
            await GetPublisherAsync(topic, cancellationToken, false);
            return true;
        }
        catch (Exception e)
        {
            errorMessageAction?.Invoke($"EnsureTopicExistsAsync failed: {e.Message}");
            return false;
        }
    }

    public async Task<bool> PublishAsync(string topic, string message, Action<string>? errorMessageAction = null, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(topic) || string.IsNullOrWhiteSpace(message))
        {
            errorMessageAction?.Invoke("Topic and message cannot be empty");
            return false;
        }

        try
        {
            var encodedTopic = EncodeTopic(topic);
            var publisher = await GetPublisherAsync(encodedTopic, cancellationToken);
            var topicName = new TopicName(_projectId, encodedTopic);
            var pubsubMessage = new PubsubMessage { Data = ByteString.CopyFromUtf8(message) };

            await publisher.PublishAsync(topicName, [pubsubMessage], cancellationToken: cancellationToken);
            return true;
        }
        catch (Exception e)
        {
            errorMessageAction?.Invoke($"Publish failed: {e.Message}");
            return false;
        }
    }

    public async Task<bool> SubscribeAsync(string topic, Func<string, string, Task>? onMessage, Action<string>? errorMessageAction = null, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(topic) || onMessage == null)
            return false;

        try
        {
            var encodedTopic = EncodeTopic(topic);

            var cts = new CancellationTokenSource();
            _subscriptions.AddOrUpdate(topic,
                [cts],
                (_, existing) => { existing.Add(cts); return existing; });

            // Start subscription in the background
            await StartSubscriptionAsync(topic, encodedTopic, onMessage, errorMessageAction, cts.Token);

            return true;
        }
        catch (Exception e)
        {
            errorMessageAction?.Invoke($"Subscribe failed: {e.Message}");
            return false;
        }
    }

    private async Task StartSubscriptionAsync(string originalTopic, string encodedTopic, Func<string, string, Task> onMessage, Action<string>? errorMessageAction, CancellationToken cancellationToken)
    {
        SubscriberClient? subscriber;
        try
        {
            var topicName = new TopicName(_projectId, encodedTopic);
            var subscriptionName = new SubscriptionName(_projectId, $"{encodedTopic}-{Guid.NewGuid():N}");

            // Ensure the topic exists first
            await EnsureTopicExistsAsync(encodedTopic, errorMessageAction, cancellationToken);

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
                    await onMessage(originalTopic, msg.Data.ToStringUtf8());
                    return SubscriberClient.Reply.Ack;
                }
                catch (Exception e)
                {
                    errorMessageAction?.Invoke($"Message handler failed: {e.Message}");
                    return SubscriberClient.Reply.Nack;
                }
            });
        }
        catch (OperationCanceledException)
        {
            // Expected on cancellation
        }
        catch (Exception e)
        {
            errorMessageAction?.Invoke($"Subscription failed: {e.Message}");
        }
    }

    public async Task<bool> DeleteTopicAsync(string topic, Action<string>? errorMessageAction = null, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(topic))
            return false;

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
                        errorMessageAction?.Invoke($"Delete subscription failed: {e.Message}");
                    }
                }));
            }

            // Remove publisher from cache and delete topic
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
            errorMessageAction?.Invoke($"Delete topic failed: {e.Message}");
            return false;
        }

        return true;
    }

    public async Task<bool> MarkUsedOnBucketEvent(string topicName, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(topicName))
            return false;

        var publisherClient = await new PublisherServiceApiClientBuilder
        {
            Credential = _credential.CreateScoped(PublisherServiceApiClient.DefaultScopes)
        }.BuildAsync(cancellationToken);

        var encodedTopic = EncodeTopic(topicName);
        var topicNameObj = new TopicName(_projectId, encodedTopic);

        try
        {
            var existingTopic = await publisherClient.GetTopicAsync(topicNameObj, cancellationToken);
            if (existingTopic == null) return false;

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

            return true;
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
            return false;
        }
    }

    public async Task<bool> UnmarkUsedOnBucketEvent(string topicName, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(topicName))
            return false;

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

            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }

    public async Task<List<string>?> GetTopicsUsedOnBucketEventAsync(
        Action<string>? errorMessageAction = null,
        CancellationToken cancellationToken = default)
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

            return topics;
        }
        catch (Exception e)
        {
            errorMessageAction?.Invoke($"Failed to get topics: {e.Message}");
            return null;
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
