// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace Cloud.Interfaces;

/// <summary>
/// Modern async interface for cloud pub/sub services providing unified access across different providers.
/// Supports proper error handling and .NET 10 features.
/// </summary>
public interface IPubSubService
{
    /// <summary>
    /// Gets a value indicating whether the pub/sub service has been successfully initialized.
    /// </summary>
    bool IsInitialized { get; }

    /// <summary>
    /// Ensures that the given topic exists.
    /// </summary>
    /// <param name="topic">Topic to be ensured on.</param>
    /// <param name="errorMessageAction">Optional error callback.</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if ensuring succeeded; otherwise, false.</returns>
    Task<bool> EnsureTopicExistsAsync(
        string topic,
        Action<string>? errorMessageAction = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Subscribes to the given topic.
    /// </summary>
    /// <param name="topic">Topic to be subscribed to.</param>
    /// <param name="onMessage">Action invoked for each received message (topic, message).</param>
    /// <param name="errorMessageAction">Optional error callback.</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if subscription succeeded; otherwise, false.</returns>
    Task<bool> SubscribeAsync(
        string topic,
        Func<string, string, Task> onMessage,
        Action<string>? errorMessageAction = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes the given message to the given topic.
    /// </summary>
    /// <param name="topic">Topic to publish to.</param>
    /// <param name="message">Message to be sent.</param>
    /// <param name="errorMessageAction">Optional error callback.</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if publish succeeded; otherwise, false.</returns>
    Task<bool> PublishAsync(
        string topic,
        string message,
        Action<string>? errorMessageAction = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes all messages and the topic of the given workspace.
    /// </summary>
    /// <param name="topic">Topic to be deleted.</param>
    /// <param name="errorMessageAction">Optional error callback.</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if publish succeeded; otherwise, false.</returns>
    Task<bool> DeleteTopicAsync(
        string topic,
        Action<string>? errorMessageAction = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Used by file services to mark files as used on bucket events. No need to call it explicit.
    /// </summary>
    /// <param name="topicName">Name of the topic</param>
    void MarkUsedOnBucketEvent(string topicName);
}
