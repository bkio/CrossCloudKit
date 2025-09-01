// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Interfaces;

/// <summary>
/// Modern async interface for cloud pub/sub services providing unified access across different providers.
/// Supports proper error handling and .NET 10 features.
/// </summary>
public interface IPubSubService
{
    protected const string UsedOnBucketEventFlagKey = "used_on_bucket_event";

    /// <summary>
    /// Gets a value indicating whether the pub/sub service has been successfully initialized.
    /// </summary>
    bool IsInitialized { get; }

    /// <summary>
    /// Ensures that the given topic exists.
    /// </summary>
    /// <param name="topic">Topic to be ensured on.</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if ensuring succeeded; otherwise, false.</returns>
    Task<OperationResult<bool>> EnsureTopicExistsAsync(
        string topic,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Subscribes to the given topic.
    /// </summary>
    /// <param name="topic">Topic to be subscribed to.</param>
    /// <param name="onMessage">Action invoked for each received message (topic, message).</param>
    /// <param name="onError">Action invoked for each error during pooling.</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if subscription succeeded; otherwise, false.</returns>
    Task<OperationResult<bool>> SubscribeAsync(
        string topic,
        Func<string, string, Task>? onMessage,
        Action<Exception>? onError = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes the given message to the given topic.
    /// </summary>
    /// <param name="topic">Topic to publish to.</param>
    /// <param name="message">Message to be sent.</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if publish succeeded; otherwise, false.</returns>
    Task<OperationResult<bool>> PublishAsync(
        string topic,
        string message,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes all messages and the topic of the given workspace.
    /// </summary>
    /// <param name="topic">Topic to be deleted.</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if publish succeeded; otherwise, false.</returns>
    Task<OperationResult<bool>> DeleteTopicAsync(
        string topic,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Used by file services to mark files as used on bucket events. No need to call it explicit.
    /// <param name="topic">Topic to be marked.</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// </summary>
    /// <returns>True if succeeded; otherwise, false.</returns>
    Task<OperationResult<bool>> MarkUsedOnBucketEvent(string topic, CancellationToken cancellationToken = default);

    /// <summary>
    /// Used by file services to unmark files as used on bucket events. No need to call it explicit.
    /// <param name="topic">Topic to be unmarked.</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// </summary>
    /// <returns>True if succeeded; otherwise, false.</returns>
    Task<OperationResult<bool>> UnmarkUsedOnBucketEvent(string topic, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get all topics used on bucket events.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns></returns>
    Task<OperationResult<List<string>>> GetTopicsUsedOnBucketEventAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Only for AWS. For other providers, this method does nothing.
    /// Sets an AWS SNS topic policy to allow a specific S3 bucket to publish messages to the SNS topic.
    /// This is typically required when you want S3 event notifications (like object creation or deletion) to be sent to an SNS topic.
    /// </summary>
    /// <param name="snsTopicArn">The Amazon Resource Name (ARN) of the SNS topic to which the S3 bucket will be allowed to publish.</param>
    /// <param name="bucketArn">The ARN of the S3 bucket that should be authorized to publish messages to the SNS topic.</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns></returns>
    Task<OperationResult<bool>> AWSSpecific_AddSnsS3PolicyAsync(string snsTopicArn, string bucketArn, CancellationToken cancellationToken = default);

    /// <summary>
    /// Only for AWS. For other providers, this method does nothing.
    /// Removes an AWS SNS topic policy that allows a specific S3 bucket to publish messages to the SNS topic.
    /// This is typically used to stop S3 event notifications from being sent to an SNS topic.
    /// </summary>
    /// <param name="encodedTopic">The Amazon Resource Name (ARN) or encoded identifier of the SNS topic from which the bucket's publish permission should be removed.</param>
    /// <param name="bucketArn">The ARN of the S3 bucket whose publish permission should be removed from the SNS topic.</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Returns an OperationResult indicating whether the removal succeeded.</returns>
    Task<OperationResult<bool>> AWSSpecific_RemoveSnsS3PolicyAsync(string encodedTopic, string bucketArn, CancellationToken cancellationToken = default);
}
