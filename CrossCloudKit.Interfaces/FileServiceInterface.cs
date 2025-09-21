// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Interfaces.Enums;
using CrossCloudKit.Interfaces.Records;
using CrossCloudKit.Utilities.Common;
// ReSharper disable UnusedAutoPropertyAccessor.Global
// ReSharper disable ClassNeverInstantiated.Global

namespace CrossCloudKit.Interfaces;

/// <summary>
/// Modern interface for cloud file storage services providing unified access across different providers.
/// Supports async operations, proper error handling, and .NET 10 features.
/// </summary>
public interface IFileService
{
    /// <summary>
    /// Gets a value indicating whether the file service has been successfully initialized.
    /// </summary>
    bool IsInitialized { get; }

    /// <summary>
    /// Uploads content to the file service.
    /// </summary>
    /// <param name="content">The content to upload (file path or stream)</param>
    /// <param name="bucketName">The name of the bucket</param>
    /// <param name="keyInBucket">The key/path within the bucket</param>
    /// <param name="accessibility">The accessibility level for the uploaded file</param>
    /// <param name="tags">Optional tags to associate with the file</param>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>A task representing the upload operation</returns>
    Task<OperationResult<FileMetadata>> UploadFileAsync(
        StringOrStream content,
        string bucketName,
        string keyInBucket,
        FileAccessibility accessibility = FileAccessibility.AuthenticatedRead,
        IReadOnlyDictionary<string, string>? tags = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Downloads a file from the file service.
    /// </summary>
    /// <param name="bucketName">The name of the bucket</param>
    /// <param name="keyInBucket">The key/path within the bucket</param>
    /// <param name="destination">The destination (file path or stream)</param>
    /// <param name="options">Download options for range requests</param>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>A task representing the download operation</returns>
    Task<OperationResult<long>> DownloadFileAsync(
        string bucketName,
        string keyInBucket,
        StringOrStream destination,
        FileDownloadOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Copies a file from one location to another within the file service.
    /// </summary>
    /// <param name="sourceBucketName">The source bucket name</param>
    /// <param name="sourceKeyInBucket">The source key within the bucket</param>
    /// <param name="destinationBucketName">The destination bucket name</param>
    /// <param name="destinationKeyInBucket">The destination key within the bucket</param>
    /// <param name="accessibility">The accessibility level for the copied file</param>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>A task representing the copy operation</returns>
    Task<OperationResult<FileMetadata>> CopyFileAsync(
        string sourceBucketName,
        string sourceKeyInBucket,
        string destinationBucketName,
        string destinationKeyInBucket,
        FileAccessibility accessibility = FileAccessibility.AuthenticatedRead,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a file from the file service.
    /// </summary>
    /// <param name="bucketName">The name of the bucket</param>
    /// <param name="keyInBucket">The key/path within the bucket</param>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>A task representing the delete operation</returns>
    Task<OperationResult<bool>> DeleteFileAsync(
        string bucketName,
        string keyInBucket,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes all files in a folder (prefix).
    /// </summary>
    /// <param name="bucketName">The name of the bucket</param>
    /// <param name="folderPrefix">The folder prefix to delete</param>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>A task representing the delete operation with count of deleted files</returns>
    Task<OperationResult<int>> DeleteFolderAsync(
        string bucketName,
        string folderPrefix,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Checks if a file exists in the file service.
    /// </summary>
    /// <param name="bucketName">The name of the bucket</param>
    /// <param name="keyInBucket">The key/path within the bucket</param>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>A task representing the existence check result</returns>
    Task<OperationResult<bool>> FileExistsAsync(
        string bucketName,
        string keyInBucket,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the size of a file in bytes.
    /// </summary>
    /// <param name="bucketName">The name of the bucket</param>
    /// <param name="keyInBucket">The key/path within the bucket</param>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>A task representing the size retrieval operation</returns>
    Task<OperationResult<long>> GetFileSizeAsync(
        string bucketName,
        string keyInBucket,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the MD5 checksum of a file.
    /// </summary>
    /// <param name="bucketName">The name of the bucket</param>
    /// <param name="keyInBucket">The key/path within the bucket</param>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>A task representing the checksum retrieval operation</returns>
    Task<OperationResult<string>> GetFileChecksumAsync(
        string bucketName,
        string keyInBucket,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets comprehensive metadata for a file.
    /// </summary>
    /// <param name="bucketName">The name of the bucket</param>
    /// <param name="keyInBucket">The key/path within the bucket</param>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>A task representing the metadata retrieval operation</returns>
    Task<OperationResult<FileMetadata>> GetFileMetadataAsync(
        string bucketName,
        string keyInBucket,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the tags associated with a file.
    /// </summary>
    /// <param name="bucketName">The name of the bucket</param>
    /// <param name="keyInBucket">The key/path within the bucket</param>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>A task representing the tags retrieval operation</returns>
    Task<OperationResult<IReadOnlyDictionary<string, string>>> GetFileTagsAsync(
        string bucketName,
        string keyInBucket,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets the tags for a file, replacing any existing tags.
    /// </summary>
    /// <param name="bucketName">The name of the bucket</param>
    /// <param name="keyInBucket">The key/path within the bucket</param>
    /// <param name="tags">The tags to set</param>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>A task representing the tags update operation result</returns>
    Task<OperationResult<bool>> SetFileTagsAsync(
        string bucketName,
        string keyInBucket,
        IReadOnlyDictionary<string, string> tags,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Changes the accessibility level of a file.
    /// </summary>
    /// <param name="bucketName">The name of the bucket</param>
    /// <param name="keyInBucket">The key/path within the bucket</param>
    /// <param name="accessibility">The new accessibility level</param>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>A task representing the accessibility update operation result</returns>
    Task<OperationResult<bool>> SetFileAccessibilityAsync(
        string bucketName,
        string keyInBucket,
        FileAccessibility accessibility,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a signed URL for uploading a file.
    /// </summary>
    /// <param name="bucketName">The name of the bucket</param>
    /// <param name="keyInBucket">The key/path within the bucket</param>
    /// <param name="options">Options for the signed URL</param>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>A task representing the signed URL creation</returns>
    Task<OperationResult<FileSignedUrl>> CreateSignedUploadUrlAsync(
        string bucketName,
        string keyInBucket,
        FileSignedUploadUrlOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a signed URL for downloading a file.
    /// </summary>
    /// <param name="bucketName">The name of the bucket</param>
    /// <param name="keyInBucket">The key/path within the bucket</param>
    /// <param name="options">Options for the signed URL</param>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>A task representing the signed URL creation</returns>
    Task<OperationResult<FileSignedUrl>> CreateSignedDownloadUrlAsync(
        string bucketName,
        string keyInBucket,
        FileSignedDownloadUrlOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists all files in a bucket with optional filtering and pagination.
    /// </summary>
    /// <param name="bucketName">The name of the bucket</param>
    /// <param name="options">Options for listing files</param>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>A task representing the file listing operation</returns>
    Task<OperationResult<FileListResult>> ListFilesAsync(
        string bucketName,
        FileListOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a pub/sub notification for file events in a bucket.
    /// </summary>
    /// <param name="bucketName">The name of the bucket</param>
    /// <param name="topicName">The pub/sub topic name</param>
    /// <param name="pathPrefix">The path prefix to listen for events. This is mandatory, wildcards are not accepted.</param>
    /// <param name="eventTypes">The types of events to listen for</param>
    /// <param name="pubSubService">Pub/Sub service instance</param>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>A task representing the notification creation result</returns>
    Task<OperationResult<string>> CreateNotificationAsync(
        string bucketName,
        string topicName,
        string pathPrefix,
        IReadOnlyList<FileNotificationEventType> eventTypes,
        IPubSubService pubSubService,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes all pub/sub notifications for a bucket, optionally filtered by topic.
    /// </summary>
    /// <param name="pubSubService">Pub/Sub service instance</param>
    /// <param name="bucketName">The name of the bucket</param>
    /// <param name="topicName">Optional topic name to filter deletions</param>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>A task representing the notification deletion operation result</returns>
    Task<OperationResult<int>> DeleteNotificationsAsync(
        IPubSubService pubSubService,
        string bucketName,
        string? topicName = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Cleans up all files in the bucket, resets to the bucket's initial state.
    /// </summary>
    /// <param name="bucketName">The name of the bucket</param>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>A task representing the notification deletion operation</returns>
    Task<OperationResult<bool>> CleanupBucketAsync(string bucketName, CancellationToken cancellationToken = default);
}
