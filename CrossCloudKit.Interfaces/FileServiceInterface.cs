// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Utilities.Common;
// ReSharper disable UnusedAutoPropertyAccessor.Global
// ReSharper disable ClassNeverInstantiated.Global

namespace CrossCloudKit.Interfaces;

/// <summary>
/// Defines the accessibility level for uploaded or copied files in cloud storage.
/// </summary>
public enum FileAccessibility
{
    /// <summary>
    /// File can only be accessed by authenticated users with proper permissions.
    /// </summary>
    AuthenticatedRead,

    /// <summary>
    /// File can be accessed by any user within the same project/organization.
    /// </summary>
    ProjectWideProtectedRead,

    /// <summary>
    /// File can be accessed publicly by anyone with the URL.
    /// </summary>
    PublicRead
}

/// <summary>
/// Defines the types of pub/sub notification events for file operations.
/// </summary>
public enum FileNotificationEventType
{
    /// <summary>
    /// Event triggered when a file is uploaded/created.
    /// </summary>
    Uploaded,

    /// <summary>
    /// Event triggered when a file is deleted.
    /// </summary>
    Deleted
}

/// <summary>
/// Represents file metadata information.
/// </summary>
public sealed record FileMetadata
{
    /// <summary>
    /// Gets the size of the file in bytes.
    /// </summary>
    public long Size { get; init; }

    /// <summary>
    /// Gets the MD5 checksum of the file.
    /// </summary>
    public string? Checksum { get; init; }

    /// <summary>
    /// Gets the content type of the file.
    /// </summary>
    public string? ContentType { get; init; }

    /// <summary>
    /// Gets the creation timestamp of the file.
    /// </summary>
    public DateTimeOffset? CreatedAt { get; init; }

    /// <summary>
    /// Gets the last modified timestamp of the file.
    /// </summary>
    public DateTimeOffset? LastModified { get; init; }

    /// <summary>
    /// Gets additional metadata properties.
    /// </summary>
    public IReadOnlyDictionary<string, string> Properties { get; init; } = new Dictionary<string, string>();

    /// <summary>
    /// Gets the file tags/labels.
    /// </summary>
    public IReadOnlyDictionary<string, string> Tags { get; init; } = new Dictionary<string, string>();
}

/// <summary>
/// Represents a signed URL for file operations.
/// </summary>
/// <param name="Url">The signed URL</param>
/// <param name="ExpiresAt">When the URL expires</param>
// ReSharper disable once NotAccessedPositionalProperty.Global
public sealed record SignedUrl(string Url, DateTimeOffset ExpiresAt);

/// <summary>
/// Options for creating signed URLs for file uploads.
/// </summary>
public sealed record SignedUploadUrlOptions
{
    /// <summary>
    /// Gets or sets the content type of the file to be uploaded.
    /// </summary>
    public string? ContentType { get; init; }

    /// <summary>
    /// Gets or sets the validity duration for the signed URL.
    /// </summary>
    public TimeSpan ValidFor { get; init; } = TimeSpan.FromMinutes(60);

    /// <summary>
    /// Gets or sets whether to support resumable uploads.
    /// </summary>
    public bool SupportResumable { get; init; }
}

/// <summary>
/// Options for creating signed URLs for file downloads.
/// </summary>
public sealed record SignedDownloadUrlOptions
{
    /// <summary>
    /// Gets or sets the validity duration for the signed URL.
    /// </summary>
    public TimeSpan ValidFor { get; init; } = TimeSpan.FromMinutes(1);
}

/// <summary>
/// Options for downloading files with range support.
/// </summary>
public sealed record DownloadOptions
{
    /// <summary>
    /// Gets or sets the starting byte index for partial downloads.
    /// </summary>
    public long StartIndex { get; init; }

    /// <summary>
    /// Gets or sets the number of bytes to download (0 for entire file).
    /// </summary>
    public long Size { get; init; }
}

/// <summary>
/// Options for listing files in a bucket.
/// </summary>
public sealed record ListFilesOptions
{
    /// <summary>
    /// Gets or sets the prefix to filter file keys.
    /// </summary>
    public string? Prefix { get; init; }

    /// <summary>
    /// Gets or sets the maximum number of files to return.
    /// </summary>
    public int? MaxResults { get; init; }

    /// <summary>
    /// Gets or sets the continuation token for paginated results.
    /// </summary>
    public string? ContinuationToken { get; init; }
}

/// <summary>
/// Result of a paginated file listing operation.
/// </summary>
public sealed record ListFilesResult
{
    /// <summary>
    /// Gets the list of file keys.
    /// </summary>
    public IReadOnlyList<string> FileKeys { get; init; } = [];

    /// <summary>
    /// Gets the continuation token for the next page, if any.
    /// </summary>
    public string? NextContinuationToken { get; init; }
}

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
        DownloadOptions? options = null,
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
    Task<OperationResult<SignedUrl>> CreateSignedUploadUrlAsync(
        string bucketName,
        string keyInBucket,
        SignedUploadUrlOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a signed URL for downloading a file.
    /// </summary>
    /// <param name="bucketName">The name of the bucket</param>
    /// <param name="keyInBucket">The key/path within the bucket</param>
    /// <param name="options">Options for the signed URL</param>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>A task representing the signed URL creation</returns>
    Task<OperationResult<SignedUrl>> CreateSignedDownloadUrlAsync(
        string bucketName,
        string keyInBucket,
        SignedDownloadUrlOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists all files in a bucket with optional filtering and pagination.
    /// </summary>
    /// <param name="bucketName">The name of the bucket</param>
    /// <param name="options">Options for listing files</param>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>A task representing the file listing operation</returns>
    Task<OperationResult<ListFilesResult>> ListFilesAsync(
        string bucketName,
        ListFilesOptions? options = null,
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
