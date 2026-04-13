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
/// <remarks>
/// <para>Providers: AWS S3, Google Cloud Storage, S3-compatible (MinIO, Wasabi, etc.), and Basic (local file system).</para>
/// <para>All methods return <see cref="OperationResult{T}"/>. Always check <c>IsSuccessful</c> before accessing <c>Data</c>.</para>
/// <para>Content parameters use <see cref="StringOrStream"/> — pass a file path (string) or a <c>Stream</c> directly.</para>
/// </remarks>
public interface IFileService
{
    /// <summary>
    /// Gets a value indicating whether the file service has been successfully initialized.
    /// </summary>
    /// <example>
    /// <code>
    /// if (!fileService.IsInitialized)
    ///     throw new InvalidOperationException("File service is not initialized.");
    /// </code>
    /// </example>
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
    /// <example>
    /// <code>
    /// // Upload from file path
    /// var result = await fileService.UploadFileAsync(
    ///     "/tmp/report.pdf", "my-bucket", "reports/2024/report.pdf");
    ///
    /// // Upload from stream
    /// using var stream = new MemoryStream(bytes);
    /// var result2 = await fileService.UploadFileAsync(
    ///     stream, "my-bucket", "data/file.bin",
    ///     accessibility: FileAccessibility.PublicRead,
    ///     tags: new Dictionary&lt;string, string&gt; { ["env"] = "prod" });
    /// </code>
    /// </example>
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
    /// <example>
    /// <code>
    /// // Download to file
    /// var result = await fileService.DownloadFileAsync(
    ///     "my-bucket", "reports/report.pdf", "/tmp/downloaded.pdf");
    ///
    /// // Download to stream
    /// using var ms = new MemoryStream();
    /// var result2 = await fileService.DownloadFileAsync(
    ///     "my-bucket", "reports/report.pdf", ms);
    /// </code>
    /// </example>
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
    /// <example>
    /// <code>
    /// var result = await fileService.CopyFileAsync(
    ///     "source-bucket", "original.pdf",
    ///     "dest-bucket", "backup/original.pdf");
    /// </code>
    /// </example>
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
    /// <example>
    /// <code>
    /// await fileService.DeleteFileAsync("my-bucket", "reports/old-report.pdf");
    /// </code>
    /// </example>
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
    /// <example>
    /// <code>
    /// var result = await fileService.DeleteFolderAsync("my-bucket", "temp/uploads/");
    /// if (result.IsSuccessful)
    ///     Console.WriteLine($"Deleted {result.Data} files.");
    /// </code>
    /// </example>
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
    /// <example>
    /// <code>
    /// var exists = await fileService.FileExistsAsync("my-bucket", "reports/report.pdf");
    /// if (exists.IsSuccessful &amp;&amp; exists.Data)
    ///     Console.WriteLine("File found.");
    /// </code>
    /// </example>
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
    /// <example>
    /// <code>
    /// var size = await fileService.GetFileSizeAsync("my-bucket", "data/file.bin");
    /// if (size.IsSuccessful)
    ///     Console.WriteLine($"Size: {size.Data} bytes");
    /// </code>
    /// </example>
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
    /// <example>
    /// <code>
    /// var checksum = await fileService.GetFileChecksumAsync("my-bucket", "data/file.bin");
    /// </code>
    /// </example>
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
    /// <example>
    /// <code>
    /// var meta = await fileService.GetFileMetadataAsync("my-bucket", "reports/report.pdf");
    /// if (meta.IsSuccessful)
    ///     Console.WriteLine($"Size: {meta.Data.Size}, Type: {meta.Data.ContentType}");
    /// </code>
    /// </example>
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
    /// <example>
    /// <code>
    /// var tags = await fileService.GetFileTagsAsync("my-bucket", "data/file.bin");
    /// if (tags.IsSuccessful)
    ///     foreach (var (k, v) in tags.Data)
    ///         Console.WriteLine($"{k} = {v}");
    /// </code>
    /// </example>
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
    /// <example>
    /// <code>
    /// var tags = new Dictionary&lt;string, string&gt; { ["env"] = "prod", ["team"] = "backend" };
    /// await fileService.SetFileTagsAsync("my-bucket", "data/file.bin", tags);
    /// </code>
    /// </example>
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
    /// <example>
    /// <code>
    /// await fileService.SetFileAccessibilityAsync(
    ///     "my-bucket", "public/logo.png", FileAccessibility.PublicRead);
    /// </code>
    /// </example>
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
    /// <example>
    /// <code>
    /// var url = await fileService.CreateSignedUploadUrlAsync(
    ///     "my-bucket", "uploads/doc.pdf");
    /// if (url.IsSuccessful)
    ///     Console.WriteLine($"Upload via: {url.Data.Url}");
    /// </code>
    /// </example>
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
    /// <example>
    /// <code>
    /// var url = await fileService.CreateSignedDownloadUrlAsync(
    ///     "my-bucket", "reports/report.pdf");
    /// if (url.IsSuccessful)
    ///     Console.WriteLine($"Download from: {url.Data.Url}");
    /// </code>
    /// </example>
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
    /// <example>
    /// <code>
    /// var list = await fileService.ListFilesAsync("my-bucket",
    ///     new FileListOptions { Prefix = "reports/", MaxResults = 100 });
    /// if (list.IsSuccessful)
    ///     foreach (var file in list.Data.Files)
    ///         Console.WriteLine(file.Key);
    /// </code>
    /// </example>
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
    /// <example>
    /// <code>
    /// var result = await fileService.CreateNotificationAsync(
    ///     "my-bucket", "file-events", "uploads/",
    ///     new[] { FileNotificationEventType.Created },
    ///     pubSubService);
    /// </code>
    /// </example>
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
    /// <example>
    /// <code>
    /// var deleted = await fileService.DeleteNotificationsAsync(
    ///     pubSubService, "my-bucket", topicName: "file-events");
    /// </code>
    /// </example>
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
    /// <example>
    /// <code>
    /// await fileService.CleanupBucketAsync("temp-bucket");
    /// </code>
    /// </example>
    Task<OperationResult<bool>> CleanupBucketAsync(string bucketName, CancellationToken cancellationToken = default);
}
