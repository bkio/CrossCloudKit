// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using Cloud.Interfaces;
using Utilities.Common;
using Tag = Amazon.S3.Model.Tag;

namespace Cloud.File.AWS;

/// <summary>
/// AWS S3 file service implementation with async/await patterns
/// </summary>
public sealed class FileServiceAWS : IFileService, IAsyncDisposable
{
    private readonly AmazonS3Client? _s3Client;
    private readonly TransferUtility? _transferUtil;
    private readonly Amazon.Runtime.AWSCredentials? _awsCredentials;
    private readonly RegionEndpoint? _regionEndpoint;

    /// <summary>
    /// Gets a value indicating whether the service was initialized successfully
    /// </summary>
    public bool IsInitialized { get; }

    /// <summary>
    /// Initializes a new instance of the FileServiceAWS class using AWS credentials
    /// </summary>
    /// <param name="accessKey">AWS Access Key</param>
    /// <param name="secretKey">AWS Secret Key</param>
    /// <param name="region">AWS Region (e.g., eu-west-1)</param>
    public FileServiceAWS(string accessKey, string secretKey, string region)
    {
        try
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(accessKey);
            ArgumentException.ThrowIfNullOrWhiteSpace(secretKey);
            ArgumentException.ThrowIfNullOrWhiteSpace(region);

            _awsCredentials = new Amazon.Runtime.BasicAWSCredentials(accessKey, secretKey);
            _regionEndpoint = RegionEndpoint.GetBySystemName(region);

            _s3Client = new AmazonS3Client(_awsCredentials, _regionEndpoint);

            var transferUtilConfig = new TransferUtilityConfig
            {
                ConcurrentServiceRequests = 10,
            };
            _transferUtil = new TransferUtility(_s3Client, transferUtilConfig);

            IsInitialized = true;
        }
        catch
        {
            IsInitialized = false;
        }
    }

    /// <summary>
    /// Initializes a new instance of the FileServiceAWS class for S3-compatible storage (e.g., MinIO)
    /// </summary>
    /// <param name="serverAddress">Server address</param>
    /// <param name="accessKey">Access key</param>
    /// <param name="secretKey">Secret key</param>
    /// <param name="region">Region</param>
    public FileServiceAWS(string serverAddress, string accessKey, string secretKey, string region)
    {
        try
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(serverAddress);
            ArgumentException.ThrowIfNullOrWhiteSpace(accessKey);
            ArgumentException.ThrowIfNullOrWhiteSpace(secretKey);
            ArgumentException.ThrowIfNullOrWhiteSpace(region);

            _awsCredentials = new Amazon.Runtime.BasicAWSCredentials(accessKey, secretKey);
            _regionEndpoint = RegionEndpoint.GetBySystemName(region);

            var clientConfig = new AmazonS3Config
            {
                AuthenticationRegion = region,
                ServiceURL = serverAddress,
                ForcePathStyle = true
            };

            _s3Client = new AmazonS3Client(_awsCredentials, clientConfig);

            var transferUtilConfig = new TransferUtilityConfig
            {
                ConcurrentServiceRequests = 10,
            };
            _transferUtil = new TransferUtility(_s3Client, transferUtilConfig);

            IsInitialized = true;
        }
        catch
        {
            IsInitialized = false;
        }
    }

    /// <summary>
    /// Uploads content to the file service
    /// </summary>
    public async Task<FileServiceResult<FileMetadata>> UploadFileAsync(
        StringOrStream content,
        string bucketName,
        string keyInBucket,
        FileAccessibility accessibility = FileAccessibility.AuthenticatedRead,
        IReadOnlyDictionary<string, string>? tags = null,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || _transferUtil is null)
            return FileServiceResult<FileMetadata>.Failure("Service not initialized");

        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);
        ArgumentException.ThrowIfNullOrWhiteSpace(keyInBucket);

        try
        {
            var uploadRequest = new TransferUtilityUploadRequest
            {
                BucketName = bucketName,
                Key = keyInBucket,
                PartSize = 16,
                CannedACL = ConvertAccessibilityToAcl(accessibility)
            };

            await content.MatchAsync<object>(
                filePath =>
                {
                    if (!System.IO.File.Exists(filePath))
                        throw new FileNotFoundException($"File not found: {filePath}");

                    uploadRequest.FilePath = filePath;
                    return Task.FromResult(new object());
                },
                (stream, _) =>
                {
                    uploadRequest.InputStream = stream;
                    return Task.FromResult(new object());
                });

            if (tags?.Count > 0)
            {
                uploadRequest.TagSet = tags.Select(kvp => new Tag
                {
                    Key = kvp.Key,
                    Value = kvp.Value
                }).ToList();
            }

            await _transferUtil.UploadAsync(uploadRequest, cancellationToken).ConfigureAwait(false);

            // Get metadata after upload
            var metadataResult = await GetFileMetadataAsync(bucketName, keyInBucket, cancellationToken);
            if (metadataResult is { IsSuccessful: true, Data: not null })
            {
                return FileServiceResult<FileMetadata>.Success(metadataResult.Data);
            }

            // Fallback metadata
            var fallbackMetadata = new FileMetadata
            {
                Size = content.Length,
                Checksum = null,
                ContentType = null,
                CreatedAt = DateTimeOffset.UtcNow,
                LastModified = DateTimeOffset.UtcNow,
                Properties = new Dictionary<string, string>(),
                Tags = tags?.ToDictionary(kvp => kvp.Key, kvp => kvp.Value) ?? new Dictionary<string, string>()
            };

            return FileServiceResult<FileMetadata>.Success(fallbackMetadata);
        }
        catch (Exception ex)
        {
            return FileServiceResult<FileMetadata>.Failure($"Upload failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Downloads content from the file service
    /// </summary>
    public async Task<FileServiceResult<long>> DownloadFileAsync(
        string bucketName,
        string keyInBucket,
        StringOrStream destination,
        DownloadOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || _s3Client is null)
            return FileServiceResult<long>.Failure("Service not initialized");

        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);
        ArgumentException.ThrowIfNullOrWhiteSpace(keyInBucket);

        try
        {
            // Check if file exists
            var existsResult = await FileExistsAsync(bucketName, keyInBucket, cancellationToken);
            if (!existsResult.IsSuccessful || !existsResult.Data)
                return FileServiceResult<long>.Failure("File does not exist");

            return await destination.MatchAsync(
                async filePath =>
                {
                    if (_transferUtil is null)
                        return FileServiceResult<long>.Failure("Transfer utility not initialized");

                    await _transferUtil.DownloadAsync(filePath, bucketName, keyInBucket, cancellationToken).ConfigureAwait(false);

                    if (!System.IO.File.Exists(filePath))
                        return FileServiceResult<long>.Failure("Download completed but file doesn't exist locally");

                    var size = new FileInfo(filePath).Length;
                    return FileServiceResult<long>.Success(size);
                },
                async (stream, _) =>
                {
                    var getRequest = new GetObjectRequest
                    {
                        BucketName = bucketName,
                        Key = keyInBucket
                    };

                    if (options is not null && options.StartIndex > 0)
                    {
                        if (options.Size > 0)
                        {
                            var rangeEnd = options.StartIndex + options.Size - 1;
                            getRequest.ByteRange = new ByteRange(options.StartIndex, rangeEnd);
                        }
                        else
                        {
                            // For open-ended range, use single parameter constructor
                            getRequest.ByteRange = new ByteRange(options.StartIndex, long.MaxValue);
                        }
                    }

                    using var response = await _s3Client.GetObjectAsync(getRequest, cancellationToken).ConfigureAwait(false);
                    await response.ResponseStream.CopyToAsync(stream, cancellationToken).ConfigureAwait(false);

                    return FileServiceResult<long>.Success(response.ContentLength);
                });
        }
        catch (Exception ex)
        {
            return FileServiceResult<long>.Failure($"Download failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Copies a file within the file service
    /// </summary>
    public async Task<FileServiceResult<FileMetadata>> CopyFileAsync(
        string sourceBucketName,
        string sourceKeyInBucket,
        string destinationBucketName,
        string destinationKeyInBucket,
        FileAccessibility accessibility = FileAccessibility.AuthenticatedRead,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || _s3Client is null)
            return FileServiceResult<FileMetadata>.Failure("Service not initialized");

        ArgumentException.ThrowIfNullOrWhiteSpace(sourceBucketName);
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceKeyInBucket);
        ArgumentException.ThrowIfNullOrWhiteSpace(destinationBucketName);
        ArgumentException.ThrowIfNullOrWhiteSpace(destinationKeyInBucket);

        try
        {
            var copyRequest = new CopyObjectRequest
            {
                SourceBucket = sourceBucketName,
                SourceKey = sourceKeyInBucket,
                DestinationBucket = destinationBucketName,
                DestinationKey = destinationKeyInBucket,
                CannedACL = ConvertAccessibilityToAcl(accessibility)
            };

            await _s3Client.CopyObjectAsync(copyRequest, cancellationToken).ConfigureAwait(false);

            var metadataResult = await GetFileMetadataAsync(destinationBucketName, destinationKeyInBucket, cancellationToken);
            if (metadataResult is { IsSuccessful: true, Data: not null })
            {
                return FileServiceResult<FileMetadata>.Success(metadataResult.Data);
            }

            return FileServiceResult<FileMetadata>.Failure("Copy completed but metadata retrieval failed");
        }
        catch (Exception ex)
        {
            return FileServiceResult<FileMetadata>.Failure($"Copy failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Deletes a file from the file service
    /// </summary>
    public async Task<FileServiceResult<bool>> DeleteFileAsync(
        string bucketName,
        string keyInBucket,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || _s3Client is null)
            return FileServiceResult<bool>.Failure("Service not initialized");

        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);
        ArgumentException.ThrowIfNullOrWhiteSpace(keyInBucket);

        try
        {
            var deleteRequest = new DeleteObjectRequest
            {
                BucketName = bucketName,
                Key = keyInBucket
            };

            await _s3Client.DeleteObjectAsync(deleteRequest, cancellationToken).ConfigureAwait(false);
            return FileServiceResult<bool>.Success(true);
        }
        catch (Exception ex)
        {
            return FileServiceResult<bool>.Failure($"Delete failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Deletes all files with the specified prefix (folder)
    /// </summary>
    public async Task<FileServiceResult<int>> DeleteFolderAsync(
        string bucketName,
        string folderPrefix,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || _s3Client is null)
            return FileServiceResult<int>.Failure("Service not initialized");

        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);
        ArgumentException.ThrowIfNullOrWhiteSpace(folderPrefix);

        try
        {
            var objectsToDelete = new List<KeyVersion>();

            // List all objects with the prefix
            var listRequest = new ListObjectsV2Request
            {
                BucketName = bucketName,
                Prefix = folderPrefix
            };

            ListObjectsV2Response? response;
            do
            {
                response = await _s3Client.ListObjectsV2Async(listRequest, cancellationToken).ConfigureAwait(false);

                objectsToDelete.AddRange(response.S3Objects.Select(obj => new KeyVersion
                {
                    Key = obj.Key
                }));

                listRequest.ContinuationToken = response.NextContinuationToken;
            }
            while ((response.IsTruncated ?? false) && !cancellationToken.IsCancellationRequested);

            if (objectsToDelete.Count == 0)
                return FileServiceResult<int>.Success(0);

            // Delete objects in batches
            const int batchSize = 1000;
            var deletedCount = 0;

            for (var i = 0; i < objectsToDelete.Count; i += batchSize)
            {
                var batch = objectsToDelete.Skip(i).Take(batchSize).ToList();
                var deleteRequest = new DeleteObjectsRequest
                {
                    BucketName = bucketName,
                    Objects = batch
                };

                await _s3Client.DeleteObjectsAsync(deleteRequest, cancellationToken).ConfigureAwait(false);
                deletedCount += batch.Count;
            }

            return FileServiceResult<int>.Success(deletedCount);
        }
        catch (Exception ex)
        {
            return FileServiceResult<int>.Failure($"Delete folder failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Checks if a file exists in the file service
    /// </summary>
    public async Task<FileServiceResult<bool>> FileExistsAsync(
        string bucketName,
        string keyInBucket,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || _s3Client is null)
            return FileServiceResult<bool>.Failure("Service not initialized");

        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);
        ArgumentException.ThrowIfNullOrWhiteSpace(keyInBucket);

        try
        {
            var metadataRequest = new GetObjectMetadataRequest
            {
                BucketName = bucketName,
                Key = keyInBucket
            };

            await _s3Client.GetObjectMetadataAsync(metadataRequest, cancellationToken).ConfigureAwait(false);
            return FileServiceResult<bool>.Success(true);
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return FileServiceResult<bool>.Success(false);
        }
        catch (Exception ex)
        {
            return FileServiceResult<bool>.Failure($"File existence check failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Gets the size of a file in bytes
    /// </summary>
    public async Task<FileServiceResult<long>> GetFileSizeAsync(
        string bucketName,
        string keyInBucket,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || _s3Client is null)
            return FileServiceResult<long>.Failure("Service not initialized");

        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);
        ArgumentException.ThrowIfNullOrWhiteSpace(keyInBucket);

        try
        {
            var metadataRequest = new GetObjectMetadataRequest
            {
                BucketName = bucketName,
                Key = keyInBucket
            };

            var response = await _s3Client.GetObjectMetadataAsync(metadataRequest, cancellationToken).ConfigureAwait(false);
            return FileServiceResult<long>.Success(response.Headers.ContentLength);
        }
        catch (Exception ex)
        {
            return FileServiceResult<long>.Failure($"Get file size failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Gets the checksum of a file
    /// </summary>
    public async Task<FileServiceResult<string>> GetFileChecksumAsync(
        string bucketName,
        string keyInBucket,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || _s3Client is null)
            return FileServiceResult<string>.Failure("Service not initialized");

        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);
        ArgumentException.ThrowIfNullOrWhiteSpace(keyInBucket);

        try
        {
            var metadataRequest = new GetObjectMetadataRequest
            {
                BucketName = bucketName,
                Key = keyInBucket
            };

            var response = await _s3Client.GetObjectMetadataAsync(metadataRequest, cancellationToken).ConfigureAwait(false);
            var checksum = response.ETag?.Trim('"').ToLowerInvariant();

            return string.IsNullOrWhiteSpace(checksum)
                ? FileServiceResult<string>.Failure("No checksum available")
                : FileServiceResult<string>.Success(checksum);
        }
        catch (Exception ex)
        {
            return FileServiceResult<string>.Failure($"Get file checksum failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Gets the metadata of a file
    /// </summary>
    public async Task<FileServiceResult<FileMetadata>> GetFileMetadataAsync(
        string bucketName,
        string keyInBucket,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || _s3Client is null)
            return FileServiceResult<FileMetadata>.Failure("Service not initialized");

        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);
        ArgumentException.ThrowIfNullOrWhiteSpace(keyInBucket);

        try
        {
            var metadataRequest = new GetObjectMetadataRequest
            {
                BucketName = bucketName,
                Key = keyInBucket
            };

            var response = await _s3Client.GetObjectMetadataAsync(metadataRequest, cancellationToken).ConfigureAwait(false);

            // Get tags
            var tags = new Dictionary<string, string>();
            try
            {
                var tagsResult = await GetFileTagsAsync(bucketName, keyInBucket, cancellationToken);
                if (tagsResult.IsSuccessful)
                {
                    var data = tagsResult.Data;
                    if (data != null)
                    {
                        tags = data.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
                    }
                }
            }
            catch
            {
                // Tags are optional, continue without them
            }

            var properties = new Dictionary<string, string>();

            // Add user metadata
            if (response.Metadata?.Count > 0)
            {
                foreach (var key in response.Metadata.Keys)
                {
                    properties[key] = response.Metadata[key];
                }
            }

            // Add response headers - Amazon.S3.Model.GetObjectMetadataResponse.Headers doesn't support enumeration
            // Common headers are accessible via specific properties

            var metadata = new FileMetadata
            {
                Size = response.Headers.ContentLength,
                Checksum = response.ETag?.Trim('"').ToLowerInvariant(),
                ContentType = response.Headers.ContentType,
                CreatedAt = null, // S3 doesn't provide creation date
                LastModified = response.LastModified,
                Properties = properties,
                Tags = tags
            };

            return FileServiceResult<FileMetadata>.Success(metadata);
        }
        catch (Exception ex)
        {
            return FileServiceResult<FileMetadata>.Failure($"Get file metadata failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Gets the tags of a file
    /// </summary>
    public async Task<FileServiceResult<IReadOnlyDictionary<string, string>>> GetFileTagsAsync(
        string bucketName,
        string keyInBucket,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || _s3Client is null)
            return FileServiceResult<IReadOnlyDictionary<string, string>>.Failure("Service not initialized");

        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);
        ArgumentException.ThrowIfNullOrWhiteSpace(keyInBucket);

        try
        {
            var taggingRequest = new GetObjectTaggingRequest
            {
                BucketName = bucketName,
                Key = keyInBucket
            };

            var response = await _s3Client.GetObjectTaggingAsync(taggingRequest, cancellationToken).ConfigureAwait(false);
            var tags = response.Tagging?.ToDictionary(tag => tag.Key, tag => tag.Value) ?? new Dictionary<string, string>();

            return FileServiceResult<IReadOnlyDictionary<string, string>>.Success(tags);
        }
        catch (Exception ex)
        {
            return FileServiceResult<IReadOnlyDictionary<string, string>>.Failure($"Get file tags failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Sets the tags of a file
    /// </summary>
    public async Task<FileServiceResult<bool>> SetFileTagsAsync(
        string bucketName,
        string keyInBucket,
        IReadOnlyDictionary<string, string> tags,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || _s3Client is null)
            return FileServiceResult<bool>.Failure("Service not initialized");

        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);
        ArgumentException.ThrowIfNullOrWhiteSpace(keyInBucket);
        ArgumentNullException.ThrowIfNull(tags);

        try
        {
            var tagSet = tags.Select(kvp => new Tag
            {
                Key = kvp.Key,
                Value = kvp.Value
            }).ToList();

            var taggingRequest = new PutObjectTaggingRequest
            {
                BucketName = bucketName,
                Key = keyInBucket,
                Tagging = new Tagging { TagSet = tagSet }
            };

            await _s3Client.PutObjectTaggingAsync(taggingRequest, cancellationToken).ConfigureAwait(false);
            return FileServiceResult<bool>.Success(true);
        }
        catch (Exception ex)
        {
            return FileServiceResult<bool>.Failure($"Set file tags failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Sets the accessibility of a file
    /// </summary>
    public async Task<FileServiceResult<bool>> SetFileAccessibilityAsync(
        string bucketName,
        string keyInBucket,
        FileAccessibility accessibility,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || _s3Client is null)
            return FileServiceResult<bool>.Failure("Service not initialized");

        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);
        ArgumentException.ThrowIfNullOrWhiteSpace(keyInBucket);

        try
        {
            var aclRequest = new PutACLRequest
            {
                BucketName = bucketName,
                Key = keyInBucket,
                CannedACL = ConvertAccessibilityToAcl(accessibility)
            };

#pragma warning disable CS0618 // Type or member is obsolete
            await _s3Client.PutACLAsync(aclRequest, cancellationToken).ConfigureAwait(false);
#pragma warning restore CS0618 // Type or member is obsolete
            return FileServiceResult<bool>.Success(true);
        }
        catch (Exception ex)
        {
            return FileServiceResult<bool>.Failure($"Set file accessibility failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Creates a signed URL for uploading a file
    /// </summary>
    public async Task<FileServiceResult<SignedUrl>> CreateSignedUploadUrlAsync(
        string bucketName,
        string keyInBucket,
        SignedUploadUrlOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || _s3Client is null)
            return FileServiceResult<SignedUrl>.Failure("Service not initialized");

        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);
        ArgumentException.ThrowIfNullOrWhiteSpace(keyInBucket);

        try
        {
            var validFor = options?.ValidFor ?? TimeSpan.FromHours(1);
            var preSignedRequest = new GetPreSignedUrlRequest
            {
                BucketName = bucketName,
                Key = keyInBucket,
                ContentType = options?.ContentType,
                Expires = DateTime.UtcNow.Add(validFor),
                Verb = HttpVerb.PUT,
                Protocol = Protocol.HTTPS
            };

            var url = await _s3Client.GetPreSignedURLAsync(preSignedRequest);
            var signedUrl = new SignedUrl(url, DateTime.UtcNow.Add(validFor));

            return FileServiceResult<SignedUrl>.Success(signedUrl);
        }
        catch (Exception ex)
        {
            return FileServiceResult<SignedUrl>.Failure($"Create signed upload URL failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Creates a signed URL for downloading a file
    /// </summary>
    public async Task<FileServiceResult<SignedUrl>> CreateSignedDownloadUrlAsync(
        string bucketName,
        string keyInBucket,
        SignedDownloadUrlOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || _s3Client is null)
            return FileServiceResult<SignedUrl>.Failure("Service not initialized");

        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);
        ArgumentException.ThrowIfNullOrWhiteSpace(keyInBucket);

        try
        {
            var validFor = options?.ValidFor ?? TimeSpan.FromMinutes(1);
            var preSignedRequest = new GetPreSignedUrlRequest
            {
                BucketName = bucketName,
                Key = keyInBucket,
                Expires = DateTime.UtcNow.Add(validFor),
                Verb = HttpVerb.GET,
                Protocol = Protocol.HTTPS
            };

            var url = await _s3Client.GetPreSignedURLAsync(preSignedRequest);
            var signedUrl = new SignedUrl(url, DateTime.UtcNow.Add(validFor));

            return FileServiceResult<SignedUrl>.Success(signedUrl);
        }
        catch (Exception ex)
        {
            return FileServiceResult<SignedUrl>.Failure($"Create signed download URL failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Lists files in a bucket
    /// </summary>
    public async Task<FileServiceResult<ListFilesResult>> ListFilesAsync(
        string bucketName,
        ListFilesOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || _s3Client is null)
            return FileServiceResult<ListFilesResult>.Failure("Service not initialized");

        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);

        try
        {
            var listRequest = new ListObjectsV2Request
            {
                BucketName = bucketName,
                Prefix = options?.Prefix,
                MaxKeys = options?.MaxResults ?? 1000,
                ContinuationToken = options?.ContinuationToken
            };

            var response = await _s3Client.ListObjectsV2Async(listRequest, cancellationToken).ConfigureAwait(false);
            var fileKeys = response.S3Objects.Select(obj => obj.Key).ToList();

            var result = new ListFilesResult
            {
                FileKeys = fileKeys,
                NextContinuationToken = response.NextContinuationToken
            };
            // Note: HasMore property might be read-only, so we create the result without setting it
            // The AWS SDK response.IsTruncated indicates if there are more results

            return FileServiceResult<ListFilesResult>.Success(result);
        }
        catch (Exception ex)
        {
            return FileServiceResult<ListFilesResult>.Failure($"List files failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Creates a notification for file events
    /// </summary>
    public async Task<FileServiceResult<string>> CreateNotificationAsync(
        string bucketName,
        string topicName,
        string pathPrefix,
        IReadOnlyList<FileNotificationEventType> eventTypes,
        IPubSubService pubSubService,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || _s3Client is null)
            return FileServiceResult<string>.Failure("Service not initialized");

        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);
        ArgumentException.ThrowIfNullOrWhiteSpace(topicName);
        ArgumentException.ThrowIfNullOrWhiteSpace(pathPrefix);
        ArgumentNullException.ThrowIfNull(eventTypes);

        try
        {
            var awsEventTypes = new List<EventType>();

            foreach (var eventType in eventTypes)
            {
                switch (eventType)
                {
                    case FileNotificationEventType.Uploaded:
                        awsEventTypes.Add(EventType.ObjectCreatedAll);
                        break;
                    case FileNotificationEventType.Deleted:
                        awsEventTypes.Add(EventType.ObjectRemovedAll);
                        break;
                }
            }

            var queueArn = topicName;

            // Always fetch account ID using Security Token Service with stored credentials
            if (_awsCredentials is null)
            {
                return FileServiceResult<string>.Failure("AWS credentials not available");
            }

            using var stsClient = new AmazonSecurityTokenServiceClient(_awsCredentials, _regionEndpoint);
            var callerIdentityRequest = new GetCallerIdentityRequest();
            var callerIdentityResponse = await stsClient.GetCallerIdentityAsync(callerIdentityRequest, cancellationToken).ConfigureAwait(false);
            var accountId = callerIdentityResponse.Account;

            if (!topicName.StartsWith("arn:aws:sqs:", StringComparison.OrdinalIgnoreCase))
            {
                if (_regionEndpoint is null)
                {
                    return FileServiceResult<string>.Failure("AWS region not available for ARN construction");
                }

                // Get a bucket location to determine region
                var bucketLocationRequest = new GetBucketLocationRequest { BucketName = bucketName };
                var bucketLocationResponse = await _s3Client.GetBucketLocationAsync(bucketLocationRequest, cancellationToken).ConfigureAwait(false);
                var region = string.IsNullOrEmpty(bucketLocationResponse.Location?.Value) ? "us-east-1" : bucketLocationResponse.Location.Value;

                queueArn = $"arn:aws:sqs:{region}:{accountId}:{topicName}";
            }

            // Ensure queue exists (pass queue name, assuming pubSubService expects name not ARN)
            await pubSubService.EnsureTopicExistsAsync(topicName, Console.WriteLine, cancellationToken);

            var notificationRequest = new PutBucketNotificationRequest
            {
                BucketName = bucketName,
                QueueConfigurations =
                [
                    new QueueConfiguration
                    {
                        Queue = queueArn,
                        Filter = new Filter()
                        {
                            S3KeyFilter = new S3KeyFilter()
                            {
                                FilterRules = [new FilterRule("prefix", pathPrefix)]
                            }
                        },
                        Events = awsEventTypes
                    }
                ]
            };

            await _s3Client.PutBucketNotificationAsync(notificationRequest, cancellationToken);

            pubSubService.MarkUsedOnBucketEvent(topicName);

            return FileServiceResult<string>.Success(queueArn);
        }
        catch (Exception ex)
        {
            return FileServiceResult<string>.Failure($"Create notification failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Deletes notifications from a bucket
    /// </summary>
    public async Task<FileServiceResult<int>> DeleteNotificationsAsync(
        string bucketName,
        string? topicName = null,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || _s3Client is null)
            return FileServiceResult<int>.Failure("Service not initialized");

        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);

        try
        {
            // Get current notifications
            var getRequest = new GetBucketNotificationRequest
            {
                BucketName = bucketName
            };

            var response = await _s3Client.GetBucketNotificationAsync(getRequest, cancellationToken).ConfigureAwait(false);
            int deletedCount;

            if (topicName is null)
            {
                // Delete all queue notifications
                deletedCount = response.QueueConfigurations.Count;

                var putRequest = new PutBucketNotificationRequest
                {
                    BucketName = bucketName,
                    QueueConfigurations = new List<QueueConfiguration>()
                };

                await _s3Client.PutBucketNotificationAsync(putRequest, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                // Delete specific queue notifications - check both queue name and ARN
                var remainingConfigurations = response.QueueConfigurations
                    .Where(config => !IsQueueMatch(config.Queue, topicName))
                    .ToList();

                deletedCount = response.QueueConfigurations.Count - remainingConfigurations.Count;

                var putRequest = new PutBucketNotificationRequest
                {
                    BucketName = bucketName,
                    QueueConfigurations = remainingConfigurations
                };

                await _s3Client.PutBucketNotificationAsync(putRequest, cancellationToken).ConfigureAwait(false);
            }

            return FileServiceResult<int>.Success(deletedCount);
        }
        catch (Exception ex)
        {
            return FileServiceResult<int>.Failure($"Delete notifications failed: {ex.Message}");
        }
    }

    private static bool IsQueueMatch(string configuredQueue, string targetQueue)
    {
        // Direct match
        if (string.Equals(configuredQueue, targetQueue, StringComparison.OrdinalIgnoreCase))
            return true;

        // If configured queue is an ARN, extract queue name and compare
        if (configuredQueue.StartsWith("arn:aws:sqs:", StringComparison.OrdinalIgnoreCase))
        {
            var arnParts = configuredQueue.Split(':');
            if (arnParts.Length >= 6)
            {
                var queueName = arnParts[5];
                if (string.Equals(queueName, targetQueue, StringComparison.OrdinalIgnoreCase))
                    return true;
            }
        }

        // If target queue is an ARN, extract queue name and compare with configured queue
        if (targetQueue.StartsWith("arn:aws:sqs:", StringComparison.OrdinalIgnoreCase))
        {
            var arnParts = targetQueue.Split(':');
            if (arnParts.Length >= 6)
            {
                var queueName = arnParts[5];
                if (string.Equals(configuredQueue, queueName, StringComparison.OrdinalIgnoreCase))
                    return true;
            }
        }

        return false;
    }

    private static S3CannedACL ConvertAccessibilityToAcl(FileAccessibility accessibility) => accessibility switch
    {
        FileAccessibility.PublicRead => S3CannedACL.PublicRead,
        _ => S3CannedACL.AuthenticatedRead
    };

    /// <summary>
    /// Disposes of the resources used by this instance
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_transferUtil is not null)
        {
            _transferUtil.Dispose();
        }

        if (_s3Client is not null)
        {
            _s3Client.Dispose();
        }

        await ValueTask.CompletedTask;
    }
}
