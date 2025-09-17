// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Utilities.Common;
using Tag = Amazon.S3.Model.Tag;

namespace CrossCloudKit.File.AWS;

/// <summary>
/// AWS S3 file service implementation with async/await patterns
/// </summary>
public class FileServiceAWS : IFileService, IAsyncDisposable
{
    protected AmazonS3Client? S3Client;
    protected TransferUtility? TransferUtil;
    protected Amazon.Runtime.AWSCredentials? AWSCredentials;
    protected RegionEndpoint? RegionEndpoint;
    protected readonly string? RegionSystemName;

    /// <summary>
    /// Gets a value indicating whether the service was initialized successfully
    /// </summary>
    public bool IsInitialized { get; protected init; }

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

            AWSCredentials = new Amazon.Runtime.BasicAWSCredentials(accessKey, secretKey);
            RegionSystemName = region;
            RegionEndpoint = RegionEndpoint.GetBySystemName(region);

            S3Client = new AmazonS3Client(AWSCredentials, RegionEndpoint);

            var transferUtilConfig = new TransferUtilityConfig
            {
                ConcurrentServiceRequests = 10,
            };
            TransferUtil = new TransferUtility(S3Client, transferUtilConfig);

            IsInitialized = true;
        }
        catch
        {
            IsInitialized = false;
        }
    }
    protected FileServiceAWS() { }

    /// <inheritdoc />
    public async Task<OperationResult<FileMetadata>> UploadFileAsync(
        StringOrStream content,
        string bucketName,
        string keyInBucket,
        FileAccessibility accessibility = FileAccessibility.AuthenticatedRead,
        IReadOnlyDictionary<string, string>? tags = null,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || TransferUtil is null)
            return OperationResult<FileMetadata>.Failure("Service not initialized");

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

            await TransferUtil.UploadAsync(uploadRequest, cancellationToken);

            // Get metadata after upload
            var metadataResult = await GetFileMetadataAsync(bucketName, keyInBucket, cancellationToken);
            if (metadataResult is { IsSuccessful: true, Data: not null })
            {
                return OperationResult<FileMetadata>.Success(metadataResult.Data);
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

            return OperationResult<FileMetadata>.Success(fallbackMetadata);
        }
        catch (Exception ex)
        {
            return OperationResult<FileMetadata>.Failure($"Upload failed: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<long>> DownloadFileAsync(
        string bucketName,
        string keyInBucket,
        StringOrStream destination,
        DownloadOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || S3Client is null)
            return OperationResult<long>.Failure("Service not initialized");

        try
        {
            // Check if file exists
            var existsResult = await FileExistsAsync(bucketName, keyInBucket, cancellationToken);
            if (!existsResult.IsSuccessful || !existsResult.Data)
                return OperationResult<long>.Failure("File does not exist");

            return await destination.MatchAsync(
                async filePath =>
                {
                    if (TransferUtil is null)
                        return OperationResult<long>.Failure("Transfer utility not initialized");

                    await TransferUtil.DownloadAsync(filePath, bucketName, keyInBucket, cancellationToken);

                    if (!System.IO.File.Exists(filePath))
                        return OperationResult<long>.Failure("Download completed but file doesn't exist locally");

                    var size = new FileInfo(filePath).Length;
                    return OperationResult<long>.Success(size);
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

                    using var response = await S3Client.GetObjectAsync(getRequest, cancellationToken);
                    await response.ResponseStream.CopyToAsync(stream, cancellationToken);

                    return OperationResult<long>.Success(response.ContentLength);
                });
        }
        catch (Exception ex)
        {
            return OperationResult<long>.Failure($"Download failed: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<FileMetadata>> CopyFileAsync(
        string sourceBucketName,
        string sourceKeyInBucket,
        string destinationBucketName,
        string destinationKeyInBucket,
        FileAccessibility accessibility = FileAccessibility.AuthenticatedRead,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || S3Client is null)
            return OperationResult<FileMetadata>.Failure("Service not initialized");

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

            await S3Client.CopyObjectAsync(copyRequest, cancellationToken);

            var metadataResult = await GetFileMetadataAsync(destinationBucketName, destinationKeyInBucket, cancellationToken);
            if (metadataResult is { IsSuccessful: true, Data: not null })
            {
                return OperationResult<FileMetadata>.Success(metadataResult.Data);
            }

            return OperationResult<FileMetadata>.Failure("Copy completed but metadata retrieval failed");
        }
        catch (Exception ex)
        {
            return OperationResult<FileMetadata>.Failure($"Copy failed: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> DeleteFileAsync(
        string bucketName,
        string keyInBucket,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || S3Client is null)
            return OperationResult<bool>.Failure("Service not initialized");

        try
        {
            var deleteRequest = new DeleteObjectRequest
            {
                BucketName = bucketName,
                Key = keyInBucket
            };

            await S3Client.DeleteObjectAsync(deleteRequest, cancellationToken);
            return OperationResult<bool>.Success(true);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure($"Delete failed: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<int>> DeleteFolderAsync(
        string bucketName,
        string folderPrefix,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || S3Client is null)
            return OperationResult<int>.Failure("Service not initialized");

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
                response = await S3Client.ListObjectsV2Async(listRequest, cancellationToken);

                objectsToDelete.AddRange(response.S3Objects.Select(obj => new KeyVersion
                {
                    Key = obj.Key
                }));

                listRequest.ContinuationToken = response.NextContinuationToken;
            }
            while ((response.IsTruncated ?? false) && !cancellationToken.IsCancellationRequested);

            if (objectsToDelete.Count == 0)
                return OperationResult<int>.Success(0);

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

                await S3Client.DeleteObjectsAsync(deleteRequest, cancellationToken);
                deletedCount += batch.Count;
            }

            return OperationResult<int>.Success(deletedCount);
        }
        catch (Exception ex)
        {
            return OperationResult<int>.Failure($"Delete folder failed: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> FileExistsAsync(
        string bucketName,
        string keyInBucket,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || S3Client is null)
            return OperationResult<bool>.Failure("Service not initialized");

        try
        {
            var metadataRequest = new GetObjectMetadataRequest
            {
                BucketName = bucketName,
                Key = keyInBucket
            };

            await S3Client.GetObjectMetadataAsync(metadataRequest, cancellationToken);
            return OperationResult<bool>.Success(true);
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return OperationResult<bool>.Success(false);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure($"File existence check failed: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<long>> GetFileSizeAsync(
        string bucketName,
        string keyInBucket,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || S3Client is null)
            return OperationResult<long>.Failure("Service not initialized");

        try
        {
            var metadataRequest = new GetObjectMetadataRequest
            {
                BucketName = bucketName,
                Key = keyInBucket
            };

            var response = await S3Client.GetObjectMetadataAsync(metadataRequest, cancellationToken);
            return OperationResult<long>.Success(response.Headers.ContentLength);
        }
        catch (Exception ex)
        {
            return OperationResult<long>.Failure($"Get file size failed: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<string>> GetFileChecksumAsync(
        string bucketName,
        string keyInBucket,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || S3Client is null)
            return OperationResult<string>.Failure("Service not initialized");

        try
        {
            var metadataRequest = new GetObjectMetadataRequest
            {
                BucketName = bucketName,
                Key = keyInBucket
            };

            var response = await S3Client.GetObjectMetadataAsync(metadataRequest, cancellationToken);
            var checksum = response.ETag?.Trim('"').ToLowerInvariant();

            return string.IsNullOrWhiteSpace(checksum)
                ? OperationResult<string>.Failure("No checksum available")
                : OperationResult<string>.Success(checksum);
        }
        catch (Exception ex)
        {
            return OperationResult<string>.Failure($"Get file checksum failed: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<FileMetadata>> GetFileMetadataAsync(
        string bucketName,
        string keyInBucket,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || S3Client is null)
            return OperationResult<FileMetadata>.Failure("Service not initialized");

        try
        {
            var metadataRequest = new GetObjectMetadataRequest
            {
                BucketName = bucketName,
                Key = keyInBucket
            };

            var response = await S3Client.GetObjectMetadataAsync(metadataRequest, cancellationToken);

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

            return OperationResult<FileMetadata>.Success(metadata);
        }
        catch (Exception ex)
        {
            return OperationResult<FileMetadata>.Failure($"Get file metadata failed: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<IReadOnlyDictionary<string, string>>> GetFileTagsAsync(
        string bucketName,
        string keyInBucket,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || S3Client is null)
            return OperationResult<IReadOnlyDictionary<string, string>>.Failure("Service not initialized");

        try
        {
            var taggingRequest = new GetObjectTaggingRequest
            {
                BucketName = bucketName,
                Key = keyInBucket
            };

            var response = await S3Client.GetObjectTaggingAsync(taggingRequest, cancellationToken);
            var tags = response.Tagging?.ToDictionary(tag => tag.Key, tag => tag.Value) ?? new Dictionary<string, string>();

            return OperationResult<IReadOnlyDictionary<string, string>>.Success(tags);
        }
        catch (Exception ex)
        {
            return OperationResult<IReadOnlyDictionary<string, string>>.Failure($"Get file tags failed: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> SetFileTagsAsync(
        string bucketName,
        string keyInBucket,
        IReadOnlyDictionary<string, string> tags,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || S3Client is null)
            return OperationResult<bool>.Failure("Service not initialized");

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

            await S3Client.PutObjectTaggingAsync(taggingRequest, cancellationToken);
            return OperationResult<bool>.Success(true);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure($"Set file tags failed: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> SetFileAccessibilityAsync(
        string bucketName,
        string keyInBucket,
        FileAccessibility accessibility,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || S3Client is null)
            return OperationResult<bool>.Failure("Service not initialized");

        try
        {
            var aclRequest = new PutACLRequest
            {
                BucketName = bucketName,
                Key = keyInBucket,
                CannedACL = ConvertAccessibilityToAcl(accessibility)
            };

#pragma warning disable CS0618 // Type or member is obsolete
            await S3Client.PutACLAsync(aclRequest, cancellationToken);
#pragma warning restore CS0618 // Type or member is obsolete
            return OperationResult<bool>.Success(true);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure($"Set file accessibility failed: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<SignedUrl>> CreateSignedUploadUrlAsync(
        string bucketName,
        string keyInBucket,
        SignedUploadUrlOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || S3Client is null)
            return OperationResult<SignedUrl>.Failure("Service not initialized");

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

            var url = await S3Client.GetPreSignedURLAsync(preSignedRequest);
            var signedUrl = new SignedUrl(url, DateTime.UtcNow.Add(validFor));

            return OperationResult<SignedUrl>.Success(signedUrl);
        }
        catch (Exception ex)
        {
            return OperationResult<SignedUrl>.Failure($"Create signed upload URL failed: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<SignedUrl>> CreateSignedDownloadUrlAsync(
        string bucketName,
        string keyInBucket,
        SignedDownloadUrlOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || S3Client is null)
            return OperationResult<SignedUrl>.Failure("Service not initialized");

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

            var url = await S3Client.GetPreSignedURLAsync(preSignedRequest);
            var signedUrl = new SignedUrl(url, DateTime.UtcNow.Add(validFor));

            return OperationResult<SignedUrl>.Success(signedUrl);
        }
        catch (Exception ex)
        {
            return OperationResult<SignedUrl>.Failure($"Create signed download URL failed: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<ListFilesResult>> ListFilesAsync(
        string bucketName,
        ListFilesOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || S3Client is null)
            return OperationResult<ListFilesResult>.Failure("Service not initialized");

        try
        {
            var listRequest = new ListObjectsV2Request
            {
                BucketName = bucketName,
                Prefix = options?.Prefix,
                MaxKeys = options?.MaxResults ?? 1000,
                ContinuationToken = options?.ContinuationToken
            };

            var response = await S3Client.ListObjectsV2Async(listRequest, cancellationToken);
            var fileKeys = response.S3Objects is null ? [] : response.S3Objects.Select(obj => obj.Key).ToList();

            var result = new ListFilesResult
            {
                FileKeys = fileKeys,
                NextContinuationToken = response.NextContinuationToken
            };
            // Note: HasMore property might be read-only, so we create the result without setting it
            // The AWS SDK response.IsTruncated indicates if there are more results

            return OperationResult<ListFilesResult>.Success(result);
        }
        catch (Exception ex)
        {
            return OperationResult<ListFilesResult>.Failure($"List files failed: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public virtual async Task<OperationResult<string>> CreateNotificationAsync(
        string bucketName,
        string topicName,
        string pathPrefix,
        IReadOnlyList<FileNotificationEventType> eventTypes,
        IPubSubService pubSubService,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || S3Client is null)
            return OperationResult<string>.Failure("Service not initialized");

        topicName = EncodingUtilities.EncodeTopic(topicName).NotNull();

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

            var topicToSnsArnResult = await TopicToSnsArn(topicName, cancellationToken);
            if (!topicToSnsArnResult.IsSuccessful)
            {
                return topicToSnsArnResult;
            }
            var topicArn = topicToSnsArnResult.Data.NotNull();

            // Ensure SNS topic exists
            await pubSubService.EnsureTopicExistsAsync(topicName, cancellationToken);

            var currentNotifications = await S3Client.GetBucketNotificationAsync(bucketName, cancellationToken);
            currentNotifications.TopicConfigurations ??= [];

            var added = false;

            foreach (var config in currentNotifications.TopicConfigurations)
            {
                if (config.Topic != topicArn
                    || config.Filter is not { S3KeyFilter.FilterRules: not null }
                    || !config.Filter.S3KeyFilter.FilterRules.Any(rule =>
                        rule.Name == "prefix" && rule.Value == pathPrefix)) continue;

                added = true;
                if (config.Events == null)
                {
                    config.Events = awsEventTypes;
                }
                else
                {
                    foreach (var eventType in awsEventTypes.Where(eventType => !config.Events.Contains(eventType)))
                    {
                        config.Events.Add(eventType);
                    }
                }
                break;
            }

            if (!added)
            {
                currentNotifications.TopicConfigurations.Add(new TopicConfiguration
                {
                    Topic = topicArn,
                    Filter = new Filter()
                    {
                        S3KeyFilter = new S3KeyFilter()
                        {
                            FilterRules = [new FilterRule("prefix", pathPrefix)]
                        }
                    },
                    Events = awsEventTypes
                });
            }

            var setPolicyResult = await pubSubService.AWSSpecific_AddSnsS3PolicyAsync(topicArn, $"arn:aws:s3:::{bucketName}",
                cancellationToken);
            if (!setPolicyResult.IsSuccessful)
            {
                return OperationResult<string>.Failure($"Add SNS policy failed: {setPolicyResult.ErrorMessage}");
            }

            var notificationRequest = new PutBucketNotificationRequest
            {
                BucketName = bucketName,
                TopicConfigurations = currentNotifications.TopicConfigurations,
                QueueConfigurations = currentNotifications.QueueConfigurations ?? [],
                EventBridgeConfiguration = currentNotifications.EventBridgeConfiguration ?? new EventBridgeConfiguration(),
                LambdaFunctionConfigurations = currentNotifications.LambdaFunctionConfigurations ?? []
            };

            await S3Client.PutBucketNotificationAsync(notificationRequest, cancellationToken);

            return !(await pubSubService.MarkUsedOnBucketEvent(topicName, cancellationToken)).IsSuccessful ? throw new Exception("Unable to mark topic as used on bucket event.") : OperationResult<string>.Success(topicArn);
        }
        catch (Exception ex)
        {
            return OperationResult<string>.Failure($"Create notification failed: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public virtual async Task<OperationResult<int>> DeleteNotificationsAsync(
        IPubSubService pubSubService,
        string bucketName,
        string? topicName = null,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || S3Client is null)
            return OperationResult<int>.Failure("Service not initialized");

        try
        {
            // Get current notifications
            var getRequest = new GetBucketNotificationRequest
            {
                BucketName = bucketName
            };

            var response = await S3Client.GetBucketNotificationAsync(getRequest, cancellationToken);
            int deletedCount;

            if (topicName == null)
            {
                // Delete all topic notifications
                deletedCount = response.TopicConfigurations.Count;

                var putRequest = new PutBucketNotificationRequest
                {
                    BucketName = bucketName,
                    TopicConfigurations = [],
                    QueueConfigurations = response.QueueConfigurations ?? [],
                    EventBridgeConfiguration = response.EventBridgeConfiguration ?? new EventBridgeConfiguration(),
                    LambdaFunctionConfigurations = response.LambdaFunctionConfigurations ?? []
                };

                await S3Client.PutBucketNotificationAsync(putRequest, cancellationToken);

                var topicsToDelete = await pubSubService.GetTopicsUsedOnBucketEventAsync(cancellationToken: cancellationToken);
                if (!topicsToDelete.IsSuccessful || topicsToDelete.Data == null)
                {
                    return OperationResult<int>.Failure("GetTopicsUsedOnBucketEventAsync has failed.");
                }

                foreach (var topic in topicsToDelete.Data)
                {
                    var removePolicyResult = await pubSubService.AWSSpecific_RemoveSnsS3PolicyAsync(EncodingUtilities.EncodeTopic(topic).NotNull(), $"arn:aws:s3:::{bucketName}",
                        cancellationToken);
                    if (!removePolicyResult.IsSuccessful)
                    {
                        return OperationResult<int>.Failure($"Remove SNS policy failed: {removePolicyResult.ErrorMessage}");
                    }
                    if (!(await pubSubService.UnmarkUsedOnBucketEvent(topic, cancellationToken)).IsSuccessful)
                    {
                        return OperationResult<int>.Failure("Unable to unmark topic as used on bucket event.");
                    }
                }
            }
            else
            {
                var encodedTopic = EncodingUtilities.EncodeTopic(topicName).NotNull();

                // Delete specific topic notifications - check both topic name and ARN
                var remainingConfigurations = response.TopicConfigurations
                    .Where(config => !IsTopicMatch(config.Topic, encodedTopic))
                    .ToList();

                deletedCount = response.TopicConfigurations.Count - remainingConfigurations.Count;

                var putRequest = new PutBucketNotificationRequest
                {
                    BucketName = bucketName,
                    TopicConfigurations = remainingConfigurations,
                    QueueConfigurations = response.QueueConfigurations ?? [],
                    EventBridgeConfiguration = response.EventBridgeConfiguration ?? new EventBridgeConfiguration(),
                    LambdaFunctionConfigurations = response.LambdaFunctionConfigurations ?? []
                };

                await S3Client.PutBucketNotificationAsync(putRequest, cancellationToken);

                var removePolicyResult = await pubSubService.AWSSpecific_RemoveSnsS3PolicyAsync(encodedTopic, $"arn:aws:s3:::{bucketName}",
                    cancellationToken);
                if (!removePolicyResult.IsSuccessful)
                {
                    return OperationResult<int>.Failure($"Remove SNS policy failed: {removePolicyResult.ErrorMessage}");
                }

                if (!(await pubSubService.UnmarkUsedOnBucketEvent(topicName, cancellationToken)).IsSuccessful)
                {
                    return OperationResult<int>.Failure("Unable to unmark topic as used on bucket event.");
                }
            }

            return OperationResult<int>.Success(deletedCount);
        }
        catch (Exception ex)
        {
            return OperationResult<int>.Failure($"Delete notifications failed: {ex.Message}");
        }
    }

    public virtual async Task<OperationResult<bool>> CleanupBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        if (!IsInitialized) return OperationResult<bool>.Failure("Service not initialized.");
        var success = true;
        var errorMessages = new List<string>();
        try
        {
            var listResult = await ListFilesAsync(bucketName, cancellationToken: cancellationToken);
            if (listResult is { IsSuccessful: true, Data: not null })
            {
                foreach (var key in listResult.Data.FileKeys)
                {
                    var deleteFileResult = await DeleteFileAsync(bucketName, key, cancellationToken);
                    if (!deleteFileResult.IsSuccessful)
                    {
                        success = false;
                        errorMessages.Add(deleteFileResult.ErrorMessage.NotNull());
                    }
                }
            }
        }
        catch { /* Ignore cleanup errors in tests */ }
        return success ? OperationResult<bool>.Success(true) : OperationResult<bool>.Failure($"Cleanup bucket failed: {string.Join(Environment.NewLine, errorMessages)}");
    }

    private static S3CannedACL ConvertAccessibilityToAcl(FileAccessibility accessibility) => accessibility switch
    {
        FileAccessibility.PublicRead => S3CannedACL.PublicRead,
        _ => S3CannedACL.AuthenticatedRead
    };

    private async Task<OperationResult<string>> TopicToSnsArn(string encodedTopic, CancellationToken cancellationToken)
    {
        if (S3Client == null)
        {
            return OperationResult<string>.Failure("Service not initialized");
        }

        using var stsClient = new AmazonSecurityTokenServiceClient(AWSCredentials, RegionEndpoint);
        var callerIdentityRequest = new GetCallerIdentityRequest();
        var callerIdentityResponse = await stsClient.GetCallerIdentityAsync(callerIdentityRequest, cancellationToken);
        var accountId = callerIdentityResponse.Account;

        if (encodedTopic.StartsWith("arn:aws:sns:", StringComparison.OrdinalIgnoreCase))
            return OperationResult<string>.Success(encodedTopic);
        return RegionSystemName is null ? OperationResult<string>.Failure("AWS region not available for ARN construction") : OperationResult<string>.Success($"arn:aws:sns:{RegionSystemName}:{accountId}:{encodedTopic}");
    }
    private static bool IsTopicMatch(string configuredTopic, string targetTopic)
    {
        // Direct match
        if (string.Equals(configuredTopic, targetTopic, StringComparison.OrdinalIgnoreCase))
            return true;
        return InternalCompareTopicMatch(configuredTopic, targetTopic) ||
               InternalCompareTopicMatch(targetTopic, configuredTopic);
    }
    private static bool InternalCompareTopicMatch(string first, string second)
    {
        if (!first.StartsWith("arn:aws:sns:", StringComparison.OrdinalIgnoreCase)) return false;
        var arnParts = first.Split(':');
        if (arnParts.Length < 6) return false;
        var topicName = arnParts[5];
        return string.Equals(topicName, second, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Disposes of the resources used by this instance
    /// </summary>
    public virtual async ValueTask DisposeAsync()
    {
        TransferUtil?.Dispose();

        S3Client?.Dispose();

        await ValueTask.CompletedTask;

        GC.SuppressFinalize(this);
    }
}
