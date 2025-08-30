// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Cloud.Interfaces;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Storage.V1;
using Utilities.Common;
using System.Net.Http.Headers;
using Google;

namespace Cloud.File.GC;

/// <summary>
/// Google Cloud Storage implementation of the file service interface.
/// Provides modern async/await support with comprehensive error handling and .NET 10 features.
/// </summary>
public sealed class FileServiceGC : IFileService, IAsyncDisposable
{
    /// <summary>Google Storage Client that is responsible to serve to this object</summary>
    private readonly StorageClient? _gsClient;

    /// <summary>Holds initialization success</summary>
    private readonly bool _initializationSucceed;

    private readonly ServiceAccountCredential? _credentialScoped;
    private readonly string _projectId;

    /// <summary>
    /// Gets a value indicating whether the file service has been successfully initialized.
    /// </summary>
    // ReSharper disable once ConvertToAutoProperty
    public bool IsInitialized => _initializationSucceed;

    /// <summary>
    /// FileServiceGC: Constructor for service account JSON file path
    /// </summary>
    /// <param name="projectId">Google Cloud Project ID</param>
    /// <param name="serviceAccountKeyFilePath">Path to the service account JSON key file</param>
    /// <param name="errorMessageAction">Error messages will be pushed to this action</param>
    public FileServiceGC(
        string projectId,
        string serviceAccountKeyFilePath,
        Action<string>? errorMessageAction = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(projectId);
        ArgumentException.ThrowIfNullOrWhiteSpace(serviceAccountKeyFilePath);

        _projectId = projectId;

        try
        {
            using var stream = new FileStream(serviceAccountKeyFilePath, FileMode.Open, FileAccess.Read);
            var credential = GoogleCredential.FromStream(stream);
            _credentialScoped = credential.CreateScoped(
                    [Google.Apis.Storage.v1.StorageService.Scope.DevstorageReadWrite])
                    .UnderlyingCredential as ServiceAccountCredential;

            _gsClient = StorageClient.Create(credential);
            _initializationSucceed = _gsClient != null;
        }
        catch (Exception e)
        {
            errorMessageAction?.Invoke($"FileServiceGC->Constructor: {e.Message}, Trace: {e.StackTrace}");
            _initializationSucceed = false;
        }
    }

    /// <summary>
    /// FileServiceGC: Constructor for service account JSON content
    /// </summary>
    /// <param name="projectId">Google Cloud Project ID</param>
    /// <param name="serviceAccountJsonContent">JSON content of the service account key</param>
    /// <param name="isBase64Encoded">Whether the JSON content is base64 encoded</param>
    /// <param name="errorMessageAction">Error messages will be pushed to this action</param>
    public FileServiceGC(
        string projectId,
        string serviceAccountJsonContent,
        bool isBase64Encoded,
        Action<string>? errorMessageAction = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(projectId);
        ArgumentException.ThrowIfNullOrWhiteSpace(serviceAccountJsonContent);

        _projectId = projectId;

        try
        {
            var jsonContent = serviceAccountJsonContent;

            if (isBase64Encoded)
            {
                try
                {
                    jsonContent = EncodingUtilities.Base64Decode(serviceAccountJsonContent);
                }
                catch (Exception e)
                {
                    errorMessageAction?.Invoke($"Base64 decode operation for service account JSON has failed: {e.Message}");
                    _initializationSucceed = false;
                    return;
                }
            }

            var credential = GoogleCredential.FromJson(jsonContent);
            _credentialScoped = credential.CreateScoped(
                    [Google.Apis.Storage.v1.StorageService.Scope.DevstorageReadWrite])
                .UnderlyingCredential as ServiceAccountCredential;

            _gsClient = StorageClient.Create(credential);
            _initializationSucceed = _gsClient != null;
        }
        catch (Exception e)
        {
            errorMessageAction?.Invoke($"FileServiceGC->Constructor: {e.Message}, Trace: {e.StackTrace}");
            _initializationSucceed = false;
        }
    }

    /// <summary>
    /// FileServiceGC: Constructor for default credentials (uses Application Default Credentials)
    /// </summary>
    /// <param name="projectId">Google Cloud Project ID</param>
    /// <param name="useDefaultCredentials">Must be true to use this constructor</param>
    /// <param name="errorMessageAction">Error messages will be pushed to this action</param>
    public FileServiceGC(
        string projectId,
        bool useDefaultCredentials,
        Action<string>? errorMessageAction = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(projectId);

        if (!useDefaultCredentials)
        {
            throw new ArgumentException("This constructor is for default credentials only. Set useDefaultCredentials to true or use a different constructor.", nameof(useDefaultCredentials));
        }

        _projectId = projectId;

        try
        {
            // Use Application Default Credentials (ADC)
            _gsClient = StorageClient.Create();
            _initializationSucceed = _gsClient != null;
        }
        catch (Exception e)
        {
            errorMessageAction?.Invoke($"FileServiceGC->Constructor: {e.Message}, Trace: {e.StackTrace}");
            _initializationSucceed = false;
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<FileMetadata>> UploadFileAsync(
        StringOrStream content,
        string bucketName,
        string keyInBucket,
        FileAccessibility accessibility = FileAccessibility.AuthenticatedRead,
        IReadOnlyDictionary<string, string>? tags = null,
        CancellationToken cancellationToken = default)
    {
        if (_gsClient == null)
        {
            return OperationResult<FileMetadata>.Failure("Google Storage client is not initialized");
        }

        try
        {
            var acl = ConvertAccessibilityToAcl(accessibility);
            Google.Apis.Storage.v1.Data.Object? uploadedObject = null;

            await content.MatchAsync<object>(
                onString: async filePath =>
                {
                    if (!System.IO.File.Exists(filePath))
                    {
                        throw new FileNotFoundException($"File not found: {filePath}");
                    }

                    await using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
                    uploadedObject = await _gsClient.UploadObjectAsync(bucketName, keyInBucket, null, fileStream, new UploadObjectOptions
                    {
                        PredefinedAcl = acl
                    }, cancellationToken);
                    return Task.CompletedTask;
                },
                onStream: async (stream, _) =>
                {
                    uploadedObject = await _gsClient.UploadObjectAsync(bucketName, keyInBucket, null, stream, new UploadObjectOptions
                    {
                        PredefinedAcl = acl
                    }, cancellationToken);
                    return Task.CompletedTask;
                });

            if (uploadedObject == null)
            {
                return OperationResult<FileMetadata>.Failure("Upload operation failed");
            }

            // Set cache control and metadata
            uploadedObject.CacheControl = acl == PredefinedObjectAcl.PublicRead ? "public" : "private";

            if (tags?.Count > 0)
            {
                uploadedObject.Metadata = new Dictionary<string, string>(tags);
            }

            // Set content disposition with filename
            var fileName = Path.GetFileName(keyInBucket);
            if (!string.IsNullOrEmpty(fileName))
            {
                uploadedObject.ContentDisposition = $"inline; filename={fileName}";
            }

            await _gsClient.PatchObjectAsync(uploadedObject, new PatchObjectOptions(), cancellationToken);

            var metadata = CreateFileMetadata(uploadedObject);
            return OperationResult<FileMetadata>.Success(metadata);
        }
        catch (Exception e)
        {
            return OperationResult<FileMetadata>.Failure($"Upload failed: {e.Message}");
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
        if (_gsClient == null)
        {
            return OperationResult<long>.Failure("Google Storage client is not initialized");
        }

        try
        {
            // Check if file exists first
            var existsResult = await FileExistsAsync(bucketName, keyInBucket, cancellationToken);
            if (!existsResult.IsSuccessful)
            {
                return OperationResult<long>.Failure(existsResult.ErrorMessage ?? "Failed to check file existence");
            }

            if (!existsResult.Data)
            {
                return OperationResult<long>.Failure("File does not exist in the storage service");
            }

            DownloadObjectOptions? downloadOptions = null;
            if (options is { Size: > 0 })
            {
                downloadOptions = new DownloadObjectOptions()
                {
                    Range = new RangeHeaderValue(options.StartIndex, options.StartIndex + options.Size - 1)
                };
            }

            long bytesDownloaded = 0;

            await destination.MatchAsync<object>(
                onString: async filePath =>
                {
                    await using var fileStream = System.IO.File.Create(filePath);
                    await _gsClient.DownloadObjectAsync(bucketName, keyInBucket, fileStream, downloadOptions, cancellationToken);
                    bytesDownloaded = fileStream.Length;

                    if (!System.IO.File.Exists(filePath))
                    {
                        throw new InvalidOperationException("Download finished, but file does not exist locally");
                    }
                    return Task.CompletedTask;
                },
                onStream: async (stream, _) =>
                {
                    var initialPosition = stream.CanSeek ? stream.Position : 0;
                    await _gsClient.DownloadObjectAsync(bucketName, keyInBucket, stream, downloadOptions, cancellationToken);

                    if (stream.CanSeek)
                    {
                        bytesDownloaded = stream.Position - initialPosition;
                        try
                        {
                            stream.Position = 0;
                        }
                        catch
                        {
                            // Best effort to reset position
                        }
                    }
                    else
                    {
                        // For non-seekable streams, we can't easily determine bytes downloaded
                        bytesDownloaded = options?.Size ?? -1;
                    }
                    return Task.CompletedTask;
                });

            return OperationResult<long>.Success(bytesDownloaded);
        }
        catch (Exception e)
        {
            return OperationResult<long>.Failure($"Download failed: {e.Message}");
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
        if (_gsClient == null)
        {
            return OperationResult<FileMetadata>.Failure("Google Storage client is not initialized");
        }

        try
        {
            var acl = ConvertAccessibilityToAcl(accessibility);

            var copiedObject = await _gsClient.CopyObjectAsync(
                sourceBucketName,
                sourceKeyInBucket,
                destinationBucketName,
                destinationKeyInBucket,
                new CopyObjectOptions
                {
                    DestinationPredefinedAcl = acl
                },
                cancellationToken);

            if (copiedObject == null)
            {
                return OperationResult<FileMetadata>.Failure("Copy operation failed");
            }

            copiedObject.CacheControl = acl == PredefinedObjectAcl.PublicRead ? "public" : "private";
            await _gsClient.PatchObjectAsync(copiedObject, new PatchObjectOptions(), cancellationToken);

            var metadata = CreateFileMetadata(copiedObject);
            return OperationResult<FileMetadata>.Success(metadata);
        }
        catch (Exception e)
        {
            return OperationResult<FileMetadata>.Failure($"Copy failed: {e.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> DeleteFileAsync(
        string bucketName,
        string keyInBucket,
        CancellationToken cancellationToken = default)
    {
        if (_gsClient == null)
        {
            return OperationResult<bool>.Failure("Google Storage client is not initialized");
        }

        try
        {
            await _gsClient.DeleteObjectAsync(bucketName, keyInBucket, new DeleteObjectOptions(), cancellationToken);
            return OperationResult<bool>.Success(true);
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure($"Delete failed: {e.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<int>> DeleteFolderAsync(
        string bucketName,
        string folderPrefix,
        CancellationToken cancellationToken = default)
    {
        if (_gsClient == null)
        {
            return OperationResult<int>.Failure("Google Storage client is not initialized");
        }

        try
        {
            var deletedCount = 0;
            var listResult = _gsClient.ListObjectsAsync(bucketName, folderPrefix);

            await foreach (var obj in listResult.WithCancellation(cancellationToken))
            {
                await _gsClient.DeleteObjectAsync(obj.Bucket, obj.Name, new DeleteObjectOptions(), cancellationToken);
                deletedCount++;
            }

            return OperationResult<int>.Success(deletedCount);
        }
        catch (Exception e)
        {
            return OperationResult<int>.Failure($"Delete folder failed: {e.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> FileExistsAsync(
        string bucketName,
        string keyInBucket,
        CancellationToken cancellationToken = default)
    {
        if (_gsClient == null)
        {
            return OperationResult<bool>.Failure("Google Storage client is not initialized");
        }

        try
        {
            var obj = await _gsClient.GetObjectAsync(bucketName, keyInBucket, new GetObjectOptions(), cancellationToken);
            return OperationResult<bool>.Success(obj != null);
        }
        catch (GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return OperationResult<bool>.Success(false);
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure($"File existence check failed: {e.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<long>> GetFileSizeAsync(
        string bucketName,
        string keyInBucket,
        CancellationToken cancellationToken = default)
    {
        if (_gsClient == null)
        {
            return OperationResult<long>.Failure("Google Storage client is not initialized");
        }

        try
        {
            var obj = await _gsClient.GetObjectAsync(bucketName, keyInBucket, new GetObjectOptions(), cancellationToken);
            if (obj?.Size == null)
            {
                return OperationResult<long>.Failure("Could not retrieve file size");
            }

            return OperationResult<long>.Success((long)obj.Size.Value);
        }
        catch (Exception e)
        {
            return OperationResult<long>.Failure($"Get file size failed: {e.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<string>> GetFileChecksumAsync(
        string bucketName,
        string keyInBucket,
        CancellationToken cancellationToken = default)
    {
        if (_gsClient == null)
        {
            return OperationResult<string>.Failure("Google Storage client is not initialized");
        }

        try
        {
            var obj = await _gsClient.GetObjectAsync(bucketName, keyInBucket, new GetObjectOptions(), cancellationToken);
            if (obj?.Md5Hash == null)
            {
                return OperationResult<string>.Failure("Could not retrieve file checksum");
            }

            var checksum = BitConverter.ToString(Convert.FromBase64String(obj.Md5Hash)).Replace("-", string.Empty, StringComparison.Ordinal).ToLowerInvariant();
            return OperationResult<string>.Success(checksum);
        }
        catch (Exception e)
        {
            return OperationResult<string>.Failure($"Get file checksum failed: {e.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<FileMetadata>> GetFileMetadataAsync(
        string bucketName,
        string keyInBucket,
        CancellationToken cancellationToken = default)
    {
        if (_gsClient == null)
        {
            return OperationResult<FileMetadata>.Failure("Google Storage client is not initialized");
        }

        try
        {
            var obj = await _gsClient.GetObjectAsync(bucketName, keyInBucket, new GetObjectOptions() { Projection = Projection.Full }, cancellationToken);
            if (obj == null)
            {
                return OperationResult<FileMetadata>.Failure("File not found");
            }

            var metadata = CreateFileMetadata(obj);
            return OperationResult<FileMetadata>.Success(metadata);
        }
        catch (Exception e)
        {
            return OperationResult<FileMetadata>.Failure($"Get file metadata failed: {e.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<IReadOnlyDictionary<string, string>>> GetFileTagsAsync(
        string bucketName,
        string keyInBucket,
        CancellationToken cancellationToken = default)
    {
        if (_gsClient == null)
        {
            return OperationResult<IReadOnlyDictionary<string, string>>.Failure("Google Storage client is not initialized");
        }

        try
        {
            var obj = await _gsClient.GetObjectAsync(bucketName, keyInBucket, new GetObjectOptions(), cancellationToken);
            if (obj?.Metadata == null)
            {
                return OperationResult<IReadOnlyDictionary<string, string>>.Success(new Dictionary<string, string>());
            }

            var tags = obj.Metadata.ToDictionary(kvp => kvp.Key, kvp => kvp.Value, StringComparer.Ordinal);
            return OperationResult<IReadOnlyDictionary<string, string>>.Success(tags.AsReadOnly());
        }
        catch (Exception e)
        {
            return OperationResult<IReadOnlyDictionary<string, string>>.Failure($"Get file tags failed: {e.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> SetFileTagsAsync(
        string bucketName,
        string keyInBucket,
        IReadOnlyDictionary<string, string> tags,
        CancellationToken cancellationToken = default)
    {
        if (_gsClient == null)
        {
            return OperationResult<bool>.Failure("Google Storage client is not initialized");
        }

        try
        {
            var existingObject = await _gsClient.GetObjectAsync(bucketName, keyInBucket, new GetObjectOptions(), cancellationToken);
            if (existingObject == null)
            {
                return OperationResult<bool>.Failure("File not found");
            }

            existingObject.Metadata = new Dictionary<string, string>(tags, StringComparer.Ordinal);

            var fileName = Path.GetFileName(keyInBucket);
            if (!string.IsNullOrEmpty(fileName))
            {
                existingObject.ContentDisposition = $"inline; filename={fileName}";
            }

            var updatedObject = await _gsClient.UpdateObjectAsync(existingObject, new UpdateObjectOptions(), cancellationToken);
            return OperationResult<bool>.Success(updatedObject != null);
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure($"Set file tags failed: {e.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> SetFileAccessibilityAsync(
        string bucketName,
        string keyInBucket,
        FileAccessibility accessibility,
        CancellationToken cancellationToken = default)
    {
        if (_gsClient == null)
        {
            return OperationResult<bool>.Failure("Google Storage client is not initialized");
        }

        try
        {
            var acl = ConvertAccessibilityToAcl(accessibility);
            var existingObject = await _gsClient.GetObjectAsync(bucketName, keyInBucket, new GetObjectOptions(), cancellationToken);
            if (existingObject == null)
            {
                return OperationResult<bool>.Failure("File not found");
            }

            existingObject.CacheControl = acl == PredefinedObjectAcl.PublicRead ? "public" : "private";

            var updatedObject = await _gsClient.UpdateObjectAsync(existingObject, new UpdateObjectOptions
            {
                PredefinedAcl = acl
            }, cancellationToken);

            return OperationResult<bool>.Success(updatedObject != null);
        }
        catch (Exception e)
        {
            return OperationResult<bool>.Failure($"Set file accessibility failed: {e.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<SignedUrl>> CreateSignedUploadUrlAsync(
        string bucketName,
        string keyInBucket,
        SignedUploadUrlOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        if (_credentialScoped == null)
        {
            return OperationResult<SignedUrl>.Failure("Credentials not available for signing URLs");
        }

        try
        {
            options ??= new SignedUploadUrlOptions();

            var signer = UrlSigner.FromCredential(_credentialScoped);
            var supportedHeaders = new Dictionary<string, IEnumerable<string>>(StringComparer.OrdinalIgnoreCase);

            if (!string.IsNullOrEmpty(options.ContentType))
            {
                supportedHeaders.Add("Content-Type", [options.ContentType]);
            }

            if (options.SupportResumable)
            {
                supportedHeaders.Add("x-goog-resumable", ["start"]);
            }

            var template = UrlSigner.RequestTemplate
                .FromBucket(bucketName)
                .WithObjectName(keyInBucket)
                .WithHttpMethod(options.SupportResumable ? HttpMethod.Post : HttpMethod.Put);

            if (supportedHeaders.Count > 0)
            {
                template = template.WithContentHeaders(supportedHeaders);
            }

            var signedUrl = await signer.SignAsync(template, UrlSigner.Options.FromDuration(options.ValidFor), cancellationToken);
            var expiresAt = DateTimeOffset.UtcNow.Add(options.ValidFor);

            return OperationResult<SignedUrl>.Success(new SignedUrl(signedUrl, expiresAt));
        }
        catch (Exception e)
        {
            return OperationResult<SignedUrl>.Failure($"Create signed upload URL failed: {e.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<SignedUrl>> CreateSignedDownloadUrlAsync(
        string bucketName,
        string keyInBucket,
        SignedDownloadUrlOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        if (_credentialScoped == null)
        {
            return OperationResult<SignedUrl>.Failure("Credentials not available for signing URLs");
        }

        try
        {
            options ??= new SignedDownloadUrlOptions();

            var signer = UrlSigner.FromCredential(_credentialScoped);
            var template = UrlSigner.RequestTemplate
                .FromBucket(bucketName)
                .WithObjectName(keyInBucket)
                .WithHttpMethod(HttpMethod.Get);

            var signedUrl = await signer.SignAsync(template, UrlSigner.Options.FromDuration(options.ValidFor), cancellationToken);
            var expiresAt = DateTimeOffset.UtcNow.Add(options.ValidFor);

            return OperationResult<SignedUrl>.Success(new SignedUrl(signedUrl, expiresAt));
        }
        catch (Exception e)
        {
            return OperationResult<SignedUrl>.Failure($"Create signed download URL failed: {e.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<ListFilesResult>> ListFilesAsync(
        string bucketName,
        ListFilesOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        if (_gsClient == null)
        {
            return OperationResult<ListFilesResult>.Failure("Google Storage client is not initialized");
        }

        try
        {
            var listOptions = new ListObjectsOptions
            {
                PageToken = options?.ContinuationToken
            };

            if (options?.MaxResults.HasValue == true)
            {
                listOptions.PageSize = options.MaxResults.Value;
            }

            var listResult = await _gsClient.ListObjectsAsync(bucketName, options?.Prefix, listOptions).ReadPageAsync(listOptions.PageSize ?? 1000, cancellationToken);

            var fileKeys = listResult.Select(obj => obj.Name).ToList();
            var result = new ListFilesResult
            {
                FileKeys = fileKeys.AsReadOnly(),
                NextContinuationToken = listResult.NextPageToken
            };

            return OperationResult<ListFilesResult>.Success(result);
        }
        catch (Exception e)
        {
            return OperationResult<ListFilesResult>.Failure($"List files failed: {e.Message}");
        }
    }

    /// <summary>
    /// <inheritdoc />
    /// Note: The pub/sub publisher role must be added to the storage service account.
    /// For example:
    /// gcloud projects add-iam-policy-binding cross-cloud-kit \
    /// --member="serviceAccount:service-xxx@gs-project-accounts.iam.gserviceaccount.com" \
    /// --role="roles/pubsub.publisher"
    /// </summary>
    public async Task<OperationResult<string>> CreateNotificationAsync(
        string bucketName,
        string topicName,
        string pathPrefix,
        IReadOnlyList<FileNotificationEventType> eventTypes,
        IPubSubService pubSubService,
        CancellationToken cancellationToken = default)
    {
        if (_gsClient == null)
        {
            return OperationResult<string>.Failure("Google Storage client is not initialized");
        }

        var fullTopicName = $"//pubsub.googleapis.com/projects/{_projectId}/topics/{topicName}";

        try
        {
            var googleEventTypes = new List<string>();
            foreach (var eventType in eventTypes)
            {
                switch (eventType)
                {
                    case FileNotificationEventType.Uploaded:
                        googleEventTypes.Add("OBJECT_FINALIZE");
                        break;
                    case FileNotificationEventType.Deleted:
                        googleEventTypes.Add("OBJECT_DELETE");
                        break;
                }
            }

            var notification = new Google.Apis.Storage.v1.Data.Notification()
            {
                PayloadFormat = "JSON_API_V1",
                Topic = fullTopicName,
                EventTypes = googleEventTypes,
                ObjectNamePrefix = pathPrefix
            };

            try
            {
                var created = await _gsClient.CreateNotificationAsync(bucketName, notification, new CreateNotificationOptions(), cancellationToken);
                if (created?.Id == null)
                {
                    return OperationResult<string>.Failure("Notification could not be created");
                }

                return !(await pubSubService.MarkUsedOnBucketEvent(topicName, cancellationToken)).IsSuccessful ? OperationResult<string>.Failure("Unable to mark queue as used on bucket event.") : OperationResult<string>.Success(created.Id);
            }
            catch (GoogleApiException ex) when (
                (ex.HttpStatusCode == System.Net.HttpStatusCode.BadRequest && ex.Message.Contains("not found")) ||
                (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound))
            {
                // Topic does not exist, try to create it and retry
                var ensureResult = await pubSubService.EnsureTopicExistsAsync(topicName, cancellationToken);
                if (!ensureResult.IsSuccessful)
                {
                    return OperationResult<string>.Failure($"Notification could not be created: {ensureResult.ErrorMessage}");
                }

                return await CreateNotificationAsync(bucketName, topicName, pathPrefix, eventTypes, pubSubService, cancellationToken);
            }
        }
        catch (Exception e)
        {
            return OperationResult<string>.Failure($"Create notification failed: {e.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<int>> DeleteNotificationsAsync(
        IPubSubService pubSubService,
        string bucketName,
        string? topicName = null,
        CancellationToken cancellationToken = default)
    {
        if (_gsClient == null)
        {
            return OperationResult<int>.Failure("Google Storage client is not initialized");
        }

        var fullTopicNamesToDelete = new Dictionary<string, string>();
        if (topicName == null)
        {
            var topicsToDelete = await pubSubService.GetTopicsUsedOnBucketEventAsync(cancellationToken: cancellationToken);
            if (!topicsToDelete.IsSuccessful || topicsToDelete.Data == null)
            {
                return OperationResult<int>.Failure("GetTopicsUsedOnBucketEventAsync has failed.");
            }

            foreach (var topic in topicsToDelete.Data)
            {
                fullTopicNamesToDelete.Add($"//pubsub.googleapis.com/projects/{_projectId}/topics/{topic}", topic);
            }
        }
        else
        {
            fullTopicNamesToDelete.Add($"//pubsub.googleapis.com/projects/{_projectId}/topics/{topicName}", topicName);
        }

        var notifications = await _gsClient.ListNotificationsAsync(bucketName, new ListNotificationsOptions(), cancellationToken);

        var deletedCount = 0;
        foreach (var notification in notifications)
        {
            if (!fullTopicNamesToDelete.TryGetValue(notification.Topic, out var tName)) continue;
            try
            {
                await _gsClient.DeleteNotificationAsync(bucketName, notification.Id, new DeleteNotificationOptions(), cancellationToken);

                if (!(await pubSubService.UnmarkUsedOnBucketEvent(tName, cancellationToken)).IsSuccessful)
                {
                    return OperationResult<int>.Failure("Unable to unmark queue as used on bucket event.");
                }
            }
            catch (Exception e)
            {
                return OperationResult<int>.Failure($"Delete notifications failed: {e.Message}");
            }
            deletedCount++;
        }
        return OperationResult<int>.Success(deletedCount);
    }

    private static PredefinedObjectAcl ConvertAccessibilityToAcl(FileAccessibility accessibility)
    {
        return accessibility switch
        {
            FileAccessibility.PublicRead => PredefinedObjectAcl.PublicRead,
            FileAccessibility.ProjectWideProtectedRead => PredefinedObjectAcl.ProjectPrivate,
            _ => PredefinedObjectAcl.AuthenticatedRead
        };
    }

    private static FileMetadata CreateFileMetadata(Google.Apis.Storage.v1.Data.Object obj)
    {
        var properties = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["Bucket"] = obj.Bucket ?? string.Empty,
            ["CacheControl"] = obj.CacheControl ?? string.Empty,
            ["ComponentCount"] = $"{obj.ComponentCount ?? -1}",
            ["ContentDisposition"] = obj.ContentDisposition ?? string.Empty,
            ["ContentEncoding"] = obj.ContentEncoding ?? string.Empty,
            ["ContentLanguage"] = obj.ContentLanguage ?? string.Empty,
            ["ContentType"] = obj.ContentType ?? string.Empty,
            ["Crc32c"] = obj.Crc32c ?? string.Empty,
            ["ETag"] = obj.ETag ?? string.Empty,
            ["Generation"] = $"{obj.Generation ?? -1}",
            ["Id"] = obj.Id ?? string.Empty,
            ["Kind"] = obj.Kind ?? string.Empty,
            ["KmsKeyName"] = obj.KmsKeyName ?? string.Empty,
            ["Md5Hash"] = obj.Md5Hash ?? string.Empty,
            ["MediaLink"] = obj.MediaLink ?? string.Empty,
            ["Metageneration"] = $"{obj.Metageneration ?? -1}",
            ["Name"] = obj.Name ?? string.Empty,
            ["Size"] = $"{obj.Size ?? 0}",
            ["StorageClass"] = obj.StorageClass ?? string.Empty,
            ["TimeCreated"] = obj.TimeCreatedRaw ?? string.Empty,
            ["Updated"] = obj.UpdatedRaw ?? string.Empty,
            ["EventBasedHold"] = $"{obj.EventBasedHold ?? false}",
            ["TemporaryHold"] = $"{obj.TemporaryHold ?? false}",
            ["RetentionExpirationTime"] = obj.RetentionExpirationTimeRaw ?? string.Empty,
        };

        var checksum = obj.Md5Hash != null
            ? BitConverter.ToString(Convert.FromBase64String(obj.Md5Hash)).Replace("-", string.Empty, StringComparison.Ordinal).ToLowerInvariant()
            : null;

        return new FileMetadata
        {
            Size = (long)(obj.Size ?? 0),
            Checksum = checksum,
            ContentType = obj.ContentType,
            CreatedAt = obj.TimeCreatedDateTimeOffset,
            LastModified = obj.UpdatedDateTimeOffset,
            Properties = properties.AsReadOnly(),
            Tags = (obj.Metadata ?? new Dictionary<string, string>()).AsReadOnly()
        };
    }

    public async ValueTask DisposeAsync()
    {
        _gsClient?.Dispose();
        // ReSharper disable once GCSuppressFinalizeForTypeWithoutDestructor
        System.GC.SuppressFinalize(this);
        await Task.CompletedTask;
    }
}
