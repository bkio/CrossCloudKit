// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Security.Cryptography;
using CrossCloudKit.File.Common.MonitorBasedPubSub;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Utilities.Common;
using Newtonsoft.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;

namespace CrossCloudKit.File.Basic;

public class FileServiceBasic : IFileService, IAsyncDisposable
{
    private const string RootFolderName = "CrossCloudKit.File.Basic";
    private const string TokensSubfolder = ".tokens";
    private const string UploadTokensSubSubfolder = ".upload";
    private const string DownloadTokensSubSubfolder = ".download";
    private const string MetadataSubfolder = ".metadata";

    public FileServiceBasic(
        IMemoryService? memoryService = null,
        IPubSubService? pubSubService = null,
        string? basePath = null,
        WebApplication? webApplicationForSignedUrls = null,
        string? publicEndpointBaseForSignedUrls = null)
    {
        // Use a default path in the temp directory if not specified
        _basePath = basePath ?? Path.Combine(Path.GetTempPath(), RootFolderName);
        _webApplication = webApplicationForSignedUrls;
        _publicEndpointBase = publicEndpointBaseForSignedUrls?.TrimEnd('/') ?? "";

        try
        {
            // Ensure the base directory exists
            Directory.CreateDirectory(_basePath);

            IsInitialized = true;

            _monitorBasedPubSub = new MonitorBasedPubSub(this, memoryService, pubSubService);

            // Register endpoints if a WebApplication is provided
            if (_webApplication != null)
            {
                RegisterSignedUrlEndpoints();
            }

            // Start a timer to clean up expired tokens every minute
            _tokenCleanupTimer = new Timer(CleanupExpiredTokens, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        }
        catch
        {
            IsInitialized = false;
        }
    }

    private readonly MonitorBasedPubSub? _monitorBasedPubSub;
    private bool _disposed;

    private readonly string _basePath;
    private readonly WebApplication? _webApplication;
    private readonly string _publicEndpointBase;
    private readonly Timer? _tokenCleanupTimer;

    public bool IsInitialized { get; }

    /// <inheritdoc />
    public async Task<OperationResult<FileMetadata>> UploadFileAsync(StringOrStream content, string bucketName, string keyInBucket,
        FileAccessibility accessibility = FileAccessibility.AuthenticatedRead, IReadOnlyDictionary<string, string>? tags = null,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<FileMetadata>.Failure("Service not initialized");

        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);
        ArgumentException.ThrowIfNullOrWhiteSpace(keyInBucket);

        await using var mutex = await CreateFileMutexScopeAsync(bucketName, cancellationToken);

        try
        {
            var filePath = GetFilePath(bucketName, keyInBucket);
            var directory = Path.GetDirectoryName(filePath).NotNull();
            Directory.CreateDirectory(directory);

            var checksum = "";
            var contentType = "";

            await content.MatchAsync<object>(
                async sourceFilePath =>
                {
                    if (!System.IO.File.Exists(sourceFilePath))
                        throw new FileNotFoundException($"File not found: {sourceFilePath}");

                    System.IO.File.Copy(sourceFilePath, filePath, true);
                    checksum = await CalculateFileChecksumAsync(filePath, cancellationToken);
                    contentType = GetContentType(filePath);
                    return new object();
                },
                async (stream, _) =>
                {
                    // Write to file and ensure stream is disposed before calculating checksum
                    await using (var fileStream = new FileStream(filePath, FileMode.Create, FileAccess.Write))
                    {
                        await stream.CopyToAsync(fileStream, cancellationToken);
                        await fileStream.FlushAsync(cancellationToken);
                    }

                    // Now calculate checksum after file stream is fully disposed
                    checksum = await CalculateFileChecksumAsync(filePath, cancellationToken);
                    contentType = GetContentType(filePath);
                    return new object();
                });

            // Get actual file size after writing
            var fileInfo = new FileInfo(filePath);
            var actualSize = fileInfo.Length;

            var now = DateTimeOffset.UtcNow;
            var metadata = new FileMetadata
            {
                Size = actualSize,
                Checksum = checksum,
                ContentType = contentType,
                CreatedAt = now,
                LastModified = now,
                Properties = new Dictionary<string, string>(),
                Tags = tags?.ToDictionary(kvp => kvp.Key, kvp => kvp.Value) ?? new Dictionary<string, string>()
            };

            // Save metadata
            await SaveFileMetadataAsync(bucketName, keyInBucket, metadata, cancellationToken);

            return OperationResult<FileMetadata>.Success(metadata);
        }
        catch (Exception ex)
        {
            return OperationResult<FileMetadata>.Failure($"Upload failed: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<long>> DownloadFileAsync(string bucketName, string keyInBucket, StringOrStream destination,
        DownloadOptions? options = null, CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<long>.Failure("Service not initialized");

        try
        {
            var filePath = GetFilePath(bucketName, keyInBucket);
            if (!System.IO.File.Exists(filePath))
                return OperationResult<long>.Failure("File does not exist");

            return await destination.MatchAsync(
                async targetFilePath =>
                {
                    if (options?.StartIndex > 0 || options?.Size > 0)
                    {
                        // Handle partial file copy
                        await using var sourceStream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
                        await using var targetStream = new FileStream(targetFilePath, FileMode.Create, FileAccess.Write);

                        if (options.StartIndex > 0)
                            sourceStream.Seek(options.StartIndex, SeekOrigin.Begin);

                        var bytesToRead = options.Size > 0 ? options.Size : sourceStream.Length - sourceStream.Position;
                        var buffer = new byte[8192];
                        long totalRead = 0;

                        while (totalRead < bytesToRead)
                        {
                            var toRead = (int)Math.Min(buffer.Length, bytesToRead - totalRead);
                            var bytesRead = await sourceStream.ReadAsync(buffer.AsMemory(0, toRead), cancellationToken);
                            if (bytesRead == 0) break;

                            await targetStream.WriteAsync(buffer.AsMemory(0, bytesRead), cancellationToken);
                            totalRead += bytesRead;
                        }

                        return OperationResult<long>.Success(totalRead);
                    }
                    else
                    {
                        System.IO.File.Copy(filePath, targetFilePath, true);
                        return OperationResult<long>.Success(new FileInfo(targetFilePath).Length);
                    }
                },
                async (stream, _) =>
                {
                    await using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read);

                    if (options?.StartIndex > 0)
                        fileStream.Seek(options.StartIndex, SeekOrigin.Begin);

                    var bytesToRead = options?.Size > 0 ? options.Size : fileStream.Length - fileStream.Position;
                    var buffer = new byte[8192];
                    long totalRead = 0;

                    while (totalRead < bytesToRead)
                    {
                        var toRead = (int)Math.Min(buffer.Length, bytesToRead - totalRead);
                        var bytesRead = await fileStream.ReadAsync(buffer.AsMemory(0, toRead), cancellationToken);
                        if (bytesRead == 0) break;

                        await stream.WriteAsync(buffer.AsMemory(0, bytesRead), cancellationToken);
                        totalRead += bytesRead;
                    }

                    return OperationResult<long>.Success(totalRead);
                });
        }
        catch (Exception ex)
        {
            return OperationResult<long>.Failure($"Download failed: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<FileMetadata>> CopyFileAsync(string sourceBucketName, string sourceKeyInBucket, string destinationBucketName,
        string destinationKeyInBucket, FileAccessibility accessibility = FileAccessibility.AuthenticatedRead,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<FileMetadata>.Failure("Service not initialized");

        await using var mutex1 = await CreateFileMutexScopeAsync(sourceBucketName, cancellationToken);
        await using var mutex2 = await
            (sourceBucketName != destinationBucketName
                ? CreateFileMutexScopeAsync(destinationBucketName, cancellationToken)
                : MonitorBasedPubSub.CreateNoopAsyncDisposableAsync());

        try
        {
            var sourceFilePath = GetFilePath(sourceBucketName, sourceKeyInBucket);
            if (!System.IO.File.Exists(sourceFilePath))
                return OperationResult<FileMetadata>.Failure("Source file does not exist");

            var destinationFilePath = GetFilePath(destinationBucketName, destinationKeyInBucket);
            var directory = Path.GetDirectoryName(destinationFilePath).NotNull();
            Directory.CreateDirectory(directory);

            System.IO.File.Copy(sourceFilePath, destinationFilePath, true);

            // Copy metadata and update accessibility
            var sourceMetadataResult = await InternalGetFileMetadataUnsafeAsync(sourceBucketName, sourceKeyInBucket, cancellationToken);
            if (!sourceMetadataResult.IsSuccessful || sourceMetadataResult.Data == null)
                return OperationResult<FileMetadata>.Failure("Failed to get source file metadata");

            var newMetadata = new FileMetadata
            {
                Size = sourceMetadataResult.Data.Size,
                Checksum = await CalculateFileChecksumAsync(destinationFilePath, cancellationToken),
                ContentType = sourceMetadataResult.Data.ContentType,
                CreatedAt = DateTimeOffset.UtcNow,
                LastModified = DateTimeOffset.UtcNow,
                Properties = sourceMetadataResult.Data.Properties,
                Tags = sourceMetadataResult.Data.Tags
            };

            await SaveFileMetadataAsync(destinationBucketName, destinationKeyInBucket, newMetadata, cancellationToken);

            return OperationResult<FileMetadata>.Success(newMetadata);
        }
        catch (Exception ex)
        {
            return OperationResult<FileMetadata>.Failure($"Copy failed: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> DeleteFileAsync(string bucketName, string keyInBucket, CancellationToken cancellationToken = default)
    {
        await using var mutex = await CreateFileMutexScopeAsync(bucketName, cancellationToken);
        return await InternalDeleteFileUnsafeAsync(bucketName, keyInBucket);
    }
    private Task<OperationResult<bool>> InternalDeleteFileUnsafeAsync(string bucketName, string keyInBucket)
    {
        if (!IsInitialized)
            return Task.FromResult(OperationResult<bool>.Failure("Service not initialized"));

        try
        {
            var filePath = GetFilePath(bucketName, keyInBucket);
            var metadataPath = GetMetadataPath(bucketName, keyInBucket);

            if (!FileSystemUtilities.DeleteFileAndCleanupParentFolders(filePath, RootFolderName))
                return Task.FromResult(OperationResult<bool>.Failure("Failed to delete file (or cleanup parent folders)"));

            if (!FileSystemUtilities.DeleteFileAndCleanupParentFolders(metadataPath, RootFolderName))
                return Task.FromResult(OperationResult<bool>.Failure("Failed to delete metadata file (or cleanup parent folders)"));

            return Task.FromResult(OperationResult<bool>.Success(true));
        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<bool>.Failure($"Delete failed: {ex.Message}"));
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<int>> DeleteFolderAsync(string bucketName, string folderPrefix, CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<int>.Failure("Service not initialized");

        await using var mutex = await CreateFileMutexScopeAsync(bucketName, cancellationToken);

        try
        {
            var listResult = await InternalListFilesUnsafeAsync(bucketName, new ListFilesOptions { Prefix = folderPrefix });
            if (!listResult.IsSuccessful || listResult.Data == null)
                return OperationResult<int>.Failure("Failed to list files for deletion");

            var deletedCount = 0;
            foreach (var fileKey in listResult.Data.FileKeys)
            {
                var deleteResult = await InternalDeleteFileUnsafeAsync(bucketName, fileKey);
                if (deleteResult.IsSuccessful)
                    deletedCount++;
            }

            return OperationResult<int>.Success(deletedCount);
        }
        catch (Exception ex)
        {
            return OperationResult<int>.Failure($"Delete folder failed: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public Task<OperationResult<bool>> FileExistsAsync(string bucketName, string keyInBucket, CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return Task.FromResult(OperationResult<bool>.Failure("Service not initialized"));

        try
        {
            var filePath = GetFilePath(bucketName, keyInBucket);
            return Task.FromResult(OperationResult<bool>.Success(System.IO.File.Exists(filePath)));
        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<bool>.Failure($"File existence check failed: {ex.Message}"));
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<long>> GetFileSizeAsync(string bucketName, string keyInBucket, CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<long>.Failure("Service not initialized");

        await using var mutex = await CreateFileMutexScopeAsync(bucketName, cancellationToken);

        try
        {
            var filePath = GetFilePath(bucketName, keyInBucket);
            if (!System.IO.File.Exists(filePath))
                return OperationResult<long>.Failure("File does not exist");

            var fileInfo = new FileInfo(filePath);
            return OperationResult<long>.Success(fileInfo.Length);
        }
        catch (Exception ex)
        {
            return OperationResult<long>.Failure($"Get file size failed: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<string>> GetFileChecksumAsync(string bucketName, string keyInBucket, CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<string>.Failure("Service not initialized");

        await using var mutex = await CreateFileMutexScopeAsync(bucketName, cancellationToken);

        try
        {
            var filePath = GetFilePath(bucketName, keyInBucket);
            if (!System.IO.File.Exists(filePath))
                return OperationResult<string>.Failure("File does not exist");

            var checksum = await CalculateFileChecksumAsync(filePath, cancellationToken);
            return OperationResult<string>.Success(checksum);
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
        await using var mutex = await CreateFileMutexScopeAsync(bucketName, cancellationToken);
        return await InternalGetFileMetadataUnsafeAsync(bucketName, keyInBucket, cancellationToken);
    }

    private async Task<OperationResult<FileMetadata>> InternalGetFileMetadataUnsafeAsync(string bucketName, string keyInBucket, CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<FileMetadata>.Failure("Service not initialized");

        try
        {
            var filePath = GetFilePath(bucketName, keyInBucket);
            if (!System.IO.File.Exists(filePath))
                return OperationResult<FileMetadata>.Failure("File does not exist");

            var fileInfo = new FileInfo(filePath);
            var metadataPath = GetMetadataPath(bucketName, keyInBucket);

            var metadata = new FileMetadata
            {
                Size = fileInfo.Length,
                Checksum = await CalculateFileChecksumAsync(filePath, cancellationToken),
                ContentType = GetContentType(filePath),
                CreatedAt = fileInfo.CreationTimeUtc,
                LastModified = fileInfo.LastWriteTimeUtc,
                Properties = new Dictionary<string, string>(),
                Tags = new Dictionary<string, string>()
            };

            // Load saved metadata if it exists
            if (System.IO.File.Exists(metadataPath))
            {
                try
                {
                    var savedMetadataJson = await System.IO.File.ReadAllTextAsync(metadataPath, cancellationToken);
                    var savedMetadata = JsonConvert.DeserializeObject<SavedFileMetadata>(savedMetadataJson);
                    if (savedMetadata != null)
                    {
                        metadata = new FileMetadata
                        {
                            Size = metadata.Size,
                            Checksum = metadata.Checksum,
                            ContentType = savedMetadata.ContentType ?? metadata.ContentType,
                            CreatedAt = savedMetadata.CreatedAt ?? metadata.CreatedAt,
                            LastModified = metadata.LastModified,
                            Properties = savedMetadata.Properties ?? metadata.Properties,
                            Tags = savedMetadata.Tags ?? metadata.Tags
                        };
                    }
                }
                catch
                {
                    // Use calculated metadata if saved metadata is corrupted
                }
            }

            return OperationResult<FileMetadata>.Success(metadata);
        }
        catch (Exception ex)
        {
            return OperationResult<FileMetadata>.Failure($"Get file metadata failed: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<IReadOnlyDictionary<string, string>>> GetFileTagsAsync(string bucketName, string keyInBucket, CancellationToken cancellationToken = default)
    {
        await using var mutex = await CreateFileMutexScopeAsync(bucketName, cancellationToken);

        var metadataResult = await InternalGetFileMetadataUnsafeAsync(bucketName, keyInBucket, cancellationToken);
        if (!metadataResult.IsSuccessful || metadataResult.Data == null)
            return OperationResult<IReadOnlyDictionary<string, string>>.Failure(metadataResult.ErrorMessage.NotNull());

        return OperationResult<IReadOnlyDictionary<string, string>>.Success(metadataResult.Data.Tags);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> SetFileTagsAsync(string bucketName, string keyInBucket, IReadOnlyDictionary<string, string> tags,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<bool>.Failure("Service not initialized");

        await using var mutex = await CreateFileMutexScopeAsync(bucketName, cancellationToken);

        try
        {
            var filePath = GetFilePath(bucketName, keyInBucket);
            if (!System.IO.File.Exists(filePath))
                return OperationResult<bool>.Failure("File does not exist");

            var metadataResult = await InternalGetFileMetadataUnsafeAsync(bucketName, keyInBucket, cancellationToken);
            if (!metadataResult.IsSuccessful || metadataResult.Data == null)
                return OperationResult<bool>.Failure("Failed to get existing metadata");

            var updatedMetadata = new FileMetadata
            {
                Size = metadataResult.Data.Size,
                Checksum = metadataResult.Data.Checksum,
                ContentType = metadataResult.Data.ContentType,
                CreatedAt = metadataResult.Data.CreatedAt,
                LastModified = metadataResult.Data.LastModified,
                Properties = metadataResult.Data.Properties,
                Tags = tags.ToDictionary(kvp => kvp.Key, kvp => kvp.Value)
            };

            await SaveFileMetadataAsync(bucketName, keyInBucket, updatedMetadata, cancellationToken);
            return OperationResult<bool>.Success(true);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure($"Set file tags failed: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> SetFileAccessibilityAsync(string bucketName, string keyInBucket, FileAccessibility accessibility,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<bool>.Failure("Service not initialized");

        await using var mutex = await CreateFileMutexScopeAsync(bucketName, cancellationToken);

        try
        {
            var filePath = GetFilePath(bucketName, keyInBucket);
            if (!System.IO.File.Exists(filePath))
                return OperationResult<bool>.Failure("File does not exist");

            var metadataResult = await InternalGetFileMetadataUnsafeAsync(bucketName, keyInBucket, cancellationToken);
            if (!metadataResult.IsSuccessful || metadataResult.Data == null)
                return OperationResult<bool>.Failure("Failed to get existing metadata");

            await SaveFileMetadataAsync(bucketName, keyInBucket, metadataResult.Data, cancellationToken);
            return OperationResult<bool>.Success(true);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure($"Set file accessibility failed: {ex.Message}");
        }
    }

    private void RegisterSignedUrlEndpoints()
    {
        if (_webApplication == null) return;

        // Register upload endpoint
        _webApplication.MapPut("/signed-upload/{token}", async (string token, HttpRequest request, CancellationToken cancellationToken) =>
        {
            try
            {
                var tokenPath = GetUploadTokenPath(token);
                if (!System.IO.File.Exists(tokenPath))
                    return Results.NotFound("Invalid or expired upload token");

                var tokenJson = await System.IO.File.ReadAllTextAsync(tokenPath, cancellationToken);
                var uploadInfo = JsonConvert.DeserializeObject<UploadTokenInfo>(tokenJson);

                if (uploadInfo == null || uploadInfo.ExpiresAt < DateTime.UtcNow)
                {
                    System.IO.File.Delete(tokenPath);
                    return Results.BadRequest("Upload token expired");
                }

                // Ensure the content type matches if specified
                if (!string.IsNullOrEmpty(uploadInfo.ContentType) &&
                    !string.Equals(request.ContentType, uploadInfo.ContentType, StringComparison.OrdinalIgnoreCase))
                {
                    return Results.BadRequest($"Content type mismatch. Expected: {uploadInfo.ContentType}");
                }

                await using var requestStream = request.Body;
                var uploadResult = await UploadFileAsync(
                    new StringOrStream(requestStream, request.ContentLength ?? 0),
                    uploadInfo.BucketName,
                    uploadInfo.KeyInBucket,
                    cancellationToken: cancellationToken);

                // Cleanup token after a successful upload
                System.IO.File.Delete(tokenPath);

                return !uploadResult.IsSuccessful
                    ? Results.BadRequest($"Upload failed: {uploadResult.ErrorMessage}")
                    : Results.Ok(new { success = true, metadata = uploadResult.Data });
            }
            catch (Exception ex)
            {
                return Results.Problem($"Upload failed: {ex.Message}");
            }
        });

        // Register download endpoint
        _webApplication.MapGet("/signed-download/{token}", async (string token, CancellationToken cancellationToken) =>
        {
            try
            {
                var tokenPath = GetDownloadTokenPath(token);
                if (!System.IO.File.Exists(tokenPath))
                    return Results.NotFound("Invalid or expired download token");

                var tokenJson = await System.IO.File.ReadAllTextAsync(tokenPath, cancellationToken);
                var downloadInfo = JsonConvert.DeserializeObject<DownloadTokenInfo>(tokenJson);

                if (downloadInfo == null || downloadInfo.ExpiresAt < DateTime.UtcNow)
                {
                    System.IO.File.Delete(tokenPath);
                    return Results.BadRequest("Download token expired");
                }

                await using var mutex = await CreateFileMutexScopeAsync(downloadInfo.BucketName, cancellationToken);

                var filePath = GetFilePath(downloadInfo.BucketName, downloadInfo.KeyInBucket);
                if (!System.IO.File.Exists(filePath))
                {
                    System.IO.File.Delete(tokenPath);
                    return Results.NotFound("File not found");
                }

                var fileInfo = new FileInfo(filePath);
                var contentType = GetContentType(filePath);

                // Clean up token after successful access
                System.IO.File.Delete(tokenPath);

                return Results.File(filePath, contentType, downloadInfo.KeyInBucket, fileInfo.LastWriteTimeUtc);
            }
            catch (Exception ex)
            {
                return Results.Problem($"Download failed: {ex.Message}");
            }
        });
    }

    /// <inheritdoc />
    public async Task<OperationResult<SignedUrl>> CreateSignedUploadUrlAsync(string bucketName, string keyInBucket, SignedUploadUrlOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<SignedUrl>.Failure("Service not initialized");

        if (_webApplication == null)
            return OperationResult<SignedUrl>.Failure("WebApplication not registered");

        await using var mutex = await CreateFileMutexScopeAsync(bucketName, cancellationToken);

        try
        {
            // For a local file system, create a temporary upload token
            var validFor = options?.ValidFor ?? TimeSpan.FromHours(1);
            var token = Guid.NewGuid().ToString("N");
            var expires = DateTime.UtcNow.Add(validFor);

            // Store the upload token with expiration
            var uploadInfo = new UploadTokenInfo
            {
                BucketName = bucketName,
                KeyInBucket = keyInBucket,
                ContentType = options?.ContentType,
                ExpiresAt = expires
            };

            var tokenPath = GetUploadTokenPath(token);
            var directory = Path.GetDirectoryName(tokenPath).NotNull();
            Directory.CreateDirectory(directory);

            var tokenJson = JsonConvert.SerializeObject(uploadInfo);
            await FileSystemUtilities.WriteToFileEnsureWrittenToDiskAsync(tokenJson, tokenPath, cancellationToken);

            // Generate a proper HTTP URL if WebApplication is registered, otherwise fallback to file:// protocol
            var signedUrlString = $"{_publicEndpointBase}/signed-upload/{token}";

            var signedUrl = new SignedUrl(signedUrlString, expires);
            return OperationResult<SignedUrl>.Success(signedUrl);
        }
        catch (Exception ex)
        {
            return OperationResult<SignedUrl>.Failure($"Create signed upload URL failed: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<SignedUrl>> CreateSignedDownloadUrlAsync(string bucketName, string keyInBucket, SignedDownloadUrlOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<SignedUrl>.Failure("Service not initialized");

        if (_webApplication == null)
            return OperationResult<SignedUrl>.Failure("WebApplication not registered");

        await using var mutex = await CreateFileMutexScopeAsync(bucketName, cancellationToken);

        try
        {
            var filePath = GetFilePath(bucketName, keyInBucket);
            if (!System.IO.File.Exists(filePath))
                return OperationResult<SignedUrl>.Failure("File does not exist");

            var validFor = options?.ValidFor ?? TimeSpan.FromMinutes(1);
            var token = Guid.NewGuid().ToString("N");
            var expires = DateTime.UtcNow.Add(validFor);

            // Store the download token with expiration
            var downloadInfo = new DownloadTokenInfo
            {
                BucketName = bucketName,
                KeyInBucket = keyInBucket,
                ExpiresAt = expires
            };

            var tokenPath = GetDownloadTokenPath(token);
            var directory = Path.GetDirectoryName(tokenPath).NotNull();
            Directory.CreateDirectory(directory);

            var tokenJson = JsonConvert.SerializeObject(downloadInfo);
            await FileSystemUtilities.WriteToFileEnsureWrittenToDiskAsync(tokenJson, tokenPath, cancellationToken);

            // Generate a proper HTTP URL if WebApplication is registered, otherwise fallback to file:// protocol
            var signedUrlString = $"{_publicEndpointBase}/signed-download/{token}";

            var signedUrl = new SignedUrl(signedUrlString, expires);
            return OperationResult<SignedUrl>.Success(signedUrl);
        }
        catch (Exception ex)
        {
            return OperationResult<SignedUrl>.Failure($"Create signed download URL failed: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<ListFilesResult>> ListFilesAsync(string bucketName, ListFilesOptions? options = null, CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<ListFilesResult>.Failure("Service not initialized");

        await using var mutex = await CreateFileMutexScopeAsync(bucketName, cancellationToken);

        return await InternalListFilesUnsafeAsync(bucketName, options);
    }
    private Task<OperationResult<ListFilesResult>> InternalListFilesUnsafeAsync(string bucketName, ListFilesOptions? options = null)
    {
        if (!IsInitialized)
            return Task.FromResult(OperationResult<ListFilesResult>.Failure("Service not initialized"));

        try
        {
            var bucketPath = GetBucketPath(bucketName);
            if (!Directory.Exists(bucketPath))
            {
                return Task.FromResult(OperationResult<ListFilesResult>.Success(new ListFilesResult
                {
                    FileKeys = new List<string>(),
                    NextContinuationToken = null
                }));
            }

            var allFiles = Directory.GetFiles(bucketPath, "*", SearchOption.AllDirectories)
                .Select(f => Path.GetRelativePath(bucketPath, f).Replace('\\', '/'))
                .Where(key => string.IsNullOrEmpty(options?.Prefix) || key.StartsWith(options.Prefix))
                .OrderBy(key => key)
                .ToList();

            var startIndex = 0;
            if (!string.IsNullOrEmpty(options?.ContinuationToken))
            {
                if (int.TryParse(options.ContinuationToken, out var parsedIndex))
                    startIndex = parsedIndex;
            }

            var maxResults = options?.MaxResults ?? 1000;
            var files = allFiles.Skip(startIndex).Take(maxResults).ToList();
            var nextToken = startIndex + files.Count < allFiles.Count ? (startIndex + files.Count).ToString() : null;

            var result = new ListFilesResult
            {
                FileKeys = files,
                NextContinuationToken = nextToken
            };

            return Task.FromResult(OperationResult<ListFilesResult>.Success(result));
        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<ListFilesResult>.Failure($"List files failed: {ex.Message}"));
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<string>> CreateNotificationAsync(string bucketName, string topicName, string pathPrefix, IReadOnlyList<FileNotificationEventType> eventTypes,
        IPubSubService pubSubService, CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || _disposed)
            return OperationResult<string>.Failure("File service is not initialized.");

        await using var mutex = await CreateFileMutexScopeAsync(bucketName, cancellationToken);

        return await _monitorBasedPubSub.NotNull().CreateNotificationAsync(
            bucketName,
            topicName,
            pathPrefix,
            eventTypes,
            pubSubService,
            cancellationToken);
    }

    /// <inheritdoc />
    public async Task<OperationResult<int>> DeleteNotificationsAsync(IPubSubService pubSubService, string bucketName, string? topicName = null,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || _disposed)
            return OperationResult<int>.Failure("File service is not initialized.");

        await using var mutex = await CreateFileMutexScopeAsync(bucketName, cancellationToken);

        return await _monitorBasedPubSub.NotNull().DeleteNotificationsAsync(
            pubSubService,
            bucketName,
            topicName,
            cancellationToken);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> CleanupBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        if (!IsInitialized) return OperationResult<bool>.Failure("Service not initialized.");

        await using var mutex = await CreateFileMutexScopeAsync(bucketName, cancellationToken);

        try
        {
            var bucketPath = GetBucketPath(bucketName);
            if (Directory.Exists(bucketPath))
            {
                Directory.Delete(bucketPath, true);
            }

            var metadataBucketPath = GetMetadataBucketPath(bucketName);
            if (Directory.Exists(metadataBucketPath))
            {
                Directory.Delete(metadataBucketPath, true);
            }
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure($"Cleanup bucket failed: {ex.Message}");
        }
        return await _monitorBasedPubSub.NotNull().CleanupBucketAsync(
            bucketName,
            cancellationToken);
    }

    // Helper methods for file paths and metadata
    private async Task<IAsyncDisposable> CreateFileMutexScopeAsync(
        string bucketName,
        CancellationToken cancellationToken)
    {
        var memoryService = _monitorBasedPubSub.NotNull().MemoryService;
        if (memoryService == null) return new NoopAsyncDisposable();
        return await MemoryServiceScopeMutex.CreateScopeAsync(
            memoryService,
            FileMutexScope,
            bucketName,
            TimeSpan.FromMinutes(1),
            cancellationToken);
    }
    private static readonly IMemoryServiceScope FileMutexScope = new LambdaMemoryServiceScope("CrossCloudKit.Database.Basic.DatabaseServiceBasic");

    private string GetBucketPath(string bucketName) => Path.Combine(_basePath, bucketName);
    private string GetMetadataBucketPath(string bucketName) => Path.Combine(_basePath, MetadataSubfolder, bucketName);

    private string GetFilePath(string bucketName, string keyInBucket) =>
        Path.Combine(GetBucketPath(bucketName), keyInBucket);

    private string GetMetadataPath(string bucketName, string keyInBucket) =>
        Path.Combine(GetMetadataBucketPath(bucketName), keyInBucket);

    private string GetUploadTokenPath(string token) =>
        Path.Combine(_basePath, TokensSubfolder, UploadTokensSubSubfolder, token + ".json");

    private string GetDownloadTokenPath(string token) =>
        Path.Combine(_basePath, TokensSubfolder, DownloadTokensSubSubfolder, token + ".json");

    private string GetTokensBasePath() => Path.Combine(_basePath, TokensSubfolder);

    private void CleanupExpiredTokens(object? state)
    {
        if (!IsInitialized || _disposed) return;

        try
        {
            CleanupExpiredTokensInDirectory(Path.Combine(_basePath, TokensSubfolder, UploadTokensSubSubfolder));
            CleanupExpiredTokensInDirectory(Path.Combine(_basePath, TokensSubfolder, DownloadTokensSubSubfolder));
        }
        catch
        {
            // Ignore cleanup errors to prevent timer from stopping
        }
    }

    private static void CleanupExpiredTokensInDirectory(string directory)
    {
        if (!Directory.Exists(directory)) return;

        var now = DateTime.UtcNow;
        var tokenFiles = Directory.GetFiles(directory, "*.json");

        foreach (var tokenFile in tokenFiles)
        {
            try
            {
                var tokenJson = System.IO.File.ReadAllText(tokenFile);

                // Try to parse as both upload and download token info
                DateTime expiresAt;
                var uploadInfo = JsonConvert.DeserializeObject<UploadTokenInfo>(tokenJson);
                if (uploadInfo != null)
                {
                    expiresAt = uploadInfo.ExpiresAt;
                }
                else
                {
                    var downloadInfo = JsonConvert.DeserializeObject<DownloadTokenInfo>(tokenJson);
                    if (downloadInfo == null) continue;
                    expiresAt = downloadInfo.ExpiresAt;
                }

                if (expiresAt < now)
                {
                    System.IO.File.Delete(tokenFile);
                }
            }
            catch
            {
                // If we can't parse the token file, consider it expired and delete it
                try
                {
                    System.IO.File.Delete(tokenFile);
                }
                catch
                {
                    // Ignore deletion errors
                }
            }
        }
    }

    private void CleanupAllTokenFiles()
    {
        try
        {
            var tokensBasePath = GetTokensBasePath();
            if (Directory.Exists(tokensBasePath))
            {
                Directory.Delete(tokensBasePath, true);
            }
        }
        catch
        {
            // Ignore cleanup errors during disposal
        }
    }

    private static async Task<string> CalculateFileChecksumAsync(string filePath, CancellationToken cancellationToken)
    {
        using var md5 = MD5.Create();
        await using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
        var hash = await md5.ComputeHashAsync(stream, cancellationToken);
        return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
    }

    private static string GetContentType(string filePath)
    {
        var extension = Path.GetExtension(filePath).ToLowerInvariant();
        return extension switch
        {
            ".txt" => "text/plain",
            ".html" => "text/html",
            ".htm" => "text/html",
            ".css" => "text/css",
            ".js" => "application/javascript",
            ".json" => "application/json",
            ".xml" => "application/xml",
            ".pdf" => "application/pdf",
            ".jpg" => "image/jpeg",
            ".jpeg" => "image/jpeg",
            ".png" => "image/png",
            ".gif" => "image/gif",
            ".bmp" => "image/bmp",
            ".zip" => "application/zip",
            ".tar" => "application/x-tar",
            ".gz" => "application/gzip",
            _ => "application/octet-stream"
        };
    }

    private async Task SaveFileMetadataAsync(string bucketName, string keyInBucket, FileMetadata metadata,
        CancellationToken cancellationToken)
    {
        var metadataPath = GetMetadataPath(bucketName, keyInBucket);
        var directory = Path.GetDirectoryName(metadataPath).NotNull();
        Directory.CreateDirectory(directory);

        var savedMetadata = new SavedFileMetadata
        {
            ContentType = metadata.ContentType,
            CreatedAt = metadata.CreatedAt,
            Properties = metadata.Properties,
            Tags = metadata.Tags
        };

        var json = JsonConvert.SerializeObject(savedMetadata, Formatting.Indented);
        await FileSystemUtilities.WriteToFileEnsureWrittenToDiskAsync(json, metadataPath, cancellationToken);
    }

    private class SavedFileMetadata
    {
        public string? ContentType { get; init; }
        public DateTimeOffset? CreatedAt { get; init; }
        public IReadOnlyDictionary<string, string>? Properties { get; init; }
        public IReadOnlyDictionary<string, string>? Tags { get; init; }
    }

    private class UploadTokenInfo
    {
        public string BucketName = "";
        public string KeyInBucket = "";
        public string? ContentType;
        public DateTime ExpiresAt;
    }

    private class DownloadTokenInfo
    {
        public string BucketName = "";
        public string KeyInBucket = "";
        public DateTime ExpiresAt;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        try
        {
            // Dispose timer first to stop cleanup operations
            if (_tokenCleanupTimer != null)
                await _tokenCleanupTimer.DisposeAsync();

            // Clean up all token files
            CleanupAllTokenFiles();

            // Dispose monitor-based pub/sub
            if (_monitorBasedPubSub != null)
                await _monitorBasedPubSub.DisposeAsync();
        }
        catch (Exception)
        {
            // Ignore exceptions during disposal
        }

        GC.SuppressFinalize(this);
    }
}
