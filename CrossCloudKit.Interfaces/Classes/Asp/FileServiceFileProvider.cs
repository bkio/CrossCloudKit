// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Primitives;
using System.Collections;
using CrossCloudKit.Interfaces.Records;
using CrossCloudKit.Utilities.Common;

namespace CrossCloudKit.Interfaces.Classes.Asp;

/// <summary>
/// File provider implementation that bridges IFileService with ASP.NET Core IFileProvider.
/// Allows CrossCloudKit file services to be used as static file providers in web applications.
/// For Watch functionality to work, pubSubService must be provided and should be the same type of service as the file service uses.
/// </summary>
public class FileServiceFileProvider(
    IFileService fileService,
    string bucketName,
    string rootPath,
    Action<Exception>? errorMessageAction = null,
    IPubSubService? pubSubService = null) : IFileProvider, IDisposable
{
    private readonly IFileService _fileService = fileService ?? throw new ArgumentNullException(nameof(fileService));
    private readonly string _bucketName = bucketName ?? throw new ArgumentNullException(nameof(bucketName));
    private readonly string _rootPath = $"{NormalizePath(rootPath ?? throw new ArgumentNullException(nameof(rootPath)))}/";
    private readonly Dictionary<string, FileServiceChangeToken> _activeTokens = new();
    private readonly Lock _tokensLock = new();
    private bool _disposed;

    /// <summary>
    /// Gets file information for the specified subpath.
    /// </summary>
    /// <param name="subpath">The relative path to the file</param>
    /// <returns>File information or NotFoundFileInfo if the file doesn't exist</returns>
    public IFileInfo GetFileInfo(string subpath)
    {
        if (!InitializationCheck())
        {
            return new NotFoundFileInfo(subpath);
        }

        // Normalize the path
        var normalizedPath = NormalizePath(subpath);
        if (string.IsNullOrEmpty(normalizedPath))
        {
            errorMessageAction?.Invoke(new InvalidOperationException($"Path {subpath} is invalid."));
            return new NotFoundFileInfo(subpath);
        }
        var finalPath = _rootPath + normalizedPath;

        // Check if a file exists first (synchronous check for IFileProvider compatibility)
        var existsResult = _fileService.FileExistsAsync(_bucketName, finalPath).GetAwaiter().GetResult();
        if (existsResult is { IsSuccessful: true, Data: true })
            return new FileServiceFileInfo(_fileService, _bucketName, finalPath, errorMessageAction);

        errorMessageAction?.Invoke(new FileNotFoundException($"File with the path {subpath} does not exist."));
        return new NotFoundFileInfo(subpath);

    }

    /// <summary>
    /// Gets directory contents for the specified subpath.
    /// </summary>
    /// <param name="subpath">The relative path to the directory</param>
    /// <returns>Directory contents or NotFoundDirectoryContents if the directory doesn't exist</returns>
    public IDirectoryContents GetDirectoryContents(string subpath)
    {
        if (!InitializationCheck())
        {
            return NotFoundDirectoryContents.Singleton;
        }

        // Normalize the path for directory listing
        var normalizedPath = NormalizePath(subpath);
        var finalPath = _rootPath + normalizedPath;
        if (!finalPath.EndsWith('/')) finalPath += '/';

        // List files with the directory prefix
        // Fetch all pages to provide complete directory contents
        var allRelevantFileKeys = new List<string>();
        string? continuationToken = null;

        do
        {
            var listResult = _fileService.ListFilesAsync(_bucketName, new FileListOptions
            {
                Prefix = finalPath,
                ContinuationToken = continuationToken,
                MaxResults = 1000 // A reasonable page size
            }).GetAwaiter().GetResult();

            if (!listResult.IsSuccessful)
            {
                return NotFoundDirectoryContents.Singleton;
            }

            allRelevantFileKeys.AddRange(listResult.Data.FileKeys);
            continuationToken = listResult.Data.NextContinuationToken;

        } while (!string.IsNullOrEmpty(continuationToken));

        return new FileServiceDirectoryContents(
            _fileService,
            _bucketName,
            allRelevantFileKeys,
            finalPath,
            errorMessageAction);
    }

    /// <summary>
    /// Watches for changes to files matching the specified filter.
    /// Uses the file service's pub/sub notification system when available.
    /// </summary>
    /// <param name="filter">The filter pattern to watch</param>
    /// <returns>A change token that fires when matching files change</returns>
    public IChangeToken Watch(string filter)
    {
        if (!InitializationCheck(true))
        {
            return NullChangeToken.Singleton;
        }

        // Normalize the filter to a path prefix
        var normalizedFilter = NormalizePath(filter);
        var watchPath = _rootPath + normalizedFilter;

        lock (_tokensLock)
        {
            // Return an existing token if we're already watching this path
            if (_activeTokens.TryGetValue(watchPath, out var existingToken))
            {
                return existingToken;
            }

            // Create a new change token with notification setup
            var changeToken = new FileServiceChangeToken(() => RemoveToken(watchPath));
            _activeTokens[watchPath] = changeToken;

            // Set up notification
            var setupResult = SetupNotificationAsync(watchPath, changeToken).GetAwaiter().GetResult();
            if (setupResult) return changeToken;

            errorMessageAction?.Invoke(new Exception($"Failed to set up notification for path {watchPath}."));
            return NullChangeToken.Singleton;
        }
    }

    /// <summary>
    /// Disposes the file provider and cleans up active change tokens.
    /// </summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        lock (_tokensLock)
        {
            // Dispose of all active tokens
            foreach (var token in _activeTokens.Values)
            {
                token.Dispose();
            }
            _activeTokens.Clear();
        }

        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Removes a change token from the active tokens collection.
    /// </summary>
    /// <param name="pathPrefix">The path prefix of the token to remove</param>
    private void RemoveToken(string pathPrefix)
    {
        if (_disposed) return;

        lock (_tokensLock)
        {
            _activeTokens.Remove(pathPrefix);
        }
    }

    /// <summary>
    /// Normalizes a path by removing leading/trailing slashes and handling empty paths.
    /// </summary>
    /// <param name="path">The path to normalize</param>
    /// <returns>The normalized path</returns>
    private static string NormalizePath(string path)
    {
        return string.IsNullOrEmpty(path) ? string.Empty : path.Replace('\\', '/').Trim('/');
    }

    private async Task<bool> SetupNotificationAsync(string pathPrefix, FileServiceChangeToken changeToken)
    {
        var topicName = $"file-watch-{Guid.NewGuid():N}";

        var notificationResult = await _fileService.CreateNotificationAsync(
            _bucketName,
            topicName,
            pathPrefix,
            [Enums.FileNotificationEventType.Uploaded, Enums.FileNotificationEventType.Deleted],
            pubSubService.NotNull());
        if (!notificationResult.IsSuccessful)
        {
            errorMessageAction?.Invoke(new Exception($"Failed to create notification for path {pathPrefix}. Error: {notificationResult.ErrorMessage}"));
            return false;
        }

        var subscribeResult = await pubSubService.NotNull().SubscribeAsync(topicName,
            (_, _) =>
            {
                changeToken.NotifyChange();
                return Task.CompletedTask;
            },
            errorMessageAction);
        if (subscribeResult.IsSuccessful) return true;

        errorMessageAction?.Invoke(new Exception($"Failed to subscribe to notification for path {pathPrefix}. Error: {subscribeResult.ErrorMessage}"));
        return false;
    }

    private bool InitializationCheck(bool forPubSubAsWell = false)
    {
        if (_disposed)
        {
            errorMessageAction?.Invoke(new ObjectDisposedException("FileServiceFileProvider is disposed."));
            return false;
        }

        if (!_fileService.IsInitialized)
        {
            errorMessageAction?.Invoke(new InvalidOperationException("File service is not initialized."));
            return false;
        }

        if (!forPubSubAsWell || pubSubService is { IsInitialized: true }) return true;
        errorMessageAction?.Invoke(new InvalidOperationException("Pub/Sub service is not initialized."));
        return false;

    }

    /// <summary>
    /// File info implementation for file service files.
    /// </summary>
    private sealed class FileServiceFileInfo(
        IFileService fileService,
        string bucketName,
        string finalPath,
        Action<Exception>? errorMessageAction) : IFileInfo
    {
        private FileMetadata? _metadata;

        public bool Exists => true;
        public long Length => GetMetadata()?.Size ?? 0;
        public string? PhysicalPath => null;
        public string Name { get; } = Path.GetFileName(finalPath);
        public DateTimeOffset LastModified => GetMetadata()?.LastModified ?? DateTimeOffset.UtcNow;
        public bool IsDirectory => false;

        private FileMetadata? GetMetadata()
        {
            if (_metadata != null) return _metadata;

            if (!fileService.IsInitialized)
            {
                errorMessageAction?.Invoke(new InvalidOperationException("File service is not initialized."));
                return null;
            }

            var metadataResult = fileService.GetFileMetadataAsync(bucketName, finalPath).GetAwaiter().GetResult();
            if (!metadataResult.IsSuccessful)
            {
                errorMessageAction?.Invoke(new InvalidOperationException($"Failed to get metadata for file {finalPath}. Error: {metadataResult.ErrorMessage}"));
                return null;
            }
            _metadata = metadataResult.Data;
            return _metadata;
        }

        public Stream CreateReadStream()
        {
            if (!fileService.IsInitialized)
                throw new InvalidOperationException("File service is not initialized.");

            var metadata = GetMetadata();
            var ms = new MemoryTributary();

            var downloadResult = fileService.DownloadFileAsync(bucketName, finalPath, new StringOrStream(ms, metadata?.Size ?? 0)).GetAwaiter().GetResult();
            return !downloadResult.IsSuccessful
                ? throw new InvalidOperationException($"Failed to download file: {downloadResult.ErrorMessage}")
                : ms;
        }
    }

    /// <summary>
    /// Directory info implementation for subdirectories.
    /// </summary>
    private sealed class FileServiceDirectoryInfo(
        IFileService fileService,
        string bucketName,
        string directoryName,
        string createdByFileInDirectory,
        Action<Exception>? errorMessageAction) : IFileInfo
    {
        public bool Exists => true;
        public long Length => -1;
        public string? PhysicalPath => null;
        public string Name { get; } = directoryName;
        public DateTimeOffset LastModified => GetLastModified();
        public bool IsDirectory => true;

        public void AddFile(string finalPath) => _fileFullPaths.Add(finalPath);

        public Stream CreateReadStream()
        {
            throw new InvalidOperationException("Cannot create a read stream for a directory.");
        }

        private readonly HashSet<string> _fileFullPaths = [createdByFileInDirectory];

        private DateTimeOffset GetLastModified() =>
            _fileFullPaths.Max(f => GetMetadata(f)?.LastModified) ?? DateTimeOffset.UtcNow;

        private FileMetadata? GetMetadata(string finalPath)
        {
            var metadataResult = fileService.GetFileMetadataAsync(bucketName, finalPath).GetAwaiter().GetResult();
            if (metadataResult.IsSuccessful) return metadataResult.Data;

            errorMessageAction?.Invoke(new InvalidOperationException($"Warning: Failed to get metadata for file {finalPath}. Error: {metadataResult.ErrorMessage}"));
            return null;
        }
    }

    /// <summary>
    /// Directory contents implementation for file service directories.
    /// </summary>
    private sealed class FileServiceDirectoryContents : IDirectoryContents
    {
        private readonly IReadOnlyList<IFileInfo> _fileInfos;

        public FileServiceDirectoryContents(
            IFileService fileService,
            string bucketName,
            IReadOnlyList<string> fileKeys,
            string directoryFinalPath, // ends with '/'
            Action<Exception>? errorMessageAction)
        {
            var discoveredDirectories = new Dictionary<string, FileServiceDirectoryInfo>();

            var fileInfos = new List<IFileInfo>();

            foreach (var key in fileKeys)
            {
                var relativePath = key[directoryFinalPath.Length..];

                // Skip if this is the current directory itself
                if (string.IsNullOrEmpty(relativePath))
                    continue;

                var isASubSub = relativePath.Count(c => c == '/') > 1;
                if (isASubSub) continue;

                var slashIndex = relativePath.IndexOf('/');

                if (slashIndex == -1)
                {
                    // This is a direct file in the current directory
                    fileInfos.Add(new FileServiceFileInfo(
                        fileService,
                        bucketName,
                        key,
                        errorMessageAction));
                }
                else
                {
                    // This file is in a subdirectory - extract the immediate subdirectory name
                    var directoryName = relativePath[..slashIndex];
                    var directoryPath = directoryFinalPath + directoryName;
                    if (!discoveredDirectories.TryGetValue(directoryPath, out var dirInfo))
                    {
                        dirInfo = new FileServiceDirectoryInfo(
                            fileService,
                            bucketName,
                            directoryName,
                            key,
                            errorMessageAction);
                        discoveredDirectories[directoryPath] = dirInfo;
                        fileInfos.Add(dirInfo);
                    }
                    else
                    {
                        dirInfo.AddFile(key);
                    }
                    // Note: We don't add the file itself since it's not a direct child of the current directory
                }
            }

            _fileInfos = fileInfos;
            Exists = fileKeys.Count > 0;
        }

        public bool Exists { get; }

        public IEnumerator<IFileInfo> GetEnumerator() => _fileInfos.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>
    /// Change-token implementation that integrates with file service notifications.
    /// </summary>
    private sealed class FileServiceChangeToken(Action onDispose) : IChangeToken, IDisposable
    {
        private readonly CancellationTokenSource _cancellationTokenSource = new();
        private volatile bool _hasChanged;

        public bool HasChanged => _hasChanged;
        public bool ActiveChangeCallbacks => true;

        public IDisposable RegisterChangeCallback(Action<object?> callback, object? state)
        {
            if (_hasChanged)
            {
                callback(state);
                return EmptyDisposable.Instance;
            }

            var registration = _cancellationTokenSource.Token.Register(callback, state);

            // Check again in case a change happened during registration
            if (_hasChanged)
            {
                callback(state);
            }

            return registration;
        }

        public void NotifyChange()
        {
            if (_hasChanged) return;
            _hasChanged = true;
            _cancellationTokenSource.Cancel();
        }

        public void Dispose()
        {
            _cancellationTokenSource.Dispose();
            onDispose();
        }

        private sealed class EmptyDisposable : IDisposable
        {
            public static EmptyDisposable Instance => new();
            public void Dispose() { }
        }
    }
}
