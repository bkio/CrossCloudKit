// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using Amazon;
using Amazon.S3;
using Amazon.S3.Transfer;
using Cloud.File.AWS;
using Cloud.Interfaces;
using Newtonsoft.Json;
using Utilities.Common;

namespace Cloud.File.S3Compatible;

public class FileServiceS3Compatible : FileServiceAWS
{
    /// <summary>
    /// Initializes a new instance of the FileServiceAWS class for S3-compatible storage (e.g., MinIO)
    /// </summary>
    /// <param name="serverAddress">Server address</param>
    /// <param name="accessKey">Access key</param>
    /// <param name="secretKey">Secret key</param>
    /// <param name="region">Region</param>
    /// <param name="memoryService">Memory service is needed for notifications to work</param>
    /// <param name="pubSubService">Pub/Sub service is needed for notifications to work</param>
    /// <param name="errorMessageAction">Error messages of the background task will be pushed here</param>
    public FileServiceS3Compatible(
        string serverAddress,
        string accessKey,
        string secretKey,
        string region,
        IMemoryService? memoryService = null,
        IPubSubService? pubSubService = null,
        Action<string>? errorMessageAction = null)
    {
        try
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(serverAddress);
            ArgumentException.ThrowIfNullOrWhiteSpace(accessKey);
            ArgumentException.ThrowIfNullOrWhiteSpace(secretKey);
            ArgumentException.ThrowIfNullOrWhiteSpace(region);

            _memoryService = memoryService;
            _pubSubService = pubSubService;
            _errorMessageAction = errorMessageAction;

            AWSCredentials = new Amazon.Runtime.BasicAWSCredentials(accessKey, secretKey);
            RegionEndpoint = RegionEndpoint.GetBySystemName(region);

            var clientConfig = new AmazonS3Config
            {
                AuthenticationRegion = region,
                ServiceURL = serverAddress,
                ForcePathStyle = true
            };

            S3Client = new AmazonS3Client(AWSCredentials, clientConfig);

            var transferUtilConfig = new TransferUtilityConfig
            {
                ConcurrentServiceRequests = 10,
            };
            TransferUtil = new TransferUtility(S3Client, transferUtilConfig);

            IsInitialized = true;

            // Start background task
            _backgroundTask = Task.Run(async () => await RunBackgroundTaskAsync(_backgroundTaskCancellationTokenSource.Token));
        }
        catch
        {
            IsInitialized = false;
        }
    }

    private readonly IMemoryService? _memoryService;
    private readonly IPubSubService? _pubSubService;
    private readonly CancellationTokenSource _backgroundTaskCancellationTokenSource = new();
    private readonly Task? _backgroundTask;
    private readonly Action<string>? _errorMessageAction;
    private bool _disposed;

    /// <inheritdoc />
    public override async Task<OperationResult<string>> CreateNotificationAsync(
        string bucketName,
        string topicName,
        string pathPrefix,
        IReadOnlyList<FileNotificationEventType> eventTypes,
        IPubSubService pubSubService,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || _disposed)
            return OperationResult<string>.Failure("File service is not initialized.");

        if (_memoryService is not { IsInitialized: true })
            return OperationResult<string>.Failure("Memory service is not initialized.");

        var sortedEventTypes = eventTypes.OrderBy(e => e).Select(s => s.ToString()).ToList();

        var valueCompiled = JsonConvert.SerializeObject(new EventNotificationConfig
        {
            TopicName = topicName,
            BucketName = bucketName,
            PathPrefix = pathPrefix,
            EventTypes = sortedEventTypes
        }, Formatting.None);

        var newNotification = await _memoryService.PushToListTailIfValuesNotExistsAsync(
            SystemClassMemoryScopeInstance,
            EventNotificationConfigsListName,
            new List<PrimitiveType> { valueCompiled },
            false,
            cancellationToken);

        if (!newNotification.IsSuccessful || newNotification.Data is null)
            return OperationResult<string>.Failure(newNotification.ErrorMessage!);

        if (newNotification.Data.Length == 0) //Notification already exists
            return OperationResult<string>.Success(topicName);

        var markResult = await pubSubService.MarkUsedOnBucketEvent(topicName, cancellationToken);
        return !markResult.IsSuccessful ? OperationResult<string>.Failure(markResult.ErrorMessage!) : OperationResult<string>.Success(topicName);
    }

    /// <inheritdoc />
    public override async Task<OperationResult<int>> DeleteNotificationsAsync(
        IPubSubService pubSubService,
        string bucketName,
        string? topicName = null,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || _disposed)
            return OperationResult<int>.Failure("File service is not initialized.");

        if (_memoryService is not { IsInitialized: true })
            return OperationResult<int>.Failure("Memory service is not initialized.");

        var eventListenConfigsResult = await _memoryService.GetAllElementsOfListAsync(
            SystemClassMemoryScopeInstance,
            EventNotificationConfigsListName,
            cancellationToken);
        if (!eventListenConfigsResult.IsSuccessful || eventListenConfigsResult.Data is null)
            return OperationResult<int>.Failure(eventListenConfigsResult.ErrorMessage!);

        var malformedConfigs = new List<PrimitiveType>();
        var deleteConfigs = new List<PrimitiveType>();
        var deleteConfigsParsed = new List<EventNotificationConfig>();

        foreach (var eventListenConfigPt in eventListenConfigsResult.Data)
        {
            var eventListenConfig = JsonConvert.DeserializeObject<EventNotificationConfig>(eventListenConfigPt.AsString);
            if (eventListenConfig == null)
            {
                malformedConfigs.Add(eventListenConfigPt);
                continue;
            }
            if (eventListenConfig.BucketName != bucketName)
                continue;

            if (topicName != null && eventListenConfig.TopicName != topicName)
                continue;

            deleteConfigs.Add(eventListenConfigPt);
            deleteConfigsParsed.Add(eventListenConfig);
        }

        var removeResult = await _memoryService.RemoveElementsFromListAsync(
            SystemClassMemoryScopeInstance,
            EventNotificationConfigsListName,
            deleteConfigs.Union(malformedConfigs),
            false,
            cancellationToken);
        if (!removeResult.IsSuccessful)
            return OperationResult<int>.Failure(removeResult.ErrorMessage!);

        foreach (var deleteConfig in deleteConfigsParsed)
        {
            var unmarkResult = await pubSubService.UnmarkUsedOnBucketEvent(deleteConfig.TopicName, cancellationToken);
            if (!unmarkResult.IsSuccessful)
                return OperationResult<int>.Failure(unmarkResult.ErrorMessage!);
        }

        return OperationResult<int>.Success(deleteConfigs.Count);
    }

    private class SystemClassMemoryScope : IMemoryServiceScope
    {
        public string Compile() => "Cloud.File.S3Compatible.FileServiceS3Compatible";
    }
    private static readonly SystemClassMemoryScope SystemClassMemoryScopeInstance = new();

    private class EventNotificationConfig
    {
        public string TopicName = "";
        public string BucketName = "";
        public string PathPrefix = "";
        public List<string> EventTypes = [];
    }
    private const string EventNotificationConfigsListName = "notification_events";

    private async Task ObserveFileServiceAndDispatchEvents(CancellationToken cancellationToken = default)
    {
        if (_memoryService == null)
            throw new InvalidOperationException("Memory service is not initialized.");
        if (_pubSubService == null)
            throw new InvalidOperationException("Pub/Sub service is not initialized.");

        //Lock mutex for this operation
        await using var _ = await MemoryServiceScopeMutex.CreateScopeAsync(
            _memoryService,
            new LambdaMemoryServiceScope("Cloud.File.S3Compatible.FileServiceS3Compatible.ObserveFileServiceAndDispatchEvents"),
            "lock",
            TimeSpan.FromMinutes(5),
            cancellationToken);

        var elements = await _memoryService.GetAllElementsOfListAsync(
            SystemClassMemoryScopeInstance,
            EventNotificationConfigsListName,
            cancellationToken);

        if (!elements.IsSuccessful || elements.Data == null)
            throw new InvalidOperationException($"GetAllElementsOfListAsync failed with: {elements.ErrorMessage!}");

        var malformedConfigs = new List<PrimitiveType>();
        var bucketNameToPathPrefixToConfigs = new Dictionary<string, Dictionary<string, List<EventNotificationConfig>>>();

        foreach (var element in elements.Data)
        {
            var eventNotificationConfig = JsonConvert.DeserializeObject<EventNotificationConfig>(element.AsString);
            if (eventNotificationConfig == null)
            {
                malformedConfigs.Add(element);
                continue;
            }

            if (!bucketNameToPathPrefixToConfigs.ContainsKey(eventNotificationConfig.BucketName))
                bucketNameToPathPrefixToConfigs.Add(eventNotificationConfig.BucketName, []);
            if (!bucketNameToPathPrefixToConfigs[eventNotificationConfig.BucketName].ContainsKey(eventNotificationConfig.PathPrefix))
                bucketNameToPathPrefixToConfigs[eventNotificationConfig.BucketName].Add(eventNotificationConfig.PathPrefix, []);

            bucketNameToPathPrefixToConfigs[eventNotificationConfig.BucketName][eventNotificationConfig.PathPrefix].Add(eventNotificationConfig);
        }

        foreach (var (bucketName, pathPrefixToConfigs) in bucketNameToPathPrefixToConfigs)
        {
            // Get all files in the bucket (once per bucket, not per path prefix)
            var allFileKeys = new List<string>();
            string? continuationToken = null;
            do
            {
                var listResult = await ListFilesAsync(bucketName, new ListFilesOptions()
                {
                    ContinuationToken = continuationToken
                }, cancellationToken);

                if (!listResult.IsSuccessful || listResult.Data == null)
                {
                    throw new InvalidOperationException($"ListFilesAsync failed with: {listResult.ErrorMessage!}");
                }

                allFileKeys.AddRange(listResult.Data.FileKeys);
                continuationToken = listResult.Data.NextContinuationToken;
            }
            while (!string.IsNullOrEmpty(continuationToken));

            // Get previous file states from memory service using list storage
            var bucketFileStatesListName = $"file_states_{bucketName}";
            var previousFileStatesResult = await _memoryService.GetAllElementsOfListAsync(
                SystemClassMemoryScopeInstance,
                bucketFileStatesListName,
                cancellationToken);

            var previousFileStates = new Dictionary<string, FileState>();
            if (previousFileStatesResult is { IsSuccessful: true, Data: not null })
            {
                foreach (var element in previousFileStatesResult.Data)
                {
                    var fileStateEntry = JsonConvert.DeserializeObject<FileStateEntry>(element.AsString);
                    if (fileStateEntry != null)
                    {
                        previousFileStates[fileStateEntry.FileKey] = fileStateEntry.State;
                    }
                }
            }

            var currentFileStates = new ConcurrentDictionary<string, FileState>();

            // Get current file states
            var getMetadataTasks = allFileKeys.Select(fileKey => Task.Run(async () =>
            {
                var metadataResult = await GetFileMetadataAsync(bucketName, fileKey, cancellationToken);
                if (metadataResult is { IsSuccessful: true, Data: not null })
                {
                    currentFileStates[fileKey] = new FileState { LastModified = metadataResult.Data.LastModified ?? DateTimeOffset.UtcNow, Size = metadataResult.Data.Size, Exists = true };
                }
            }, cancellationToken))
            .ToList();

            await Task.WhenAll(getMetadataTasks);

            // Detect changes and publish events for each path prefix configuration
            foreach (var (pathPrefix, configs) in pathPrefixToConfigs)
            {
                foreach (var config in configs)
                {
                    foreach (var eventType in config.EventTypes)
                    {
                        if (!Enum.TryParse(eventType, out FileNotificationEventType eventTypeEnum))
                            continue;

                        switch (eventTypeEnum)
                        {
                            case FileNotificationEventType.Uploaded:
                                // Check for new or modified files matching the path prefix
                                foreach (var (fileKey, currentState) in currentFileStates)
                                {
                                    if (!fileKey.StartsWith(pathPrefix)) continue;

                                    var isNewFile = !previousFileStates.ContainsKey(fileKey);
                                    var isModified = !isNewFile &&
                                                   (previousFileStates[fileKey].LastModified != currentState.LastModified ||
                                                    previousFileStates[fileKey].Size != currentState.Size);

                                    if (!isNewFile && !isModified) continue;
                                    var message = JsonConvert.SerializeObject(new
                                    {
                                        bucket = bucketName,
                                        key = fileKey,
                                        eventType = "Uploaded",
                                        timestamp = DateTimeOffset.UtcNow.ToString("O"),
                                        size = currentState.Size,
                                        lastModified = currentState.LastModified.ToString("O")
                                    });

                                    await _pubSubService.PublishAsync(config.TopicName, message, cancellationToken);
                                }
                                break;

                            case FileNotificationEventType.Deleted:
                                // Check for deleted files matching the path prefix
                                foreach (var (fileKey, previousState) in previousFileStates)
                                {
                                    if (!fileKey.StartsWith(pathPrefix)) continue;

                                    if (!previousState.Exists || currentFileStates.ContainsKey(fileKey)) continue;

                                    var message = JsonConvert.SerializeObject(new
                                    {
                                        bucket = bucketName,
                                        key = fileKey,
                                        eventType = "Deleted",
                                        timestamp = DateTimeOffset.UtcNow.ToString("O")
                                    });

                                    await _pubSubService.PublishAsync(config.TopicName, message, cancellationToken);
                                }
                                break;
                        }
                    }
                }
            }

            // Update stored file states for the entire bucket (once per bucket)
            // Use a more efficient update method: only update changed/new files and remove deleted files
            var filesToRemove = previousFileStates.Keys
                .Where(key => !currentFileStates.ContainsKey(key))
                .ToList();

            var filesToUpdate = currentFileStates
                .Where(kvp => !previousFileStates.ContainsKey(kvp.Key) ||
                             !AreFileStatesEqual(previousFileStates[kvp.Key], kvp.Value))
                .ToList();

            // Remove deleted files from storage
            if (filesToRemove.Count > 0)
            {
                var elementsToRemove = filesToRemove.Select(fileKey => new FileStateEntry { FileKey = fileKey, State = previousFileStates[fileKey] }).Select(fileStateEntry => JsonConvert.SerializeObject(fileStateEntry, Formatting.None)).Select(dummy => (PrimitiveType)dummy).ToList();

                var removeResult = await _memoryService.RemoveElementsFromListAsync(
                    SystemClassMemoryScopeInstance,
                    bucketFileStatesListName,
                    elementsToRemove,
                    false,
                    cancellationToken);
                if (!removeResult.IsSuccessful)
                    throw new InvalidOperationException($"RemoveElementsFromListAsync failed with: {removeResult.ErrorMessage!}");
            }

            // Add/update changed files section
            //
            if (filesToUpdate.Count <= 0) continue;

            // For updates, we need to remove old entries first, then add new ones
            var oldElementsToRemove = new List<PrimitiveType>();
            var newElementsToAdd = new List<PrimitiveType>();

            foreach (var (fileKey, newState) in filesToUpdate)
            {
                if (previousFileStates.TryGetValue(fileKey, out var state))
                {
                    var oldFileStateEntry = new FileStateEntry { FileKey = fileKey, State = state };
                    oldElementsToRemove.Add(JsonConvert.SerializeObject(oldFileStateEntry, Formatting.None));
                }

                var newFileStateEntry = new FileStateEntry { FileKey = fileKey, State = newState };
                newElementsToAdd.Add(JsonConvert.SerializeObject(newFileStateEntry, Formatting.None));
            }

            if (oldElementsToRemove.Count > 0)
            {
                var removeOldResult = await _memoryService.RemoveElementsFromListAsync(
                    SystemClassMemoryScopeInstance,
                    bucketFileStatesListName,
                    oldElementsToRemove,
                    false,
                    cancellationToken);
                if (!removeOldResult.IsSuccessful)
                    throw new InvalidOperationException($"RemoveElementsFromListAsync failed with: {removeOldResult.ErrorMessage!}");
            }

            var addResult = await _memoryService.PushToListTailAsync(
                SystemClassMemoryScopeInstance,
                bucketFileStatesListName,
                newElementsToAdd,
                false,
                false,
                cancellationToken);
            if (!addResult.IsSuccessful)
                throw new InvalidOperationException($"PushToListTailAsync failed with: {addResult.ErrorMessage!}");
        }

        if (malformedConfigs.Count > 0)
        {
            await _memoryService.RemoveElementsFromListAsync(
                SystemClassMemoryScopeInstance,
                EventNotificationConfigsListName,
                malformedConfigs,
                false,
                cancellationToken);
        }
    }

    private class FileState
    {
        public DateTimeOffset LastModified { get; init; }
        public long Size { get; init; }
        public bool Exists { get; init; }
    }

    private class FileStateEntry
    {
        public string FileKey = "";
        public FileState State = new();
    }

    private static bool AreFileStatesEqual(FileState state1, FileState state2)
    {
        return state1.LastModified == state2.LastModified &&
               state1.Size == state2.Size &&
               state1.Exists == state2.Exists;
    }

    /// <summary>
    /// Background task that runs continuously while the service instance is alive
    /// </summary>
    private async Task RunBackgroundTaskAsync(CancellationToken cancellationToken, int errorRetryCount = 0)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(3), cancellationToken);

                await ObserveFileServiceAndDispatchEvents(cancellationToken);
                errorRetryCount = 0; //Reset
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when cancellation is requested
        }
        catch (Exception e)
        {
            _errorMessageAction?.Invoke($"Background task error: {e.Message}");

            if (errorRetryCount == 10)
            {
                _errorMessageAction?.Invoke($"Background task has been terminated after {errorRetryCount} times of errors occurred.");
                return;
            }
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            await RunBackgroundTaskAsync(cancellationToken, errorRetryCount + 1);
        }
    }

    /// <summary>
    /// Disposes the FileServiceS3Compatible instance and stops the background task
    /// </summary>
    public override async ValueTask DisposeAsync()
    {
        try
        {
            // Cancel the background task
            await _backgroundTaskCancellationTokenSource.CancelAsync();

            // Wait for the background task to complete (with timeout)
            await _backgroundTask?.WaitAsync(TimeSpan.FromSeconds(5))!;
        }
        catch
        {
            // Ignore exceptions during disposal
        }
        finally
        {
            try
            {
                _backgroundTaskCancellationTokenSource.Dispose();
            }
            catch (Exception)
            {
                // ignored
            }

            _disposed = true;

            await base.DisposeAsync().ConfigureAwait(false);

            GC.SuppressFinalize(this);
        }
    }
}
