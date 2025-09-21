// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Net;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Interfaces.Enums;
using CrossCloudKit.Interfaces.Records;
using CrossCloudKit.Utilities.Common;
using Newtonsoft.Json;

namespace CrossCloudKit.File.Common.MonitorBasedPubSub;

public class MonitorBasedPubSub: IAsyncDisposable
{
    public MonitorBasedPubSub(
        IFileService fileService,
        IMemoryService? memoryService = null,
        IPubSubService? pubSubService = null,
        Action<string>? errorMessageAction = null)
    {
        MemoryService = memoryService;
        _fileService = fileService;
        _pubSubService = pubSubService;
        _errorMessageAction = errorMessageAction;

        // Start background task
        _backgroundTask = Task.Run(async () => await RunBackgroundTaskAsync(_backgroundTaskCancellationTokenSource.Token));
    }

    private readonly IFileService _fileService;
    private readonly IPubSubService? _pubSubService;
    private readonly CancellationTokenSource _backgroundTaskCancellationTokenSource = new();
    private readonly Task? _backgroundTask;
    private readonly Action<string>? _errorMessageAction;
    private bool _disposed;

    public IMemoryService? MemoryService { get; }

    public async Task<OperationResult<string>> CreateNotificationAsync(
        string bucketName,
        string topicName,
        string pathPrefix,
        IReadOnlyList<FileNotificationEventType> eventTypes,
        IPubSubService pubSubService,
        CancellationToken cancellationToken = default)
    {
        if (_disposed)
            return OperationResult<string>.Failure("File service is not initialized.", HttpStatusCode.ServiceUnavailable);

        if (MemoryService is not { IsInitialized: true })
            return OperationResult<string>.Failure("Memory service is not initialized.", HttpStatusCode.ServiceUnavailable);

        var sortedEventTypes = eventTypes.OrderBy(e => e).Select(s => s.ToString()).ToList();

        var valueCompiled = JsonConvert.SerializeObject(new EventNotificationConfig
        {
            TopicName = topicName,
            BucketName = bucketName,
            PathPrefix = pathPrefix,
            EventTypes = sortedEventTypes
        }, Formatting.None);

        var newNotification = await MemoryService.PushToListTailIfValuesNotExistsAsync(
            SystemClassMemoryScopeInstance,
            EventNotificationConfigsListName,
            new List<PrimitiveType> { valueCompiled },
            false,
            cancellationToken);

        if (!newNotification.IsSuccessful)
            return OperationResult<string>.Failure(newNotification.ErrorMessage, newNotification.StatusCode);

        if (newNotification.Data.Length == 0) //Notification already exists
            return OperationResult<string>.Success(topicName);

        var markResult = await pubSubService.MarkUsedOnBucketEvent(topicName, cancellationToken);
        return !markResult.IsSuccessful ? OperationResult<string>.Failure(markResult.ErrorMessage, markResult.StatusCode) : OperationResult<string>.Success(topicName);
    }

    public async Task<OperationResult<int>> DeleteNotificationsAsync(
        IPubSubService pubSubService,
        string bucketName,
        string? topicName = null,
        CancellationToken cancellationToken = default)
    {
        if (_disposed)
            return OperationResult<int>.Failure("File service is not initialized.", HttpStatusCode.ServiceUnavailable);

        if (MemoryService is not { IsInitialized: true })
            return OperationResult<int>.Failure("Memory service is not initialized.", HttpStatusCode.ServiceUnavailable);

        var eventListenConfigsResult = await MemoryService.GetAllElementsOfListAsync(
            SystemClassMemoryScopeInstance,
            EventNotificationConfigsListName,
            cancellationToken);
        if (!eventListenConfigsResult.IsSuccessful)
            return OperationResult<int>.Failure(eventListenConfigsResult.ErrorMessage, eventListenConfigsResult.StatusCode);

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

        var removeResult = await MemoryService.RemoveElementsFromListAsync(
            SystemClassMemoryScopeInstance,
            EventNotificationConfigsListName,
            deleteConfigs.Union(malformedConfigs),
            false,
            cancellationToken);
        if (!removeResult.IsSuccessful)
            return OperationResult<int>.Failure(removeResult.ErrorMessage, removeResult.StatusCode);

        foreach (var deleteConfig in deleteConfigsParsed)
        {
            var unmarkResult = await pubSubService.UnmarkUsedOnBucketEvent(deleteConfig.TopicName, cancellationToken);
            if (!unmarkResult.IsSuccessful)
                return OperationResult<int>.Failure(unmarkResult.ErrorMessage, unmarkResult.StatusCode);
        }

        return OperationResult<int>.Success(deleteConfigs.Count);
    }

    public async Task<OperationResult<bool>> CleanupBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        if (MemoryService is not { IsInitialized: true })
            return OperationResult<bool>.Success(true); //Intentional

        //Lock mutex for this operation
        await using var mutex = await ObserveFileServiceAndDispatchEventsMutex(cancellationToken);

        await MemoryService.EmptyListAsync(SystemClassMemoryScopeInstance, GetFileStateBucketListName(bucketName), false, cancellationToken);

        return OperationResult<bool>.Success(true);
    }

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
        if (MemoryService == null)
            throw new InvalidOperationException("Memory service is not initialized.");
        if (_pubSubService == null)
            throw new InvalidOperationException("Pub/Sub service is not initialized.");

        //Lock mutex for this operation
        await using var mutex = await ObserveFileServiceAndDispatchEventsMutex(cancellationToken);

        var elements = await MemoryService.GetAllElementsOfListAsync(
            SystemClassMemoryScopeInstance,
            EventNotificationConfigsListName,
            cancellationToken);

        if (!elements.IsSuccessful || elements.Data == null)
            throw new InvalidOperationException($"GetAllElementsOfListAsync failed with: {elements.ErrorMessage}");

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
                var listResult = await _fileService.ListFilesAsync(bucketName, new FileListOptions
                {
                    ContinuationToken = continuationToken
                }, cancellationToken);

                if (!listResult.IsSuccessful || listResult.Data == null)
                {
                    throw new InvalidOperationException($"ListFilesAsync failed with: {listResult.ErrorMessage}");
                }

                allFileKeys.AddRange(listResult.Data.FileKeys);
                continuationToken = listResult.Data.NextContinuationToken;
            }
            while (!string.IsNullOrEmpty(continuationToken));

            // Get previous file states from memory service using list storage
            var bucketFileStatesListName = GetFileStateBucketListName(bucketName);
            var previousFileStatesResult = await MemoryService.GetAllElementsOfListAsync(
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
                    var metadataResult = await _fileService.GetFileMetadataAsync(bucketName, fileKey, cancellationToken);
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
                                                     !AreFileStatesEqual(previousFileStates[fileKey], currentState, false);

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

                var removeResult = await MemoryService.RemoveElementsFromListAsync(
                    SystemClassMemoryScopeInstance,
                    bucketFileStatesListName,
                    elementsToRemove,
                    false,
                    cancellationToken);
                if (!removeResult.IsSuccessful)
                    throw new InvalidOperationException($"RemoveElementsFromListAsync failed with: {removeResult.ErrorMessage}");
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
                var removeOldResult = await MemoryService.RemoveElementsFromListAsync(
                    SystemClassMemoryScopeInstance,
                    bucketFileStatesListName,
                    oldElementsToRemove,
                    false,
                    cancellationToken);
                if (!removeOldResult.IsSuccessful)
                    throw new InvalidOperationException($"RemoveElementsFromListAsync failed with: {removeOldResult.ErrorMessage}");
            }

            var addResult = await MemoryService.PushToListTailAsync(
                SystemClassMemoryScopeInstance,
                bucketFileStatesListName,
                newElementsToAdd,
                false,
                false,
                cancellationToken);
            if (!addResult.IsSuccessful)
                throw new InvalidOperationException($"PushToListTailAsync failed with: {addResult.ErrorMessage}");
        }

        if (malformedConfigs.Count > 0)
        {
            await MemoryService.RemoveElementsFromListAsync(
                SystemClassMemoryScopeInstance,
                EventNotificationConfigsListName,
                malformedConfigs,
                false,
                cancellationToken);
        }
    }

    private static string GetFileStateBucketListName(string bucketName) => $"file_states_{bucketName}";

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

    private static bool AreFileStatesEqual(FileState state1, FileState state2, bool checkExist = true)
    {
        var isEqual = state1.LastModified == state2.LastModified &&
                      state1.Size == state2.Size &&
                      (!checkExist || state1.Exists == state2.Exists);
        return isEqual;
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

    private static readonly MemoryScopeLambda ObserveFileServiceAndDispatchEventsMemoryServiceScope =
        new(
            "CrossCloudKit.File.Common.MonitorBasedPubSub.ObserveFileServiceAndDispatchEvents");
    private async Task<IAsyncDisposable> ObserveFileServiceAndDispatchEventsMutex(CancellationToken cancellationToken)
    {
        if (MemoryService == null) return new NoopAsyncDisposable();
        return await MemoryScopeMutex.CreateScopeAsync(
            MemoryService,
            ObserveFileServiceAndDispatchEventsMemoryServiceScope,
            "lock",
            TimeSpan.FromMinutes(5),
            cancellationToken);
    }

    private static readonly MemoryScopeLambda SystemClassMemoryScopeInstance =
        new(
            "CrossCloudKit.File.Common.MonitorBasedPubSub.FileService");
    public static Task<IAsyncDisposable> CreateNoopAsyncDisposableAsync()
    {
        return Task.FromResult<IAsyncDisposable>(new NoopAsyncDisposable());
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            // Cancel the background task
            await _backgroundTaskCancellationTokenSource.CancelAsync();

            // Wait for the background task to complete (with timeout)
            await _backgroundTask.NotNull().WaitAsync(TimeSpan.FromSeconds(5));
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

            GC.SuppressFinalize(this);
        }
    }
}

public sealed class NoopAsyncDisposable : IAsyncDisposable
{
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
