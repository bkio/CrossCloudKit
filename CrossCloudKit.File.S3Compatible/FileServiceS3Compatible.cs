// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;
using Amazon;
using Amazon.S3;
using Amazon.S3.Transfer;
using CrossCloudKit.File.AWS;
using CrossCloudKit.File.Common.MonitorBasedPubSub;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Interfaces.Enums;
using CrossCloudKit.Utilities.Common;

namespace CrossCloudKit.File.S3Compatible;

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

            _monitorBasedPubSub = new MonitorBasedPubSub(this, memoryService, pubSubService, errorMessageAction);
        }
        catch
        {
            IsInitialized = false;
        }
    }

    private readonly MonitorBasedPubSub? _monitorBasedPubSub;
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
            return OperationResult<string>.Failure("File service is not initialized.", HttpStatusCode.ServiceUnavailable);

        return await _monitorBasedPubSub.NotNull().CreateNotificationAsync(
            bucketName,
            topicName,
            pathPrefix,
            eventTypes,
            pubSubService,
            cancellationToken);
    }

    /// <inheritdoc />
    public override async Task<OperationResult<int>> DeleteNotificationsAsync(
        IPubSubService pubSubService,
        string bucketName,
        string? topicName = null,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || _disposed)
            return OperationResult<int>.Failure("File service is not initialized.", HttpStatusCode.ServiceUnavailable);

        return await _monitorBasedPubSub.NotNull().DeleteNotificationsAsync(
            pubSubService,
            bucketName,
            topicName,
            cancellationToken);
    }

    /// <inheritdoc />
    public override async Task<OperationResult<bool>> CleanupBucketAsync(string bucketName,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized || _disposed)
            return OperationResult<bool>.Failure("File service is not initialized.", HttpStatusCode.ServiceUnavailable);

        var baseSuccess = await base.CleanupBucketAsync(bucketName, cancellationToken);

        var result = await _monitorBasedPubSub.NotNull().CleanupBucketAsync(
            bucketName,
            cancellationToken);
        return !result.IsSuccessful ? baseSuccess : result;
    }

    /// <summary>
    /// Disposes the FileServiceS3Compatible instance and stops the background task
    /// </summary>
    public override async ValueTask DisposeAsync()
    {
        try
        {
            await _monitorBasedPubSub.NotNull().DisposeAsync();
        }
        catch (Exception)
        {
            // ignored
        }

        _disposed = true;

        await base.DisposeAsync();

        GC.SuppressFinalize(this);
    }
}
