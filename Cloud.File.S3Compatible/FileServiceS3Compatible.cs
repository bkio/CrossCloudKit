// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Amazon;
using Amazon.S3;
using Amazon.S3.Transfer;
using Cloud.File.AWS;

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
    public FileServiceS3Compatible(string serverAddress, string accessKey, string secretKey, string region)
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
}
