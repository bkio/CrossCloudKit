// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces;
using CrossCloudKit.Utilities.Common;
using FluentAssertions;
using xRetry;
using Xunit.Abstractions;

namespace CrossCloudKit.File.Tests.Common;

public abstract class FileServiceTestBase(ITestOutputHelper testOutputHelper)
{
    protected abstract IFileService CreateFileService();
    protected abstract IPubSubService CreatePubSubService();
    protected abstract string GetTestBucketName();

    protected virtual string GenerateRandomKey(string prefix = "test-file-")
        => $"{prefix}{Guid.NewGuid():N}";

    private static StringOrStream ContentStream(string content)
    {
        return new StringOrStream(
            new MemoryTributary(
            System.Text.Encoding.UTF8.GetBytes(content)),
            System.Text.Encoding.UTF8.GetByteCount(content));
    }

    [RetryFact(3, 5000)]
    public virtual async Task UploadFileAsync_ShouldUploadFile()
    {
        var service = CreateFileService();
        var bucket = GetTestBucketName();
        try
        {
            var key = GenerateRandomKey();
            const string content = "Hello, cloud!";
            await using var sos = ContentStream(content);

            var result = await service.UploadFileAsync(sos, bucket, key);
            result.IsSuccessful.Should().BeTrue(result.ErrorMessage);
            result.Data.Should().NotBeNull(result.ErrorMessage);
            result.Data!.Size.Should().Be(content.Length);
        }
        finally
        {
            await service.CleanupBucketAsync(bucket);
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task DownloadFileAsync_ShouldDownloadFile()
    {
        var service = CreateFileService();
        var bucket = GetTestBucketName();
        try
        {
            var key = GenerateRandomKey();
            const string content = "Download test content";
            await using var sos = ContentStream(content);
            var uploadResult = await service.UploadFileAsync(sos, bucket, key);
            uploadResult.IsSuccessful.Should().BeTrue(uploadResult.ErrorMessage);

            using var ms = new MemoryStream();
            var dest = new StringOrStream(ms, 0);
            var result = await service.DownloadFileAsync(bucket, key, dest);
            result.IsSuccessful.Should().BeTrue(result.ErrorMessage);
            ms.Position = 0;
            using var reader = new StreamReader(ms, System.Text.Encoding.UTF8);
            var downloaded = await reader.ReadToEndAsync();
            downloaded.Should().Be(content);
        }
        finally
        {
            await service.CleanupBucketAsync(bucket);
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task CopyFileAsync_ShouldCopyFile()
    {
        var service = CreateFileService();
        var bucket = GetTestBucketName();
        try
        {
            var srcKey = GenerateRandomKey();
            var dstKey = GenerateRandomKey("copy-");
            var content = "Copy test content";
            await using var sos = ContentStream(content);
            var uploadResult = await service.UploadFileAsync(sos, bucket, srcKey);
            uploadResult.IsSuccessful.Should().BeTrue(uploadResult.ErrorMessage);

            var result = await service.CopyFileAsync(bucket, srcKey, bucket, dstKey);
            result.IsSuccessful.Should().BeTrue(result.ErrorMessage);
            result.Data.Should().NotBeNull(result.ErrorMessage);
            (await service.FileExistsAsync(bucket, dstKey)).Data.Should().BeTrue();
        }
        finally
        {
            await service.CleanupBucketAsync(bucket);
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task DeleteFileAsync_ShouldDeleteFile()
    {
        var service = CreateFileService();
        var bucket = GetTestBucketName();
        try
        {
            var key = GenerateRandomKey();
            await using var sos = ContentStream("Delete me");
            var uploadResult = await service.UploadFileAsync(sos, bucket, key);
            uploadResult.IsSuccessful.Should().BeTrue(uploadResult.ErrorMessage);
            var delResult = await service.DeleteFileAsync(bucket, key);
            delResult.IsSuccessful.Should().BeTrue(delResult.ErrorMessage);
            (await service.FileExistsAsync(bucket, key)).Data.Should().BeFalse();
        }
        finally
        {
            await service.CleanupBucketAsync(bucket);
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task DeleteFolderAsync_ShouldDeleteAllFilesInFolder()
    {
        var service = CreateFileService();
        var bucket = GetTestBucketName();
        try
        {
            var prefix = GenerateRandomKey("folder/");
            var key1 = prefix + "/file1.txt";
            var key2 = prefix + "/file2.txt";
            var upload1 = await service.UploadFileAsync(ContentStream("A"), bucket, key1);
            upload1.IsSuccessful.Should().BeTrue(upload1.ErrorMessage);
            var upload2 = await service.UploadFileAsync(ContentStream("B"), bucket, key2);
            upload2.IsSuccessful.Should().BeTrue(upload2.ErrorMessage);
            var delResult = await service.DeleteFolderAsync(bucket, prefix);
            delResult.IsSuccessful.Should().BeTrue(delResult.ErrorMessage);
            (await service.FileExistsAsync(bucket, key1)).Data.Should().BeFalse();
            (await service.FileExistsAsync(bucket, key2)).Data.Should().BeFalse();
        }
        finally
        {
            await service.CleanupBucketAsync(bucket);
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task FileExistsAsync_ShouldReturnCorrectExistence()
    {
        var service = CreateFileService();
        var bucket = GetTestBucketName();
        try
        {
            var key = GenerateRandomKey();
            (await service.FileExistsAsync(bucket, key)).Data.Should().BeFalse();
            var uploadResult = await service.UploadFileAsync(ContentStream("exists"), bucket, key);
            uploadResult.IsSuccessful.Should().BeTrue(uploadResult.ErrorMessage);
            (await service.FileExistsAsync(bucket, key)).Data.Should().BeTrue();
        }
        finally
        {
            await service.CleanupBucketAsync(bucket);
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task GetFileSizeAsync_ShouldReturnFileSize()
    {
        var service = CreateFileService();
        var bucket = GetTestBucketName();
        try
        {
            var key = GenerateRandomKey();
            const string content = "1234567890";
            var uploadResult = await service.UploadFileAsync(ContentStream(content), bucket, key);
            uploadResult.IsSuccessful.Should().BeTrue(uploadResult.ErrorMessage);
            var result = await service.GetFileSizeAsync(bucket, key);
            result.IsSuccessful.Should().BeTrue(result.ErrorMessage);
            result.Data.Should().Be(content.Length);
        }
        finally
        {
            await service.CleanupBucketAsync(bucket);
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task GetFileChecksumAsync_ShouldReturnChecksum()
    {
        var service = CreateFileService();
        var bucket = GetTestBucketName();
        try
        {
            var key = GenerateRandomKey();
            const string content = "checksum-test";
            var uploadResult = await service.UploadFileAsync(ContentStream(content), bucket, key);
            uploadResult.IsSuccessful.Should().BeTrue(uploadResult.ErrorMessage);
            var result = await service.GetFileChecksumAsync(bucket, key);
            result.IsSuccessful.Should().BeTrue(result.ErrorMessage);
            result.Data.Should().NotBeNullOrEmpty(result.ErrorMessage);
        }
        finally
        {
            await service.CleanupBucketAsync(bucket);
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task GetFileMetadataAsync_ShouldReturnMetadata()
    {
        var service = CreateFileService();
        var bucket = GetTestBucketName();
        try
        {
            var key = GenerateRandomKey();
            var content = "metadata-test";
            var uploadResult = await service.UploadFileAsync(ContentStream(content), bucket, key);
            uploadResult.IsSuccessful.Should().BeTrue(uploadResult.ErrorMessage);
            var result = await service.GetFileMetadataAsync(bucket, key);
            result.IsSuccessful.Should().BeTrue(result.ErrorMessage);
            result.Data.Should().NotBeNull(result.ErrorMessage);
            result.Data!.Size.Should().Be(content.Length);
        }
        finally
        {
            await service.CleanupBucketAsync(bucket);
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task GetFileTagsAsync_ShouldReturnTags()
    {
        var service = CreateFileService();
        var bucket = GetTestBucketName();
        try
        {
            var key = GenerateRandomKey();
            var tags = new Dictionary<string, string> { ["foo"] = "bar" };
            var uploadResult = await service.UploadFileAsync(ContentStream("tagged"), bucket, key, tags: tags);
            uploadResult.IsSuccessful.Should().BeTrue(uploadResult.ErrorMessage);
            var result = await service.GetFileTagsAsync(bucket, key);
            result.IsSuccessful.Should().BeTrue(result.ErrorMessage);
            result.Data.Should().ContainKey("foo").WhoseValue.Should().Be("bar");
        }
        finally
        {
            await service.CleanupBucketAsync(bucket);
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task SetFileTagsAsync_ShouldSetTags()
    {
        var service = CreateFileService();
        var bucket = GetTestBucketName();
        try
        {
            var key = GenerateRandomKey();
            var uploadResult = await service.UploadFileAsync(ContentStream("tag-set"), bucket, key);
            uploadResult.IsSuccessful.Should().BeTrue(uploadResult.ErrorMessage);
            var tags = new Dictionary<string, string> { ["alpha"] = "beta" };
            var setResult = await service.SetFileTagsAsync(bucket, key, tags);
            setResult.IsSuccessful.Should().BeTrue(setResult.ErrorMessage);
            var getResult = await service.GetFileTagsAsync(bucket, key);
            getResult.IsSuccessful.Should().BeTrue(getResult.ErrorMessage);
            getResult.Data.Should().ContainKey("alpha").WhoseValue.Should().Be("beta");
        }
        finally
        {
            await service.CleanupBucketAsync(bucket);
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task SetFileAccessibilityAsync_ShouldSetAccessibility()
    {
        var service = CreateFileService();
        var bucket = GetTestBucketName();
        try
        {
            var key = GenerateRandomKey();
            var uploadResult = await service.UploadFileAsync(ContentStream("accessibility"), bucket, key);
            uploadResult.IsSuccessful.Should().BeTrue(uploadResult.ErrorMessage);
            var result = await service.SetFileAccessibilityAsync(bucket, key, FileAccessibility.PublicRead);
            if (result.IsSuccessful && !IsPubSubServiceS3Compatible(service)) // Minio doesn't support objet ACLs.
                result.IsSuccessful.Should().BeTrue(result.ErrorMessage);
        }
        finally
        {
            await service.CleanupBucketAsync(bucket);
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task CreateSignedUploadUrlAsync_ShouldCreateSignedUploadUrl()
    {
        var service = CreateFileService();
        if (IsFileServiceBasic(service)) return; //Manually tested and verified. Github actions do not support http server creation.

        var bucket = GetTestBucketName();
        try
        {
            var key = GenerateRandomKey();
            var content = $"Signed upload test {Guid.NewGuid()}";
            var result = await service.CreateSignedUploadUrlAsync(bucket, key);
            result.IsSuccessful.Should().BeTrue(result.ErrorMessage);
            result.Data.Should().NotBeNull(result.ErrorMessage);
            result.Data!.Url.Should().NotBeNullOrEmpty(result.ErrorMessage);

            // Upload content using the signed URL
            using var httpClient = new HttpClient();
            var putResponse = await httpClient.PutAsync(result.Data.Url, new StringContent(content, System.Text.Encoding.UTF8));
            putResponse.EnsureSuccessStatusCode();

            // Download and verify content
            using var ms = new MemoryStream();
            var downloadResult = await service.DownloadFileAsync(bucket, key, new StringOrStream(ms, 0));
            downloadResult.IsSuccessful.Should().BeTrue(downloadResult.ErrorMessage);
            ms.Position = 0;
            using var reader = new StreamReader(ms, System.Text.Encoding.UTF8);
            var downloaded = await reader.ReadToEndAsync();
            downloaded.Should().Be(content);
        }
        finally
        {
            await service.CleanupBucketAsync(bucket);
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task CreateSignedDownloadUrlAsync_ShouldCreateSignedDownloadUrl()
    {
        var service = CreateFileService();
        if (IsFileServiceBasic(service)) return; //Manually tested and verified. Github actions do not support http server creation.
        var bucket = GetTestBucketName();
        try
        {
            var key = GenerateRandomKey();
            var content = $"Signed download test {Guid.NewGuid()}";
            var uploadResult = await service.UploadFileAsync(ContentStream(content), bucket, key);
            uploadResult.IsSuccessful.Should().BeTrue(uploadResult.ErrorMessage);
            var result = await service.CreateSignedDownloadUrlAsync(bucket, key);
            result.IsSuccessful.Should().BeTrue(result.ErrorMessage);
            result.Data.Should().NotBeNull(result.ErrorMessage);
            result.Data!.Url.Should().NotBeNullOrEmpty(result.ErrorMessage);

            // Download content using the signed URL
            using var httpClient = new HttpClient();
            var getResponse = await httpClient.GetAsync(result.Data.Url);
            getResponse.EnsureSuccessStatusCode();
            var downloaded = await getResponse.Content.ReadAsStringAsync();
            downloaded.Should().Be(content);
        }
        finally
        {
            await service.CleanupBucketAsync(bucket);
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task ListFilesAsync_ShouldListFiles()
    {
        var service = CreateFileService();
        var bucket = GetTestBucketName();
        try
        {
            var key = GenerateRandomKey();
            var uploadResult = await service.UploadFileAsync(ContentStream("list-me"), bucket, key);
            uploadResult.IsSuccessful.Should().BeTrue(uploadResult.ErrorMessage);
            var result = await service.ListFilesAsync(bucket);
            result.IsSuccessful.Should().BeTrue(result.ErrorMessage);
            result.Data.Should().NotBeNull(result.ErrorMessage);
            result.Data!.FileKeys.Should().Contain(key, result.ErrorMessage);
        }
        finally
        {
            await service.CleanupBucketAsync(bucket);
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task CreateNotificationAsync_ShouldCreateNotification()
    {
        var service = CreateFileService();
        if (IsFileServiceAWSNotButS3Compatible(service)) return; //Manually tested and verified. Eventual consistency makes this test flaky.

        var bucket = GetTestBucketName();
        var topic = $"test-topic-{StringUtilities.GenerateRandomString(8)}";
        var prefix = GenerateRandomKey("notif/");
        var uploadedKey = prefix + "/file.txt";
        var uploadedContent = $"Notification test {Guid.NewGuid()}";
        var pubsubService = CreatePubSubService();

        try
        {
            // Set up message receipt tracking
            var messageReceived1 = new TaskCompletionSource<string>();
            var messageReceived2 = new TaskCompletionSource<string>();
            var subscribeResult1 = await pubsubService.SubscribeAsync(topic, (_, message) =>
            {
                if (message.Contains(uploadedKey) && CheckForUploadedEventContent(message))
                {
                    messageReceived1.TrySetResult(message);
                }
                return Task.CompletedTask;
            });
            var subscribeResult2 = await pubsubService.SubscribeAsync(topic, (_, message) =>
            {
                if (message.Contains(uploadedKey) && CheckForUploadedEventContent(message))
                {
                    messageReceived2.TrySetResult(message);
                }
                return Task.CompletedTask;
            });

            subscribeResult1.IsSuccessful.Should().BeTrue("Failed to subscribe to notification topic(1)");
            subscribeResult2.IsSuccessful.Should().BeTrue("Failed to subscribe to notification topic(2)");

            // Create notification for upload events
            var result = await service.CreateNotificationAsync(bucket, topic, prefix, [FileNotificationEventType.Uploaded], pubsubService);
            result.IsSuccessful.Should().BeTrue(result.ErrorMessage);

            // Wait a bit for the subscription to be ready
            await Task.Delay(5000);

            // Publish an event: upload a file that matches the prefix
            var uploadResult = await service.UploadFileAsync(ContentStream(uploadedContent), bucket, uploadedKey);
            uploadResult.IsSuccessful.Should().BeTrue(uploadResult.ErrorMessage);

            // Wait and check for the Pub/Sub message
            var receivedMessage1 = await messageReceived1.Task.WaitAsync(TimeSpan.FromSeconds(10));
            var receivedMessage2 = await messageReceived2.Task.WaitAsync(TimeSpan.FromSeconds(5));
            receivedMessage1.Should().NotBeNullOrEmpty($"(1) Expected a Pub/Sub message for uploaded file '{uploadedKey}'");
            receivedMessage2.Should().NotBeNullOrEmpty($"(2) Expected a Pub/Sub message for uploaded file '{uploadedKey}'");
        }
        finally
        {
            await service.CleanupBucketAsync(bucket);

            try
            {
                await service.DeleteNotificationsAsync(pubsubService, bucket, topic);
            }
            catch (Exception)
            {
                // ignored
            }

            try
            {
                await pubsubService.DeleteTopicAsync(topic);
            }
            catch
            {
                // ignored
            }

            // Dispose pub/sub service if it implements IAsyncDisposable
            if (pubsubService is IAsyncDisposable asyncDisposable)
            {
                try
                {
                    await asyncDisposable.DisposeAsync();
                }
                catch
                {
                    // ignored
                }
            }
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task CreateNotificationAsync_ShouldDetectDeletedFiles()
    {
        var service = CreateFileService();
        if (IsFileServiceAWSNotButS3Compatible(service)) return; //Manually tested and verified. Eventual consistency makes this test flaky.

        var bucket = GetTestBucketName();
        var topic = $"test-delete-topic-{StringUtilities.GenerateRandomString(8)}";
        var prefix = GenerateRandomKey("delete-notif/");
        var deletedKey = prefix + "/file-to-delete.txt";
        var deletedContent = $"Delete notification test {Guid.NewGuid()}";
        var pubsubService = CreatePubSubService();

        try
        {
            // Upload a file first
            var uploadResult = await service.UploadFileAsync(ContentStream(deletedContent), bucket, deletedKey);
            uploadResult.IsSuccessful.Should().BeTrue(uploadResult.ErrorMessage);

            // Set up message receipt tracking for delete events
            var deleteMessageReceived = new TaskCompletionSource<bool>();

            var subscribeResult = await pubsubService.SubscribeAsync(topic, (_, message) =>
            {
                if (message.Contains(deletedKey)
                    && CheckForDeletedEventContent(message))
                {
                    deleteMessageReceived.TrySetResult(true);
                }
                return Task.CompletedTask;
            });
            subscribeResult.IsSuccessful.Should().BeTrue("Failed to subscribe to delete notification topic");

            // Create notification for delete events
            var result = await service.CreateNotificationAsync(bucket, topic, prefix, [FileNotificationEventType.Deleted], pubsubService);
            result.IsSuccessful.Should().BeTrue(result.ErrorMessage);

            // Wait for the S3Compatible background task to establish the baseline file state
            if (IsPubSubServiceS3Compatible(service))
            {
                await Task.Delay(10000);
            }
            else
            {
                await Task.Delay(5000);
            }
            // Delete the file
            var deleteResult = await service.DeleteFileAsync(bucket, deletedKey);
            deleteResult.IsSuccessful.Should().BeTrue(deleteResult.ErrorMessage);

            // Wait and check for the delete notification
            var receivedMessage = await deleteMessageReceived.Task.WaitAsync(TimeSpan.FromSeconds(15));
            receivedMessage.Should().Be(true, "Not found.");
        }
        finally
        {
            await service.CleanupBucketAsync(bucket);

            try
            {
                await service.DeleteNotificationsAsync(pubsubService, bucket, topic);
                await pubsubService.DeleteTopicAsync(topic);
            }
            catch
            {
                // ignored
            }

            if (pubsubService is IAsyncDisposable asyncDisposable)
            {
                try
                {
                    await asyncDisposable.DisposeAsync();
                }
                catch
                {
                    // ignored
                }
            }
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task CreateNotificationAsync_ShouldFilterByPathPrefix()
    {
        var service = CreateFileService();
        if (IsFileServiceAWSNotButS3Compatible(service)) return; //Manually tested and verified. Eventual consistency makes this test flaky.
        var bucket = GetTestBucketName();
        var topic = $"test-prefix-topic-{StringUtilities.GenerateRandomString(8)}";
        var matchingPrefix = GenerateRandomKey("matching/");
        var nonMatchingPrefix = GenerateRandomKey("non-matching/");
        var matchingKey = matchingPrefix + "/file.txt";
        var nonMatchingKey = nonMatchingPrefix + "/file.txt";
        var pubsubService = CreatePubSubService();

        try
        {
            var messagesReceived = new List<string>();
            var subscribeResult = await pubsubService.SubscribeAsync(topic, (_, message) =>
            {
                messagesReceived.Add(message);
                return Task.CompletedTask;
            });
            subscribeResult.IsSuccessful.Should().BeTrue("Failed to subscribe to prefix notification topic");

            // Create notification only for matching prefix
            var result = await service.CreateNotificationAsync(bucket, topic, matchingPrefix, [FileNotificationEventType.Uploaded], pubsubService);
            result.IsSuccessful.Should().BeTrue(result.ErrorMessage);

            await Task.Delay(5000);

            // Upload files with both prefixes
            var matchingUpload = await service.UploadFileAsync(ContentStream("matching content"), bucket, matchingKey);
            matchingUpload.IsSuccessful.Should().BeTrue(matchingUpload.ErrorMessage);

            var nonMatchingUpload = await service.UploadFileAsync(ContentStream("non-matching content"), bucket, nonMatchingKey);
            nonMatchingUpload.IsSuccessful.Should().BeTrue(nonMatchingUpload.ErrorMessage);

            // Wait for notifications
            await Task.Delay(10000);

            messagesReceived.Should().NotBeEmpty("Should receive notifications for matching prefix");
            messagesReceived.Should().ContainSingle(m => m.Contains(matchingKey), "Should only receive notification for matching prefix file");
            messagesReceived.Should().NotContain(m => m.Contains(nonMatchingKey), "Should not receive notification for non-matching prefix file");
        }
        finally
        {
            await service.CleanupBucketAsync(bucket);

            try
            {
                await service.DeleteNotificationsAsync(pubsubService, bucket, topic);
                await pubsubService.DeleteTopicAsync(topic);
            }
            catch
            {
                // ignored
            }

            if (pubsubService is IAsyncDisposable asyncDisposable)
            {
                try
                {
                    await asyncDisposable.DisposeAsync();
                }
                catch
                {
                    // ignored
                }
            }
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task CreateNotificationAsync_ShouldHandleMultipleEventTypes()
    {
        var service = CreateFileService();
        if (IsFileServiceAWSNotButS3Compatible(service)) return; //Manually tested and verified. Eventual consistency makes this test flaky.
        var bucket = GetTestBucketName();
        var topic = $"test-multi-event-topic-{StringUtilities.GenerateRandomString(8)}";
        var prefix = GenerateRandomKey("multi-event/");
        var fileKey = prefix + "/multi-event-file.txt";
        var pubsubService = CreatePubSubService();

        try
        {
            var messagesReceived = new List<string>();
            var subscribeResult = await pubsubService.SubscribeAsync(topic, (_, message) =>
            {
                if (message.Contains(fileKey))
                {
                    messagesReceived.Add(message);
                }
                return Task.CompletedTask;
            });
            subscribeResult.IsSuccessful.Should().BeTrue("Failed to subscribe to multi-event notification topic");

            // Create notification for both upload and delete events
            var result = await service.CreateNotificationAsync(bucket, topic, prefix,
                [FileNotificationEventType.Uploaded, FileNotificationEventType.Deleted], pubsubService);
            result.IsSuccessful.Should().BeTrue(result.ErrorMessage);

            // Wait for S3Compatible background task to establish baseline (if applicable)
            if (IsPubSubServiceS3Compatible(service))
            {
                await Task.Delay(8000);
            }
            else
            {
                await Task.Delay(5000);
            }

            // Upload a file
            var uploadResult = await service.UploadFileAsync(ContentStream("multi-event content"), bucket, fileKey);
            uploadResult.IsSuccessful.Should().BeTrue(uploadResult.ErrorMessage);

            await Task.Delay(5000);

            // Delete the file
            var deleteResult = await service.DeleteFileAsync(bucket, fileKey);
            deleteResult.IsSuccessful.Should().BeTrue(deleteResult.ErrorMessage);

            // Wait for both notifications
            await Task.Delay(10000);

            messagesReceived.Should().NotBeEmpty("Should receive notifications for both events");
            messagesReceived.Should().Contain(m => CheckForUploadedEventContent(m),
                "Should receive upload notification");
            messagesReceived.Should().Contain(m => CheckForDeletedEventContent(m),
                "Should receive delete notification");
        }
        finally
        {
            await service.CleanupBucketAsync(bucket);

            try
            {
                await service.DeleteNotificationsAsync(pubsubService, bucket, topic);
                await pubsubService.DeleteTopicAsync(topic);
            }
            catch
            {
                // ignored
            }

            if (pubsubService is IAsyncDisposable asyncDisposable)
            {
                try
                {
                    await asyncDisposable.DisposeAsync();
                }
                catch
                {
                    // ignored
                }
            }
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task DeleteNotificationsAsync_ShouldDeleteSpecificNotification()
    {
        var service = CreateFileService();
        if (IsFileServiceAWSNotButS3Compatible(service)) return; //Manually tested and verified. Eventual consistency makes this test flaky.
        var bucket = GetTestBucketName();
        var topic1 = $"test-delete-notif1-{StringUtilities.GenerateRandomString(8)}";
        var topic2 = $"test-delete-notif2-{StringUtilities.GenerateRandomString(8)}";
        var prefix1 = GenerateRandomKey("notif1/");
        var prefix2 = GenerateRandomKey("notif2/");
        var pubsubService = CreatePubSubService();

        try
        {
            // Create two different notifications
            var result1 = await service.CreateNotificationAsync(bucket, topic1, prefix1, [FileNotificationEventType.Uploaded], pubsubService);
            result1.IsSuccessful.Should().BeTrue(result1.ErrorMessage);

            var result2 = await service.CreateNotificationAsync(bucket, topic2, prefix2, [FileNotificationEventType.Uploaded], pubsubService);
            result2.IsSuccessful.Should().BeTrue(result2.ErrorMessage);

            // Delete only the first notification
            var deleteResult = await service.DeleteNotificationsAsync(pubsubService, bucket, topic1);
            deleteResult.IsSuccessful.Should().BeTrue(deleteResult.ErrorMessage);
            deleteResult.Data.Should().Be(1, "Should delete exactly one notification");

            // Upload files to both prefixes and verify only the second notification works
            var messagesReceived = new List<string>();
            var subscribeResult2 = await pubsubService.SubscribeAsync(topic2, (_, message) =>
            {
                messagesReceived.Add(message);
                return Task.CompletedTask;
            });
            subscribeResult2.IsSuccessful.Should().BeTrue("Failed to subscribe to remaining topic");

            await Task.Delay(5000);

            var key1 = prefix1 + "/file1.txt";
            var key2 = prefix2 + "/file2.txt";

            await service.UploadFileAsync(ContentStream("content1"), bucket, key1);
            await service.UploadFileAsync(ContentStream("content2"), bucket, key2);

            await Task.Delay(8000);

            messagesReceived.Should().ContainSingle(m => m.Contains(key2), "Should only receive notification for topic2");
            messagesReceived.Should().NotContain(m => m.Contains(key1), "Should not receive notification for deleted topic1");
        }
        finally
        {
            await service.CleanupBucketAsync(bucket);

            try
            {
                await service.DeleteNotificationsAsync(pubsubService, bucket);
                await pubsubService.DeleteTopicAsync(topic1);
                await pubsubService.DeleteTopicAsync(topic2);
            }
            catch
            {
                // ignored
            }

            if (pubsubService is IAsyncDisposable asyncDisposable)
            {
                try
                {
                    await asyncDisposable.DisposeAsync();
                }
                catch
                {
                    // ignored
                }
            }
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task DeleteNotificationsAsync_ShouldDeleteAllNotifications()
    {
        var service = CreateFileService();
        var bucket = GetTestBucketName();
        var topic1 = $"test-delete-all1-{StringUtilities.GenerateRandomString(8)}";
        var topic2 = $"test-delete-all2-{StringUtilities.GenerateRandomString(8)}";
        var prefix1 = GenerateRandomKey("all1/");
        var prefix2 = GenerateRandomKey("all2/");
        var pubsubService = CreatePubSubService();

        try
        {
            // Create multiple notifications
            var result1 = await service.CreateNotificationAsync(bucket, topic1, prefix1, [FileNotificationEventType.Uploaded], pubsubService);
            result1.IsSuccessful.Should().BeTrue(result1.ErrorMessage);

            var result2 = await service.CreateNotificationAsync(bucket, topic2, prefix2, [FileNotificationEventType.Deleted], pubsubService);
            result2.IsSuccessful.Should().BeTrue(result2.ErrorMessage);

            // Delete all notifications
            var deleteResult = await service.DeleteNotificationsAsync(pubsubService, bucket);
            deleteResult.IsSuccessful.Should().BeTrue(deleteResult.ErrorMessage);
            deleteResult.Data.Should().Be(2, "Should delete exactly two notifications");

            // Verify no notifications work after deletion
            var messagesReceived = new List<string>();
            _ = await pubsubService.SubscribeAsync(topic1, (_, message) =>
            {
                messagesReceived.Add(message);
                return Task.CompletedTask;
            });
            _ = await pubsubService.SubscribeAsync(topic2, (_, message) =>
            {
                messagesReceived.Add(message);
                return Task.CompletedTask;
            });

            await Task.Delay(5000);

            var key1 = prefix1 + "/file1.txt";
            var key2 = prefix2 + "/file2.txt";

            await service.UploadFileAsync(ContentStream("content1"), bucket, key1);
            await service.DeleteFileAsync(bucket, key1);
            await service.UploadFileAsync(ContentStream("content2"), bucket, key2);

            await Task.Delay(10000);

            messagesReceived.Should().BeEmpty("Should not receive any notifications after deletion");
        }
        finally
        {
            await service.CleanupBucketAsync(bucket);

            try
            {
                await pubsubService.DeleteTopicAsync(topic1);
                await pubsubService.DeleteTopicAsync(topic2);
            }
            catch
            {
                // ignored
            }

            if (pubsubService is IAsyncDisposable asyncDisposable)
            {
                try
                {
                    await asyncDisposable.DisposeAsync();
                }
                catch
                {
                    // ignored
                }
            }
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task CreateNotificationAsync_ShouldHandleRapidFileOperations()
    {
        var service = CreateFileService();
        if (IsFileServiceAWSNotButS3Compatible(service)) return; //Manually tested and verified. Eventual consistency makes this test flaky.
        var bucket = GetTestBucketName();
        var topic = $"test-rapid-topic-{StringUtilities.GenerateRandomString(8)}";
        var prefix = GenerateRandomKey("rapid/");
        var pubsubService = CreatePubSubService();

        try
        {
            var messagesReceived = new List<string>();
            var subscribeResult = await pubsubService.SubscribeAsync(topic, (_, message) =>
            {
                messagesReceived.Add(message);
                return Task.CompletedTask;
            },
            e => testOutputHelper.WriteLine(e.ToString()));
            subscribeResult.IsSuccessful.Should().BeTrue("Failed to subscribe to rapid operations topic");

            var result = await service.CreateNotificationAsync(bucket, topic, prefix,
                [FileNotificationEventType.Uploaded, FileNotificationEventType.Deleted], pubsubService);
            result.IsSuccessful.Should().BeTrue(result.ErrorMessage);

            // Wait for S3Compatible background task to establish baseline (if applicable)
            if (IsPubSubServiceS3Compatible(service))
            {
                await Task.Delay(8000);
            }
            else
            {
                await Task.Delay(5000);
            }

            // Perform rapid file operations
            var tasks = new List<Task>();
            for (var i = 0; i < 5; i++)
            {
                var fileKey = prefix + $"/rapid-file-{i}.txt";
                var content = $"rapid content {i}";

                tasks.Add(Task.Run(async () =>
                {
                    await service.UploadFileAsync(ContentStream(content), bucket, fileKey);
                }));
            }

            await Task.WhenAll(tasks);

            var retryCount = 0;
            while (messagesReceived.Count < 5 && ++retryCount < 40)
            {
                await Task.Delay(1000);
            }

            messagesReceived.Count.Should().BeGreaterOrEqualTo(5, "Should receive at least 5 notifications");
        }
        finally
        {
            await service.CleanupBucketAsync(bucket);

            try
            {
                await service.DeleteNotificationsAsync(pubsubService, bucket, topic);
                await pubsubService.DeleteTopicAsync(topic);
            }
            catch
            {
                // ignored
            }

            if (pubsubService is IAsyncDisposable asyncDisposable)
            {
                try
                {
                    await asyncDisposable.DisposeAsync();
                }
                catch
                {
                    // ignored
                }
            }
        }
    }

    [RetryFact(3, 5000)]
    public virtual async Task CreateNotificationAsync_ShouldDetectFileModifications()
    {
        var service = CreateFileService();
        if (IsFileServiceAWSNotButS3Compatible(service)) return; //Manually tested and verified. Eventual consistency makes this test flaky.
        var bucket = GetTestBucketName();
        var topic = $"test-modification-topic-{StringUtilities.GenerateRandomString(8)}";
        var prefix = GenerateRandomKey("modification/");
        var fileKey = prefix + "/file-to-modify.txt";
        var pubsubService = CreatePubSubService();

        try
        {
            var messagesReceived = new List<string>();
            var subscribeResult = await pubsubService.SubscribeAsync(topic, (_, message) =>
            {
                if (message.Contains(fileKey))
                {
                    messagesReceived.Add(message);
                }
                return Task.CompletedTask;
            });
            subscribeResult.IsSuccessful.Should().BeTrue("Failed to subscribe to modification notification topic");

            var result = await service.CreateNotificationAsync(bucket, topic, prefix, [FileNotificationEventType.Uploaded], pubsubService);
            result.IsSuccessful.Should().BeTrue(result.ErrorMessage);

            // Upload initial file
            var initialUpload = await service.UploadFileAsync(ContentStream("initial content"), bucket, fileKey);
            initialUpload.IsSuccessful.Should().BeTrue(initialUpload.ErrorMessage);

            // Wait for background task to establish baseline
            await Task.Delay(8000);

            // Clear received messages after baseline is established
            messagesReceived.Clear();

            // Modify the file (re-upload with different content)
            var modifiedUpload = await service.UploadFileAsync(ContentStream("modified content - this is different"), bucket, fileKey);
            modifiedUpload.IsSuccessful.Should().BeTrue(modifiedUpload.ErrorMessage);

            // Wait for modification notification
            var retryCount = 0;
            while (!messagesReceived.Any(CheckForUploadedEventContent) && ++retryCount < 30)
            {
                await Task.Delay(1000);

            }

            messagesReceived.Should().NotBeEmpty("Should receive notification for file modification");
            messagesReceived.Should().Contain(m => CheckForUploadedEventContent(m), "Should receive upload notification for modification");
        }
        finally
        {
            await service.CleanupBucketAsync(bucket);

            try
            {
                await service.DeleteNotificationsAsync(pubsubService, bucket, topic);
                await pubsubService.DeleteTopicAsync(topic);
            }
            catch
            {
                // ignored
            }

            if (pubsubService is IAsyncDisposable asyncDisposable)
            {
                try
                {
                    await asyncDisposable.DisposeAsync();
                }
                catch
                {
                    // ignored
                }
            }
        }
    }

    private static bool IsPubSubServiceS3Compatible(object obj)
    {
        return IsServiceDerivedFrom(obj, "S3Compatible");
    }
    private static bool IsFileServiceAWSNotButS3Compatible(object obj)
    {
        return !IsServiceDerivedFrom(obj, "S3Compatible") && IsServiceDerivedFrom(obj, "AWS");
    }

    private static bool IsFileServiceBasic(object obj)
    {
        return IsServiceDerivedFrom(obj, "Basic");
    }
    private static bool IsServiceDerivedFrom(object obj, string from)
    {
        var type = obj.GetType();
        while (type?.FullName != null)
        {
            if (type.FullName.Contains(from)) return true;
            type = type.BaseType;
        }
        return false;
    }

    private static bool CheckForUploadedEventContent(string message)
    {
        return message.Contains("Uploaded") || message.Contains("ObjectCreated") || message.Contains("OBJECT_FINALIZE");
    }

    private static bool CheckForDeletedEventContent(string message)
    {
        return message.Contains("Deleted") || message.Contains("ObjectRemoved") || message.Contains("OBJECT_DELETE");
    }
}
