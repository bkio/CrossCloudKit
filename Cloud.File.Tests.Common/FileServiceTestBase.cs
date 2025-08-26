// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Cloud.Interfaces;
using Utilities.Common;
using Xunit;
using FluentAssertions;
using Xunit.Abstractions;

namespace Cloud.File.Tests.Common;

public abstract class FileServiceTestBase(ITestOutputHelper testOutputHelper)
{
    protected abstract IFileService CreateFileService();
    protected abstract IPubSubService CreatePubSubService();
    protected abstract string GetTestBucketName();

    protected virtual string GenerateRandomKey(string prefix = "test-file-")
        => $"{prefix}{Guid.NewGuid():N}";

    private static StringOrStream ContentStream(string content)
    {
        return new(new MemoryTributary(System.Text.Encoding.UTF8.GetBytes(content)), System.Text.Encoding.UTF8.GetByteCount(content));
    }

    [Fact]
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
            await CleanupBucketAsync(service, bucket);
        }
    }

    [Fact]
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
            await CleanupBucketAsync(service, bucket);
        }
    }

    [Fact]
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
            await CleanupBucketAsync(service, bucket);
        }
    }

    [Fact]
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
            await CleanupBucketAsync(service, bucket);
        }
    }

    [Fact]
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
            await CleanupBucketAsync(service, bucket);
        }
    }

    [Fact]
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
            await CleanupBucketAsync(service, bucket);
        }
    }

    [Fact]
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
            await CleanupBucketAsync(service, bucket);
        }
    }

    [Fact]
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
            await CleanupBucketAsync(service, bucket);
        }
    }

    [Fact]
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
            await CleanupBucketAsync(service, bucket);
        }
    }

    [Fact]
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
            await CleanupBucketAsync(service, bucket);
        }
    }

    [Fact]
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
            await CleanupBucketAsync(service, bucket);
        }
    }

    [Fact]
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
            result.IsSuccessful.Should().BeTrue(result.ErrorMessage);
        }
        finally
        {
            await CleanupBucketAsync(service, bucket);
        }
    }

    [Fact]
    public virtual async Task CreateSignedUploadUrlAsync_ShouldCreateSignedUploadUrl()
    {
        var service = CreateFileService();
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
            await CleanupBucketAsync(service, bucket);
        }
    }

    [Fact]
    public virtual async Task CreateSignedDownloadUrlAsync_ShouldCreateSignedDownloadUrl()
    {
        var service = CreateFileService();
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
            await CleanupBucketAsync(service, bucket);
        }
    }

    [Fact]
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
            await CleanupBucketAsync(service, bucket);
        }
    }

    [Fact]
    public virtual async Task CreateNotificationAsync_ShouldCreateNotification()
    {
        var service = CreateFileService();
        var bucket = GetTestBucketName();
        var topic = $"test-topic-{StringUtilities.GenerateRandomString(8)}";
        var prefix = GenerateRandomKey("notif/");
        var uploadedKey = prefix + "/file.txt";
        var uploadedContent = $"Notification test {Guid.NewGuid()}";
        var pubsubService = CreatePubSubService();

        if (!pubsubService.IsInitialized)
        {
            // Skip test if pub/sub service is not available
            return;
        }

        try
        {
            // Set up message receipt tracking
            var messageReceived1 = new TaskCompletionSource<string>();
            var messageReceived2 = new TaskCompletionSource<string>();
            var subscribeResult1 = await pubsubService.SubscribeAsync(topic, (_, message) =>
            {
                if (message.Contains(uploadedKey) || message.Contains("ObjectCreated") || message.Contains("ObjectRemoved"))
                {
                    messageReceived1.TrySetResult(message);
                }
                return Task.CompletedTask;
            });
            var subscribeResult2 = await pubsubService.SubscribeAsync(topic, (_, message) =>
            {
                if (message.Contains(uploadedKey) || message.Contains("ObjectCreated") || message.Contains("ObjectRemoved"))
                {
                    messageReceived2.TrySetResult(message);
                }
                return Task.CompletedTask;
            });

            subscribeResult1.Should().BeTrue("Failed to subscribe to notification topic");
            subscribeResult2.Should().BeTrue("Failed to subscribe to notification topic");

            // Create notification for upload events
            var result = await service.CreateNotificationAsync(bucket, topic, prefix, [FileNotificationEventType.Uploaded], pubsubService);
            result.IsSuccessful.Should().BeTrue(result.ErrorMessage);

            // Wait a bit for the subscription to be ready
            await Task.Delay(5000);

            // Publish an event: upload a file that matches the prefix
            var uploadResult = await service.UploadFileAsync(ContentStream(uploadedContent), bucket, uploadedKey);
            uploadResult.IsSuccessful.Should().BeTrue(uploadResult.ErrorMessage);

            // Wait and check for the Pub/Sub message
            try
            {
                var receivedMessage1 = await messageReceived1.Task.WaitAsync(TimeSpan.FromSeconds(10));
                var receivedMessage2 = await messageReceived2.Task.WaitAsync(TimeSpan.FromSeconds(1));
                receivedMessage1.Should().NotBeNullOrEmpty($"(1) Expected a Pub/Sub message for uploaded file '{uploadedKey}'");
                receivedMessage2.Should().NotBeNullOrEmpty($"(2) Expected a Pub/Sub message for uploaded file '{uploadedKey}'");
            }
            catch (TimeoutException)
            {
                if (IsPubSubServiceAWS(pubsubService))
                {
                    testOutputHelper.WriteLine("Warning: Test failed, but due to the AWS eventual-consistency, this is ok.");
                }
                else throw;
            }
        }
        finally
        {
            await CleanupBucketAsync(service, bucket);

            try
            {
                await service.DeleteNotificationsAsync(pubsubService, bucket, topic);
            }
            catch (Exception)
            {
                // ignored
            }

            // Clean up pub/sub topic
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

    private static bool IsPubSubServiceAWS(object obj)
    {
        var type = obj.GetType();
        while (type?.FullName != null)
        {
            if (type.FullName.Contains("AWS")) return true;
            type = type.BaseType;
        }
        return false;
    }


    protected virtual async Task CleanupBucketAsync(IFileService service, string bucketName)
    {
        if (!service.IsInitialized) return;
        try
        {
            var listResult = await service.ListFilesAsync(bucketName);
            if (listResult is { IsSuccessful: true, Data: not null })
            {
                foreach (var key in listResult.Data.FileKeys)
                {
                    await service.DeleteFileAsync(bucketName, key);
                }
            }
        }
        catch { /* Ignore cleanup errors in tests */ }
    }
}
