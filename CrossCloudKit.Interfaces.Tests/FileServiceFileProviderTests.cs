// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces.Classes.Asp;
using CrossCloudKit.File.Basic;
using CrossCloudKit.Memory.Basic;
using CrossCloudKit.PubSub.Basic;
using CrossCloudKit.Utilities.Common;
using FluentAssertions;
using Microsoft.Extensions.FileProviders;
using Xunit;

namespace CrossCloudKit.Interfaces.Tests;

public class FileServiceFileProviderTests : IDisposable
{
    private readonly IFileService _fileService;
    private readonly IPubSubService _pubSubService;
    private readonly string _bucketName = "test-file-provider-bucket";
    private readonly string _rootPath = "file-provider-root";
    private readonly List<Exception> _capturedErrors = new();

    private static StringOrStream ContentStream(string content)
    {
        return new StringOrStream(
            new MemoryTributary(
                System.Text.Encoding.UTF8.GetBytes(content)),
            System.Text.Encoding.UTF8.GetByteCount(content));
    }

    public FileServiceFileProviderTests()
    {
        _pubSubService = new PubSubServiceBasic();
        _fileService = new FileServiceBasic(
            memoryService: new MemoryServiceBasic(),
            pubSubService: _pubSubService);
    }

    public void Dispose()
    {
        _fileService.CleanupBucketAsync(_bucketName);
    }

    private FileServiceFileProvider CreateFileProvider(Action<Exception>? errorAction = null)
    {
        return new FileServiceFileProvider(
            _fileService,
            _bucketName,
            _rootPath,
            errorAction ?? (ex => _capturedErrors.Add(ex)),
            _pubSubService);
    }

    #region Constructor Tests

    [Fact]
    public void Constructor_WithNullFileService_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new FileServiceFileProvider(
            null!, _bucketName, _rootPath));
    }

    [Fact]
    public void Constructor_WithNullBucketName_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new FileServiceFileProvider(
            _fileService, null!, _rootPath));
    }

    [Fact]
    public void Constructor_WithNullRootPath_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new FileServiceFileProvider(
            _fileService, _bucketName, null!));
    }

    [Fact]
    public void Constructor_WithValidParameters_ShouldSucceed()
    {
        // Act
        using var provider = CreateFileProvider();

        // Assert
        provider.Should().NotBeNull();
    }

    #endregion

    #region GetFileInfo Tests

    [Fact]
    public async Task GetFileInfo_WithExistingFile_ShouldReturnFileInfo()
    {
        // Arrange
        using var provider = CreateFileProvider();
        var filePath = "test.txt";
        var content = "Hello World";
        await using var sos = ContentStream(content);

        await _fileService.UploadFileAsync(
            sos,
            _bucketName,
            $"{_rootPath}/{filePath}");

        // Act
        var fileInfo = provider.GetFileInfo(filePath);

        // Assert
        fileInfo.Should().NotBeNull();
        fileInfo.Exists.Should().BeTrue();
        fileInfo.IsDirectory.Should().BeFalse();
        fileInfo.Name.Should().Be("test.txt");
        fileInfo.Length.Should().Be(content.Length);
        fileInfo.PhysicalPath.Should().BeNull();
    }

    [Fact]
    public void GetFileInfo_WithNonExistingFile_ShouldReturnNotFoundFileInfo()
    {
        // Arrange
        using var provider = CreateFileProvider();
        var filePath = "nonexistent.txt";

        // Act
        var fileInfo = provider.GetFileInfo(filePath);

        // Assert
        fileInfo.Should().NotBeNull();
        fileInfo.Exists.Should().BeFalse();
        fileInfo.Should().BeOfType<NotFoundFileInfo>();
        _capturedErrors.Should().HaveCount(1);
        _capturedErrors[0].Should().BeOfType<FileNotFoundException>();
    }

    [Fact]
    public void GetFileInfo_WithEmptyPath_ShouldReturnNotFoundFileInfo()
    {
        // Arrange
        using var provider = CreateFileProvider();

        // Act
        var fileInfo = provider.GetFileInfo("");

        // Assert
        fileInfo.Should().NotBeNull();
        fileInfo.Exists.Should().BeFalse();
        fileInfo.Should().BeOfType<NotFoundFileInfo>();
        _capturedErrors.Should().HaveCount(1);
        _capturedErrors[0].Should().BeOfType<InvalidOperationException>();
    }

    [Fact]
    public async Task GetFileInfo_FileStream_ShouldReturnCorrectContent()
    {
        // Arrange
        using var provider = CreateFileProvider();
        var filePath = "streamtest.txt";
        var content = "Stream test content";
        await using var sos = ContentStream(content);

        await _fileService.UploadFileAsync(
            sos,
            _bucketName,
            $"{_rootPath}/{filePath}");

        // Act
        var fileInfo = provider.GetFileInfo(filePath);
        await using var stream = fileInfo.CreateReadStream();
        using var reader = new StreamReader(stream);
        var actualContent = await reader.ReadToEndAsync();

        // Assert
        actualContent.Should().Be(content);
    }

    [Fact]
    public void GetFileInfo_WithDisposedProvider_ShouldReturnNotFoundFileInfo()
    {
        // Arrange
        var provider = CreateFileProvider();
        provider.Dispose();

        // Act
        var fileInfo = provider.GetFileInfo("test.txt");

        // Assert
        fileInfo.Should().NotBeNull();
        fileInfo.Exists.Should().BeFalse();
        fileInfo.Should().BeOfType<NotFoundFileInfo>();
        _capturedErrors.Should().HaveCount(1);
        _capturedErrors[0].Should().BeOfType<ObjectDisposedException>();
    }

    #endregion

    #region GetDirectoryContents Tests

    [Fact]
    public async Task GetDirectoryContents_WithFilesAndDirectories_ShouldReturnCorrectStructure()
    {
        // Arrange
        using var provider = CreateFileProvider();

        await using var sos = ContentStream("content");

        // Create test structure:
        // root/
        //   file1.txt
        //   file2.txt
        //   subdir1/
        //     nested1.txt
        //   subdir2/
        //     nested2.txt
        //     deepdir/
        //       deep.txt

        await _fileService.UploadFileAsync(sos, _bucketName, $"{_rootPath}/file1.txt");
        await _fileService.UploadFileAsync(sos, _bucketName, $"{_rootPath}/file2.txt");
        await _fileService.UploadFileAsync(sos, _bucketName, $"{_rootPath}/subdir1/nested1.txt");
        await _fileService.UploadFileAsync(sos, _bucketName, $"{_rootPath}/subdir2/nested2.txt");
        await _fileService.UploadFileAsync(sos, _bucketName, $"{_rootPath}/subdir2/deepdir/deep.txt");

        // Act
        var contents = provider.GetDirectoryContents("");

        // Assert
        contents.Should().NotBeNull();
        contents.Exists.Should().BeTrue();

        var items = contents.ToList();
        items.Should().HaveCount(4); // 2 files + 2 directories

        var files = items.Where(i => !i.IsDirectory).ToList();
        var directories = items.Where(i => i.IsDirectory).ToList();

        files.Should().HaveCount(2);
        files.Should().Contain(f => f.Name == "file1.txt");
        files.Should().Contain(f => f.Name == "file2.txt");

        directories.Should().HaveCount(2);
        directories.Should().Contain(d => d.Name == "subdir1");
        directories.Should().Contain(d => d.Name == "subdir2");
    }

    [Fact]
    public async Task GetDirectoryContents_WithSubdirectory_ShouldReturnOnlyImmediateChildren()
    {
        // Arrange
        using var provider = CreateFileProvider();

        await using var sos = ContentStream("content");

        await _fileService.UploadFileAsync(sos, _bucketName, $"{_rootPath}/subdir/file1.txt");
        await _fileService.UploadFileAsync(sos, _bucketName, $"{_rootPath}/subdir/nested/deep.txt");

        // Act
        var contents = provider.GetDirectoryContents("subdir");

        // Assert
        contents.Should().NotBeNull();
        contents.Exists.Should().BeTrue();

        var items = contents.ToList();
        items.Should().HaveCount(2); // 1 file + 1 directory

        var files = items.Where(i => !i.IsDirectory).ToList();
        var directories = items.Where(i => i.IsDirectory).ToList();

        files.Should().HaveCount(1);
        files[0].Name.Should().Be("file1.txt");

        directories.Should().HaveCount(1);
        directories[0].Name.Should().Be("nested");
    }

    [Fact]
    public void GetDirectoryContents_WithNonExistentDirectory_ShouldReturnNotFound()
    {
        // Arrange
        using var provider = CreateFileProvider();

        // Act
        var contents = provider.GetDirectoryContents("nonexistent");

        // Assert
        contents.Should().NotBeNull();
        contents.Exists.Should().BeFalse();
        contents.Should().Equal(NotFoundDirectoryContents.Singleton);
    }

    [Fact]
    public Task GetDirectoryContents_WithEmptyDirectory_ShouldReturnEmptyContents()
    {
        // Arrange
        using var provider = CreateFileProvider();

        // Act - No files uploaded, so directory should be empty
        var contents = provider.GetDirectoryContents("");

        // Assert
        contents.Should().NotBeNull();
        contents.Exists.Should().BeFalse();
        contents.Should().BeEmpty();
        return Task.CompletedTask;
    }

    [Fact]
    public void GetDirectoryContents_WithDisposedProvider_ShouldReturnNotFound()
    {
        // Arrange
        var provider = CreateFileProvider();
        provider.Dispose();

        // Act
        var contents = provider.GetDirectoryContents("");

        // Assert
        contents.Should().NotBeNull();
        contents.Exists.Should().BeFalse();
        contents.Should().Equal(NotFoundDirectoryContents.Singleton);
        _capturedErrors.Should().HaveCount(1);
        _capturedErrors[0].Should().BeOfType<ObjectDisposedException>();
    }

    [Fact]
    public async Task GetDirectoryContents_DirectoryInfo_ShouldHaveCorrectProperties()
    {
        // Arrange
        using var provider = CreateFileProvider();

        await using var sos = ContentStream("content");

        await _fileService.UploadFileAsync(sos, _bucketName, $"{_rootPath}/testdir/file.txt");

        // Act
        var contents = provider.GetDirectoryContents("");
        var directory = contents.FirstOrDefault(i => i.IsDirectory);

        // Assert - Check if directory was found
        directory.Should().NotBeNull("Expected to find a directory in the contents");
        directory!.Exists.Should().BeTrue();
        directory.IsDirectory.Should().BeTrue();
        directory.Name.Should().Be("testdir");
        directory.Length.Should().Be(-1);
        directory.PhysicalPath.Should().BeNull();
        directory.LastModified.Should().BeCloseTo(DateTimeOffset.UtcNow, TimeSpan.FromMinutes(1));

        // Creating read stream should throw
        Assert.Throws<InvalidOperationException>(() => directory.CreateReadStream());
    }

    #endregion

    #region Watch Tests

    [Fact]
    public void Watch_WithValidFilter_ShouldReturnChangeToken()
    {
        // Arrange
        using var provider = CreateFileProvider();

        // Act
        var changeToken = provider.Watch("*");

        // Assert
        changeToken.Should().NotBeNull();
        changeToken.HasChanged.Should().BeFalse();
        changeToken.ActiveChangeCallbacks.Should().BeTrue();
    }

    [Fact]
    public void Watch_WithSameFilter_ShouldReturnSameToken()
    {
        // Arrange
        using var provider = CreateFileProvider();

        // Act
        var token1 = provider.Watch("test/*");
        var token2 = provider.Watch("test/*");

        // Assert
        token1.Should().BeSameAs(token2);
    }

    [Fact]
    public void Watch_WithDisposedProvider_ShouldReturnNullToken()
    {
        // Arrange
        var provider = CreateFileProvider();
        provider.Dispose();

        // Act
        var changeToken = provider.Watch("*");

        // Assert
        changeToken.Should().Be(NullChangeToken.Singleton);
        _capturedErrors.Should().HaveCount(1);
        _capturedErrors[0].Should().BeOfType<ObjectDisposedException>();
    }

    [Fact]
    public void Watch_WithNullPubSubService_ShouldReturnNullToken()
    {
        // Arrange
        using var provider = new FileServiceFileProvider(_fileService, _bucketName, _rootPath);

        // Act
        var changeToken = provider.Watch("*");

        // Assert
        changeToken.Should().Be(NullChangeToken.Singleton);
    }

    [Fact]
    public void Watch_ChangeTokenCallback_ShouldTriggerWhenNotified()
    {
        // Arrange
        using var provider = CreateFileProvider();
        var changeToken = provider.Watch("*");
        var callbackTriggered = false;
        object? callbackState = null;

        // Act
        using var registration = changeToken.RegisterChangeCallback(state =>
        {
            callbackTriggered = true;
            callbackState = state;
        }, "test-state");

        // Simulate change notification using reflection
        var tokenType = changeToken.GetType();
        var notifyMethod = tokenType.GetMethod("NotifyChange");
        notifyMethod?.Invoke(changeToken, null);

        // Assert
        changeToken.HasChanged.Should().BeTrue();
        callbackTriggered.Should().BeTrue();
        callbackState.Should().Be("test-state");
    }

    #endregion

    #region Edge Cases and Error Handling

    [Theory]
    [InlineData("/test.txt")]
    [InlineData("\\test.txt")]
    [InlineData("test\\file.txt")]
    [InlineData("test/file.txt")]
    public async Task GetFileInfo_WithVariousPathFormats_ShouldNormalizePaths(string inputPath)
    {
        // Arrange
        using var provider = CreateFileProvider();
        var normalizedPath = "test/file.txt";

        await using var sos = ContentStream("content");

        await _fileService.UploadFileAsync(
            sos,
            _bucketName,
            $"{_rootPath}/{normalizedPath}");

        // Act
        var fileInfo = provider.GetFileInfo(inputPath);

        // Assert - Should find the file regardless of path format
        if (inputPath.Contains("test") && inputPath.Contains("file.txt"))
        {
            fileInfo.Exists.Should().BeTrue();
        }
    }

    [Fact]
    public async Task GetDirectoryContents_WithDeeplyNestedStructure_ShouldOnlyShowImmediateChildren()
    {
        // Arrange
        using var provider = CreateFileProvider();

        await using var sos = ContentStream("content");

        // Create deeply nested structure
        await _fileService.UploadFileAsync(sos, _bucketName, $"{_rootPath}/level1/level2/level3/level4/deep.txt");
        await _fileService.UploadFileAsync(sos, _bucketName, $"{_rootPath}/level1/file1.txt");
        await _fileService.UploadFileAsync(sos, _bucketName, $"{_rootPath}/level1/level2/file2.txt");

        // Act
        var rootContents = provider.GetDirectoryContents("").ToList();
        var level1Contents = provider.GetDirectoryContents("level1").ToList();

        // Assert
        rootContents.Should().HaveCount(1);
        rootContents[0].IsDirectory.Should().BeTrue();
        rootContents[0].Name.Should().Be("level1");

        level1Contents.Should().HaveCount(2); // file1.txt + level2 directory
        level1Contents.Should().Contain(i => !i.IsDirectory && i.Name == "file1.txt");
        level1Contents.Should().Contain(i => i.IsDirectory && i.Name == "level2");
    }

    [Fact]
    public async Task GetDirectoryContents_WithManyFiles_ShouldHandlePagination()
    {
        // Arrange
        using var provider = CreateFileProvider();
        var fileCount = 50;

        await using var sos = ContentStream("content");

        // Upload many files
        for (int i = 0; i < fileCount; i++)
        {
            await _fileService.UploadFileAsync(
                sos,
                _bucketName,
                $"{_rootPath}/file{i:D3}.txt");
        }

        // Act
        var contents = provider.GetDirectoryContents("").ToList();

        // Assert
        contents.Should().HaveCount(fileCount);
        contents.Should().OnlyContain(f => !f.IsDirectory);
    }

    #endregion

    #region Disposal Tests

    [Fact]
    public void Dispose_ShouldCleanupResourcesAndNotThrow()
    {
        // Arrange
        var provider = CreateFileProvider();
        var changeToken = provider.Watch("*");

        // Act & Assert
        var exception1 = Record.Exception(() => provider.Dispose());
        var exception2 = Record.Exception(() => provider.Dispose()); // Double disposal should be safe

        exception1.Should().BeNull();
        exception2.Should().BeNull();
    }

    [Fact]
    public void Dispose_AfterDisposal_AllOperationsShouldReturnSafeDefaults()
    {
        // Arrange
        var provider = CreateFileProvider();
        provider.Dispose();

        // Act & Assert
        var fileInfo = provider.GetFileInfo("test.txt");
        fileInfo.Should().BeOfType<NotFoundFileInfo>();

        var contents = provider.GetDirectoryContents("");
        contents.Should().Equal(NotFoundDirectoryContents.Singleton);

        var changeToken = provider.Watch("*");
        changeToken.Should().Be(NullChangeToken.Singleton);
    }

    #endregion
}
