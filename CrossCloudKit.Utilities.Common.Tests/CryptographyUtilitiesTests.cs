// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Xunit;

namespace CrossCloudKit.Utilities.Common.Tests;

public class CryptographyUtilitiesTests
{
    [Fact]
    public void CalculateStringSha256_WithSimpleString_ReturnsExpectedHash()
    {
        // Arrange
        const string input = "hello world";
        const string expectedHash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";

        // Act
        var result = CryptographyUtilities.CalculateStringSha256(input);

        // Assert
        Assert.Equal(expectedHash, result);
    }

    [Fact]
    public void CalculateStringSha256_WithEmptyString_ReturnsEmptyStringHash()
    {
        // Arrange
        const string input = "";
        const string expectedHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

        // Act
        var result = CryptographyUtilities.CalculateStringSha256(input);

        // Assert
        Assert.Equal(expectedHash, result);
    }

    [Fact]
    public void CalculateStringSha256_WithUnicodeString_ReturnsCorrectHash()
    {
        // Arrange
        const string input = "Hello 世界 🌍";

        // Act
        var result = CryptographyUtilities.CalculateStringSha256(input);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(64, result.Length); // SHA-256 produces 64 hex characters
        Assert.True(result.All(c => char.IsDigit(c) || c is >= 'a' and <= 'f'));
    }

    [Theory]
    [InlineData("test")]
    [InlineData("Test")]
    [InlineData("TEST")]
    [InlineData("12345")]
    [InlineData("!@#$%^&*()")]
    public void CalculateStringSha256_WithVariousInputs_ReturnsConsistentHashes(string input)
    {
        // Act
        var hash1 = CryptographyUtilities.CalculateStringSha256(input);
        var hash2 = CryptographyUtilities.CalculateStringSha256(input);

        // Assert
        Assert.Equal(hash1, hash2);
        Assert.Equal(64, hash1.Length);
        Assert.True(hash1.All(c => char.IsDigit(c) || c is >= 'a' and <= 'f'));
    }

    [Fact]
    public async Task CalculateFileSha256Async_WithRealFile_ReturnsCorrectHash()
    {
        // Arrange
        var tempFile = Path.GetTempFileName();
        const string fileContent = "This is a test file for SHA-256 hashing.";
        await File.WriteAllTextAsync(tempFile, fileContent);

        try
        {
            // Act
            var fileHash = await CryptographyUtilities.CalculateFileSha256Async(tempFile);
            var stringHash = CryptographyUtilities.CalculateStringSha256(fileContent);

            // Assert
            Assert.Equal(stringHash, fileHash);
            Assert.Equal(64, fileHash.Length);
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task CalculateFileSha256Async_WithEmptyFile_ReturnsEmptyHash()
    {
        // Arrange
        var tempFile = Path.GetTempFileName();

        try
        {
            // Act
            var result = await CryptographyUtilities.CalculateFileSha256Async(tempFile);
            var expectedHash = CryptographyUtilities.CalculateStringSha256("");

            // Assert
            Assert.Equal(expectedHash, result);
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task CalculateFileSha256Async_WithLargeFile_ReturnsCorrectHash()
    {
        // Arrange
        var tempFile = Path.GetTempFileName();
        const int fileSize = 1024 * 1024; // 1MB
        var largeContent = new string('A', fileSize);
        await File.WriteAllTextAsync(tempFile, largeContent);

        try
        {
            // Act
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            var fileHash = await CryptographyUtilities.CalculateFileSha256Async(tempFile);
            stopwatch.Stop();

            // Assert
            Assert.NotNull(fileHash);
            Assert.Equal(64, fileHash.Length);
            Assert.True(stopwatch.ElapsedMilliseconds < 5000); // Should complete within 5 seconds
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task CalculateFileSha256Async_WithCancellation_ThrowsOperationCanceledException()
    {
        // Arrange
        var tempFile = Path.GetTempFileName();
        var largeContent = new string('X', 10 * 1024 * 1024); // 10MB
        await File.WriteAllTextAsync(tempFile, largeContent);

        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(1));

            // Act & Assert
            await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await CryptographyUtilities.CalculateFileSha256Async(tempFile, cts.Token);
            });
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task CalculateStreamSha256Async_WithMemoryStream_ReturnsCorrectHash()
    {
        // Arrange
        const string content = "Stream content for hashing";
        var contentBytes = System.Text.Encoding.UTF8.GetBytes(content);
        using var stream = new MemoryStream(contentBytes);

        // Act
        var streamHash = await CryptographyUtilities.CalculateStreamSha256Async(stream);
        var stringHash = CryptographyUtilities.CalculateStringSha256(content);

        // Assert
        Assert.Equal(stringHash, streamHash);
        Assert.Equal(0, stream.Position); // Stream should be reset to beginning
    }

    [Fact]
    public async Task CalculateStreamSha256Async_WithNonSeekableStream_WorksCorrectly()
    {
        // Arrange
        const string content = "Non-seekable stream content";
        var contentBytes = System.Text.Encoding.UTF8.GetBytes(content);
        using var memoryStream = new MemoryStream(contentBytes);
        await using var nonSeekableStream = new NonSeekableStreamWrapper(memoryStream);

        // Act
        var streamHash = await CryptographyUtilities.CalculateStreamSha256Async(nonSeekableStream);
        var stringHash = CryptographyUtilities.CalculateStringSha256(content);

        // Assert
        Assert.Equal(stringHash, streamHash);
    }

    [Fact]
    public async Task CalculateStreamSha256Async_PreservesStreamPosition_WhenSeekable()
    {
        // Arrange
        const string content = "Stream position test content";
        var contentBytes = System.Text.Encoding.UTF8.GetBytes(content);
        using var stream = new MemoryStream(contentBytes);
        stream.Position = 10; // Set to middle of stream

        // Act
        await CryptographyUtilities.CalculateStreamSha256Async(stream);

        // Assert
        Assert.Equal(10, stream.Position); // Position should be restored
    }

    [Fact]
    public void CalculateStringMd5_WithKnownInput_ReturnsExpectedHash()
    {
        // Arrange
        const string input = "hello world";
        const string expectedMd5 = "5eb63bbbe01eeed093cb22bb8f5acdc3";

        // Act
        #pragma warning disable CS0618 // Type or member is obsolete
        var result = CryptographyUtilities.CalculateStringMd5(input);
        #pragma warning restore CS0618

        // Assert
        Assert.Equal(expectedMd5, result);
    }

    [Fact]
    public void CalculateStringMd5_WithNullInput_ThrowsArgumentNullException()
    {
        // Act & Assert
        #pragma warning disable CS0618 // Type or member is obsolete
        Assert.Throws<ArgumentNullException>(() => CryptographyUtilities.CalculateStringMd5(null!));
        #pragma warning restore CS0618
    }

    [Fact]
    public async Task CalculateFileMd5Async_WithRealFile_ReturnsCorrectHash()
    {
        // Arrange
        var tempFile = Path.GetTempFileName();
        const string fileContent = "MD5 test content";
        await File.WriteAllTextAsync(tempFile, fileContent);

        try
        {
            // Act
            #pragma warning disable CS0618 // Type or member is obsolete
            var fileHash = await CryptographyUtilities.CalculateFileMd5Async(tempFile);
            var stringHash = CryptographyUtilities.CalculateStringMd5(fileContent);
            #pragma warning restore CS0618

            // Assert
            Assert.Equal(stringHash, fileHash);
            Assert.Equal(32, fileHash.Length); // MD5 produces 32 hex characters
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task CalculateStreamMd5Async_WithMemoryStream_ReturnsCorrectHash()
    {
        // Arrange
        const string content = "MD5 stream content";
        var contentBytes = System.Text.Encoding.UTF8.GetBytes(content);
        using var stream = new MemoryStream(contentBytes);

        // Act
        #pragma warning disable CS0618 // Type or member is obsolete
        var streamHash = await CryptographyUtilities.CalculateStreamMd5Async(stream);
        var stringHash = CryptographyUtilities.CalculateStringMd5(content);
        #pragma warning restore CS0618

        // Assert
        Assert.Equal(stringHash, streamHash);
    }

    [Fact]
    public async Task HashMethods_ProduceDifferentResults_ForSha256AndMd5()
    {
        // Arrange
        const string input = "Compare hash algorithms";
        var tempFile = Path.GetTempFileName();
        await File.WriteAllTextAsync(tempFile, input);

        try
        {
            // Act
            var sha256String = CryptographyUtilities.CalculateStringSha256(input);
            var sha256File = await CryptographyUtilities.CalculateFileSha256Async(tempFile);

            #pragma warning disable CS0618 // Type or member is obsolete
            var md5String = CryptographyUtilities.CalculateStringMd5(input);
            var md5File = await CryptographyUtilities.CalculateFileMd5Async(tempFile);
            #pragma warning restore CS0618

            // Assert
            Assert.NotEqual(sha256String, md5String);
            Assert.NotEqual(sha256File, md5File);
            Assert.Equal(sha256String, sha256File);
            Assert.Equal(md5String, md5File);
            Assert.Equal(64, sha256String.Length);
            Assert.Equal(32, md5String.Length);
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task CryptographyUtilities_AllMethods_ProduceLowercaseOutput()
    {
        // Arrange
        const string input = "Test for lowercase output";
        var tempFile = Path.GetTempFileName();
        await File.WriteAllTextAsync(tempFile, input);

        try
        {
            // Act
            var sha256String = CryptographyUtilities.CalculateStringSha256(input);
            var sha256File = await CryptographyUtilities.CalculateFileSha256Async(tempFile);

            #pragma warning disable CS0618 // Type or member is obsolete
            var md5String = CryptographyUtilities.CalculateStringMd5(input);
            var md5File = await CryptographyUtilities.CalculateFileMd5Async(tempFile);
            #pragma warning restore CS0618

            // Assert
            Assert.Equal(sha256String, sha256String.ToLowerInvariant());
            Assert.Equal(sha256File, sha256File.ToLowerInvariant());
            Assert.Equal(md5String, md5String.ToLowerInvariant());
            Assert.Equal(md5File, md5File.ToLowerInvariant());
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task CalculateFileSha256Async_WithNonExistentFile_ThrowsException()
    {
        // Arrange
        const string nonExistentFile = "non_existent_file_12345.txt";

        // Act & Assert
        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await CryptographyUtilities.CalculateFileSha256Async(nonExistentFile);
        });
    }

    [Fact]
    public async Task HashMethods_WithBinaryContent_WorkCorrectly()
    {
        // Arrange
        var binaryContent = new byte[] { 0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD };
        var tempFile = Path.GetTempFileName();
        await File.WriteAllBytesAsync(tempFile, binaryContent);

        try
        {
            // Act
            var fileHash = await CryptographyUtilities.CalculateFileSha256Async(tempFile);

            using var stream = new MemoryStream(binaryContent);
            var streamHash = await CryptographyUtilities.CalculateStreamSha256Async(stream);

            // Assert
            Assert.Equal(fileHash, streamHash);
            Assert.Equal(64, fileHash.Length);
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    // Helper class to test non-seekable streams
    private class NonSeekableStreamWrapper(Stream innerStream) : Stream
    {
        public override bool CanRead => innerStream.CanRead;
        public override bool CanSeek => false; // Always return false
        public override bool CanWrite => innerStream.CanWrite;
        public override long Length => innerStream.Length;

        public override long Position
        {
            get => innerStream.Position;
            set => throw new NotSupportedException();
        }

        public override void Flush() => innerStream.Flush();
        public override int Read(byte[] buffer, int offset, int count) => innerStream.Read(buffer, offset, count);
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => innerStream.Write(buffer, offset, count);

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                innerStream.Dispose();
            }
            base.Dispose(disposing);
        }
    }
}
