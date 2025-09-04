// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using FluentAssertions;
using Xunit;

namespace CrossCloudKit.Utilities.Common.Tests;

public class StreamUtilitiesTests
{
    private readonly byte[] _testData = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    private readonly byte[] _emptyData = [];

    [Fact]
    public async Task ReadAllBytesAsync_WithMemoryStream_ShouldReturnAllBytes()
    {
        // Arrange
        using var stream = new MemoryStream(_testData);

        // Act
        var result = await StreamUtilities.ReadAllBytesAsync(stream);

        // Assert
        result.Should().BeEquivalentTo(_testData);
    }

    [Fact]
    public async Task ReadAllBytesAsync_WithMemoryStreamAtNonZeroPosition_ShouldReturnAllBytes()
    {
        // Arrange
        using var stream = new MemoryStream(_testData);
        stream.Position = 5; // Move to middle of stream

        // Act
        var result = await StreamUtilities.ReadAllBytesAsync(stream);

        // Assert
        result.Should().BeEquivalentTo(_testData);
    }

    [Fact]
    public async Task ReadAllBytesAsync_WithMemoryTributary_ShouldReturnAllBytes()
    {
        // Arrange
        await using var stream = new MemoryTributary(_testData);

        // Act
        var result = await StreamUtilities.ReadAllBytesAsync(stream);

        // Assert
        result.Should().BeEquivalentTo(_testData);
    }

    [Fact]
    public async Task ReadAllBytesAsync_WithEmptyMemoryStream_ShouldReturnEmptyArray()
    {
        // Arrange
        using var stream = new MemoryStream(_emptyData);

        // Act
        var result = await StreamUtilities.ReadAllBytesAsync(stream);

        // Assert
        result.Should().BeEmpty();
    }

    [Fact]
    public async Task ReadAllBytesAsync_WithCancellationToken_ShouldRespectCancellation()
    {
        // Arrange
        await using var stream = new MemoryTributary(_testData);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Act & Assert
        await Assert.ThrowsAsync<TaskCanceledException>(
            async () => await StreamUtilities.ReadAllBytesAsync(stream, cts.Token));
    }

    [Fact]
    public async Task ReadAllBytesAsync_WithNonSeekableStream_ShouldReturnAllBytes()
    {
        // Arrange
        using var memoryStream = new MemoryStream(_testData);
        await using var nonSeekableStream = new NonSeekableStreamWrapper(memoryStream);

        // Act
        var result = await StreamUtilities.ReadAllBytesAsync(nonSeekableStream);

        // Assert
        result.Should().BeEquivalentTo(_testData);
    }

    [Fact]
    public void ReadAllBytes_WithMemoryStream_ShouldReturnAllBytesAndPreservePosition()
    {
        // Arrange
        using var stream = new MemoryStream(_testData);
        stream.Position = 3;
        var originalPosition = stream.Position;

        // Act
        var result = StreamUtilities.ReadAllBytes(stream);

        // Assert
        result.Should().BeEquivalentTo(_testData);
        stream.Position.Should().Be(originalPosition);
    }

    [Fact]
    public void ReadAllBytes_WithMemoryTributary_ShouldReturnAllBytesAndPreservePosition()
    {
        // Arrange
        using var stream = new MemoryTributary(_testData);
        stream.Position = 7;
        var originalPosition = stream.Position;

        // Act
        var result = StreamUtilities.ReadAllBytes(stream);

        // Assert
        result.Should().BeEquivalentTo(_testData);
        stream.Position.Should().Be(originalPosition);
    }

    [Fact]
    public void ReadAllBytes_WithEmptyStream_ShouldReturnEmptyArray()
    {
        // Arrange
        using var stream = new MemoryStream(_emptyData);

        // Act
        var result = StreamUtilities.ReadAllBytes(stream);

        // Assert
        result.Should().BeEmpty();
    }

    [Fact]
    public void ReadAllBytes_WithNonSeekableStream_ShouldReturnAllBytes()
    {
        // Arrange
        using var memoryStream = new MemoryStream(_testData);
        using var nonSeekableStream = new NonSeekableStreamWrapper(memoryStream);

        // Act
        var result = StreamUtilities.ReadAllBytes(nonSeekableStream);

        // Assert
        result.Should().BeEquivalentTo(_testData);
    }

    [Fact]
    public void ReadAllBytes_WithStreamAtEnd_ShouldReturnAllBytesAndPreservePosition()
    {
        // Arrange
        using var stream = new MemoryStream(_testData);
        stream.Position = stream.Length; // Move to end
        var originalPosition = stream.Position;

        // Act
        var result = StreamUtilities.ReadAllBytes(stream);

        // Assert
        result.Should().BeEquivalentTo(_testData);
        stream.Position.Should().Be(originalPosition);
    }

    [Fact]
    public void ReadAllBytes_WithLargeStream_ShouldHandleCorrectly()
    {
        // Arrange
        var largeData = new byte[1_000_000];
        Random.Shared.NextBytes(largeData);
        using var stream = new MemoryTributary(largeData);
        stream.Position = 50000; // Set position somewhere in the middle

        // Act
        var result = StreamUtilities.ReadAllBytes(stream);

        // Assert
        result.Should().BeEquivalentTo(largeData);
        stream.Position.Should().Be(50000);
    }

    [Fact]
    public async Task ReadAllBytesAsync_WithLargeStream_ShouldHandleCorrectly()
    {
        // Arrange
        var largeData = new byte[1_000_000];
        Random.Shared.NextBytes(largeData);
        await using var stream = new MemoryTributary(largeData);

        // Act
        var result = await StreamUtilities.ReadAllBytesAsync(stream);

        // Assert
        result.Should().BeEquivalentTo(largeData);
    }

    /// <summary>
    /// A wrapper that makes a seekable stream appear non-seekable for testing purposes
    /// </summary>
    private class NonSeekableStreamWrapper(Stream innerStream) : Stream
    {
        public override bool CanRead => innerStream.CanRead;
        public override bool CanSeek => false; // Override to false
        public override bool CanWrite => innerStream.CanWrite;
        public override long Length => innerStream.Length;
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Flush() => innerStream.Flush();
        public override int Read(byte[] buffer, int offset, int count) => innerStream.Read(buffer, offset, count);
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => innerStream.Write(buffer, offset, count);
        public override async Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
            => await innerStream.CopyToAsync(destination, bufferSize, cancellationToken);

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                innerStream.Dispose();
            base.Dispose(disposing);
        }
    }
}
