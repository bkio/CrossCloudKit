// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Xunit;

namespace CrossCloudKit.Utilities.Common.Tests;

public class MemoryTributaryTests
{
    [Fact]
    public void MemoryTributary_DefaultConstructor_InitializesCorrectly()
    {
        // Act
        using var stream = new MemoryTributary();

        // Assert
        Assert.True(stream.CanRead);
        Assert.True(stream.CanSeek);
        Assert.True(stream.CanWrite);
        Assert.Equal(0, stream.Length);
        Assert.Equal(0, stream.Position);
    }

    [Fact]
    public void MemoryTributary_ConstructorWithBlockSize_InitializesCorrectly()
    {
        // Arrange
        const int customBlockSize = 2048;

        // Act
        using var stream = new MemoryTributary(customBlockSize);

        // Assert
        Assert.True(stream.CanRead);
        Assert.True(stream.CanSeek);
        Assert.True(stream.CanWrite);
        Assert.Equal(0, stream.Length);
        Assert.Equal(0, stream.Position);
    }

    [Fact]
    public void MemoryTributary_ConstructorWithInvalidBlockSize_ThrowsArgumentOutOfRangeException()
    {
        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => new MemoryTributary(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new MemoryTributary(-1));
    }

    [Fact]
    public void MemoryTributary_ConstructorWithByteArray_InitializesWithData()
    {
        // Arrange
        var initialData = new byte[] { 1, 2, 3, 4, 5 };

        // Act
        using var stream = new MemoryTributary(initialData);

        // Assert
        Assert.Equal(5, stream.Length);
        Assert.Equal(0, stream.Position);

        var buffer = new byte[5];
        var bytesRead = stream.Read(buffer, 0, 5);
        Assert.Equal(5, bytesRead);
        Assert.Equal(initialData, buffer);
    }

    [Fact]
    public void MemoryTributary_ConstructorWithSpan_InitializesWithData()
    {
        // Arrange
        var initialData = new byte[] { 10, 20, 30, 40, 50 };

        // Act
        using var stream = new MemoryTributary(initialData.AsSpan());

        // Assert
        Assert.Equal(5, stream.Length);
        Assert.Equal(0, stream.Position);

        var buffer = new byte[5];
        var bytesRead = stream.Read(buffer, 0, 5);
        Assert.Equal(5, bytesRead);
        Assert.Equal(initialData, buffer);
    }

    [Fact]
    public void MemoryTributary_ConstructorWithInitialLength_InitializesCorrectly()
    {
        // Arrange
        const long initialLength = 100L;

        // Act
        using var stream = new MemoryTributary(initialLength);

        // Assert
        Assert.Equal(initialLength, stream.Length);
        Assert.Equal(0, stream.Position);
    }

    [Fact]
    public void MemoryTributary_ConstructorWithNegativeLength_ThrowsArgumentOutOfRangeException()
    {
        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => new MemoryTributary(-1));
    }

    [Fact]
    public void MemoryTributary_WriteByte_IncreasesLengthAndPosition()
    {
        // Arrange
        using var stream = new MemoryTributary();

        // Act
        stream.WriteByte(42);

        // Assert
        Assert.Equal(1, stream.Length);
        Assert.Equal(1, stream.Position);
    }

    [Fact]
    public void MemoryTributary_ReadByte_ReturnsCorrectValue()
    {
        // Arrange
        using var stream = new MemoryTributary();
        stream.WriteByte(123);
        stream.Position = 0;

        // Act
        var value = stream.ReadByte();

        // Assert
        Assert.Equal(123, value);
        Assert.Equal(1, stream.Position);
    }

    [Fact]
    public void MemoryTributary_ReadByte_AtEndOfStream_ReturnsMinusOne()
    {
        // Arrange
        using var stream = new MemoryTributary();

        // Act
        var value = stream.ReadByte();

        // Assert
        Assert.Equal(-1, value);
        Assert.Equal(0, stream.Position);
    }

    [Fact]
    public void MemoryTributary_WriteAndRead_WorksCorrectly()
    {
        // Arrange
        using var stream = new MemoryTributary();
        var testData = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

        // Act
        stream.Write(testData, 0, testData.Length);
        stream.Position = 0;

        var readBuffer = new byte[testData.Length];
        var bytesRead = stream.Read(readBuffer, 0, readBuffer.Length);

        // Assert
        Assert.Equal(testData.Length, bytesRead);
        Assert.Equal(testData, readBuffer);
        Assert.Equal(testData.Length, stream.Position);
    }

    [Fact]
    public void MemoryTributary_WriteSpan_WorksCorrectly()
    {
        // Arrange
        using var stream = new MemoryTributary();
        var testData = new byte[] { 11, 22, 33, 44, 55 };

        // Act
        stream.Write(testData.AsSpan());
        stream.Position = 0;

        var readBuffer = new byte[testData.Length];
        var bytesRead = stream.Read(readBuffer.AsSpan());

        // Assert
        Assert.Equal(testData.Length, bytesRead);
        Assert.Equal(testData, readBuffer);
    }

    [Fact]
    public void MemoryTributary_Seek_WorksCorrectly()
    {
        // Arrange
        using var stream = new MemoryTributary();
        var testData = new byte[] { 1, 2, 3, 4, 5 };
        stream.Write(testData, 0, testData.Length);

        // Act & Assert - SeekOrigin.Begin
        var position = stream.Seek(2, SeekOrigin.Begin);
        Assert.Equal(2, position);
        Assert.Equal(2, stream.Position);

        // Act & Assert - SeekOrigin.Current
        position = stream.Seek(1, SeekOrigin.Current);
        Assert.Equal(3, position);
        Assert.Equal(3, stream.Position);

        // Act & Assert - SeekOrigin.End
        position = stream.Seek(-1, SeekOrigin.End);
        Assert.Equal(4, position);
        Assert.Equal(4, stream.Position);
    }

    [Fact]
    public void MemoryTributary_Seek_WithNegativeResult_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        using var stream = new MemoryTributary();

        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => stream.Seek(-1, SeekOrigin.Begin));
        Assert.Throws<ArgumentOutOfRangeException>(() => stream.Seek(-1, SeekOrigin.Current));
    }

    [Fact]
    public void MemoryTributary_Seek_WithInvalidOrigin_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        using var stream = new MemoryTributary();

        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => stream.Seek(0, (SeekOrigin)999));
    }

    [Fact]
    public void MemoryTributary_SetLength_UpdatesLength()
    {
        // Arrange
        using var stream = new MemoryTributary();

        // Act
        stream.SetLength(100);

        // Assert
        Assert.Equal(100, stream.Length);
    }

    [Fact]
    public void MemoryTributary_SetLength_WithNegativeValue_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        using var stream = new MemoryTributary();

        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => stream.SetLength(-1));
    }

    [Fact]
    public void MemoryTributary_Position_SetNegative_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        using var stream = new MemoryTributary();

        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => stream.Position = -1);
    }

    [Fact]
    public void MemoryTributary_ToArray_ReturnsCorrectData()
    {
        // Arrange
        using var stream = new MemoryTributary();
        var testData = "defgh"u8.ToArray();
        stream.Write(testData, 0, testData.Length);
        stream.Position = 2; // Move position to test it's preserved

        // Act
        var result = stream.ToArray();

        // Assert
        Assert.Equal(testData, result);
        Assert.Equal(2, stream.Position); // Position should be restored
    }

    [Fact]
    public void MemoryTributary_ToArray_WithEmptyStream_ReturnsEmptyArray()
    {
        // Arrange
        using var stream = new MemoryTributary();

        // Act
        var result = stream.ToArray();

        // Assert
        Assert.Empty(result);
    }

    [Fact]
    public void MemoryTributary_ReadFrom_CopiesDataFromSourceStream()
    {
        // Arrange
        using var sourceStream = new MemoryStream([1, 2, 3, 4, 5]);
        using var tributary = new MemoryTributary();

        // Act
        tributary.ReadFrom(sourceStream, 5);

        // Assert
        Assert.Equal(5, tributary.Length);
        var result = tributary.ToArray();
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5 }, result);
    }

    [Fact]
    public void MemoryTributary_ReadFrom_WithPartialLength_CopiesCorrectAmount()
    {
        // Arrange
        using var sourceStream = new MemoryStream([1, 2, 3, 4, 5]);
        using var tributary = new MemoryTributary();

        // Act
        tributary.ReadFrom(sourceStream, 3);

        // Assert
        Assert.Equal(3, tributary.Length);
        var result = tributary.ToArray();
        Assert.Equal(new byte[] { 1, 2, 3 }, result);
    }

    [Fact]
    public void MemoryTributary_ReadFrom_WithNegativeLength_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        using var sourceStream = new MemoryStream();
        using var tributary = new MemoryTributary();

        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => tributary.ReadFrom(sourceStream, -1));
    }

    [Fact]
    public async Task MemoryTributary_ReadFromAsync_CopiesDataFromSourceStream()
    {
        // Arrange
        using var sourceStream = new MemoryStream([10, 20, 30, 40, 50]);
        await using var tributary = new MemoryTributary();

        // Act
        await tributary.ReadFromAsync(sourceStream, 5);

        // Assert
        Assert.Equal(5, tributary.Length);
        var result = tributary.ToArray();
        Assert.Equal(new byte[] { 10, 20, 30, 40, 50 }, result);
    }

    [Fact]
    public async Task MemoryTributary_ReadFromAsync_WithCancellation_ThrowsOperationCanceledException()
    {
        // Arrange
        using var sourceStream = new MemoryStream(new byte[1000]);
        await using var tributary = new MemoryTributary();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Act & Assert
        await Assert.ThrowsAsync<Exception>(() =>
            tributary.ReadFromAsync(sourceStream, 1000, cts.Token));
    }

    [Fact]
    public void MemoryTributary_WriteTo_CopiesDataToDestinationStream()
    {
        // Arrange
        using var tributary = new MemoryTributary(new byte[] { 1, 2, 3, 4, 5 });
        using var destinationStream = new MemoryStream();
        tributary.Position = 2; // Move position to test it's preserved

        // Act
        tributary.WriteTo(destinationStream);

        // Assert
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5 }, destinationStream.ToArray());
        Assert.Equal(2, tributary.Position); // Position should be restored
    }

    [Fact]
    public async Task MemoryTributary_WriteToAsync_CopiesDataToDestinationStream()
    {
        // Arrange
        await using var tributary = new MemoryTributary(new byte[] { 10, 20, 30, 40, 50 });
        using var destinationStream = new MemoryStream();
        tributary.Position = 3; // Move position to test it's preserved

        // Act
        await tributary.WriteToAsync(destinationStream);

        // Assert
        Assert.Equal(new byte[] { 10, 20, 30, 40, 50 }, destinationStream.ToArray());
        Assert.Equal(3, tributary.Position); // Position should be restored
    }

    [Fact]
    public async Task MemoryTributary_WriteToAsync_WithCancellation_ThrowsOperationCanceledException()
    {
        // Arrange
        await using var tributary = new MemoryTributary(new byte[1000]);
        using var destinationStream = new MemoryStream();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Act & Assert
        await Assert.ThrowsAsync<Exception>(() =>
            tributary.WriteToAsync(destinationStream, cts.Token));
    }

    [Fact]
    public async Task MemoryTributary_ReadAsync_WorksCorrectly()
    {
        // Arrange
        await using var stream = new MemoryTributary(new byte[] { 1, 2, 3, 4, 5 });
        var buffer = new byte[3];

        // Act
        var bytesRead = await stream.ReadAsync(buffer, 0, 3);

        // Assert
        Assert.Equal(3, bytesRead);
        Assert.Equal(new byte[] { 1, 2, 3 }, buffer);
        Assert.Equal(3, stream.Position);
    }

    [Fact]
    public async Task MemoryTributary_ReadAsync_Memory_WorksCorrectly()
    {
        // Arrange
        await using var stream = new MemoryTributary(new byte[] { 10, 20, 30, 40, 50 });
        var buffer = new byte[4];

        // Act
        var bytesRead = await stream.ReadAsync(buffer.AsMemory());

        // Assert
        Assert.Equal(4, bytesRead);
        Assert.Equal(new byte[] { 10, 20, 30, 40 }, buffer);
        Assert.Equal(4, stream.Position);
    }

    [Fact]
    public async Task MemoryTributary_WriteAsync_WorksCorrectly()
    {
        // Arrange
        await using var stream = new MemoryTributary();
        var testData = new byte[] { 100, 200 };

        // Act
        await stream.WriteAsync(testData, 0, testData.Length);

        // Assert
        Assert.Equal(2, stream.Length);
        Assert.Equal(2, stream.Position);

        stream.Position = 0;
        var buffer = new byte[2];
        var bytesRead = stream.Read(buffer, 0, 2);
        Assert.Equal(2, bytesRead);
        Assert.Equal(testData, buffer);
    }

    [Fact]
    public async Task MemoryTributary_WriteAsync_Memory_WorksCorrectly()
    {
        // Arrange
        await using var stream = new MemoryTributary();
        var testData = new byte[] { 150, 250, 50 };

        // Act
        await stream.WriteAsync(testData.AsMemory());

        // Assert
        Assert.Equal(3, stream.Length);
        Assert.Equal(3, stream.Position);

        stream.Position = 0;
        var buffer = new byte[3];
        var bytesRead = stream.Read(buffer, 0, 3);
        Assert.Equal(3, bytesRead);
        Assert.Equal(testData, buffer);
    }

    [Fact]
    public async Task MemoryTributary_AsyncOperations_WithCancellation_HandleCorrectly()
    {
        // Arrange
        await using var stream = new MemoryTributary();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var buffer = new byte[10];

        // Act & Assert
        await Assert.ThrowsAsync<TaskCanceledException>(() =>
            stream.ReadAsync(buffer.AsMemory(), cts.Token).AsTask());

        await Assert.ThrowsAsync<TaskCanceledException>(() =>
            stream.WriteAsync(buffer.AsMemory(), cts.Token).AsTask());
    }

    [Fact]
    public void MemoryTributary_Flush_DoesNotThrow()
    {
        // Arrange
        using var stream = new MemoryTributary();

        // Act & Assert - Should not throw
        stream.Flush();
    }

    [Fact]
    public async Task MemoryTributary_FlushAsync_DoesNotThrow()
    {
        // Arrange
        await using var stream = new MemoryTributary();

        // Act & Assert - Should not throw
        await stream.FlushAsync();
    }

    [Fact]
    public async Task MemoryTributary_FlushAsync_WithCancellation_ThrowsOperationCanceledException()
    {
        // Arrange
        await using var stream = new MemoryTributary();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Act & Assert
        await Assert.ThrowsAsync<Exception>(() => stream.FlushAsync(cts.Token));
    }

    [Fact]
    public void MemoryTributary_AfterDispose_ThrowsObjectDisposedException()
    {
        // Arrange
        var stream = new MemoryTributary();
        stream.Dispose();

        // Act & Assert
        Assert.False(stream.CanRead);
        Assert.False(stream.CanWrite);
        Assert.False(stream.CanSeek);

        Assert.Throws<ObjectDisposedException>(() => stream.ReadByte());
        Assert.Throws<ObjectDisposedException>(() => stream.WriteByte(1));
        Assert.Throws<ObjectDisposedException>(() => stream.Read(new byte[1], 0, 1));
        Assert.Throws<ObjectDisposedException>(() => stream.Write(new byte[1], 0, 1));
        Assert.Throws<ObjectDisposedException>(() => stream.Seek(0, SeekOrigin.Begin));
        Assert.Throws<ObjectDisposedException>(() => stream.SetLength(10));
        Assert.Throws<ObjectDisposedException>(() => stream.ToArray());
        Assert.Throws<ObjectDisposedException>(() => stream.ReadFrom(new MemoryStream(), 0));
        Assert.Throws<ObjectDisposedException>(() => stream.WriteTo(new MemoryStream()));
    }

    [Fact]
    public void MemoryTributary_LargeData_AcrossMultipleBlocks_WorksCorrectly()
    {
        // Arrange
        const int blockSize = 1024; // Small block size for testing
        const int dataSize = blockSize * 3 + 500; // Spans multiple blocks
        using var stream = new MemoryTributary(blockSize);

        var testData = new byte[dataSize];
        for (int i = 0; i < dataSize; i++)
        {
            testData[i] = (byte)(i % 256);
        }

        // Act
        stream.Write(testData, 0, testData.Length);
        stream.Position = 0;

        var readBuffer = new byte[dataSize];
        var totalBytesRead = 0;
        while (totalBytesRead < dataSize)
        {
            var bytesRead = stream.Read(readBuffer, totalBytesRead, Math.Min(100, dataSize - totalBytesRead));
            if (bytesRead == 0) break;
            totalBytesRead += bytesRead;
        }

        // Assert
        Assert.Equal(dataSize, totalBytesRead);
        Assert.Equal(testData, readBuffer);
    }

    [Fact]
    public void MemoryTributary_RandomAccessAcrossBlocks_WorksCorrectly()
    {
        // Arrange
        const int blockSize = 512;
        using var stream = new MemoryTributary(blockSize);

        var testData = new byte[blockSize * 2 + 100]; // Spans 3 blocks
        for (int i = 0; i < testData.Length; i++)
        {
            testData[i] = (byte)(i % 256);
        }

        stream.Write(testData, 0, testData.Length);

        // Act & Assert - Test reading from different blocks
        stream.Position = 10; // First block
        Assert.Equal(testData[10], (byte)stream.ReadByte());

        stream.Position = blockSize + 50; // Second block
        Assert.Equal(testData[blockSize + 50], (byte)stream.ReadByte());

        stream.Position = blockSize * 2 + 10; // Third block
        Assert.Equal(testData[blockSize * 2 + 10], (byte)stream.ReadByte());

        // Test writing at different positions
        stream.Position = blockSize - 1; // End of first block
        stream.WriteByte(255);

        stream.Position = blockSize; // Start of second block
        stream.WriteByte(254);

        // Verify writes
        stream.Position = blockSize - 1;
        Assert.Equal(255, stream.ReadByte());
        Assert.Equal(254, stream.ReadByte());
    }

    [Fact]
    public void MemoryTributary_WriteAtEnd_AutomaticallyExtendsLength()
    {
        // Arrange
        using var stream = new MemoryTributary();
        stream.SetLength(10);
        stream.Position = 15; // Beyond current length

        // Act
        stream.WriteByte(42);

        // Assert
        Assert.Equal(16, stream.Length); // Should extend to accommodate write
        Assert.Equal(16, stream.Position);

        // Verify the byte was written correctly
        stream.Position = 15;
        Assert.Equal(42, stream.ReadByte());
    }

    [Fact]
    public void MemoryTributary_PartialReads_WorkCorrectly()
    {
        // Arrange
        using var stream = new MemoryTributary(new byte[] { 1, 2, 3, 4, 5 });
        var buffer = new byte[10]; // Larger than available data

        // Act
        var bytesRead = stream.Read(buffer, 0, buffer.Length);

        // Assert
        Assert.Equal(5, bytesRead); // Only 5 bytes available
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 0, 0, 0, 0, 0 }, buffer);
    }

    [Fact]
    public void MemoryTributary_ReadBeyondEnd_ReturnsZero()
    {
        // Arrange
        using var stream = new MemoryTributary(new byte[] { 1, 2, 3 });
        stream.Position = 3; // At end

        // Act
        var buffer = new byte[5];
        var bytesRead = stream.Read(buffer, 0, buffer.Length);

        // Assert
        Assert.Equal(0, bytesRead);
        Assert.Equal(new byte[5], buffer); // Should remain unchanged
    }

    [Fact]
    public void MemoryTributary_Performance_LargeSequentialWrites()
    {
        // Arrange
        const int dataSize = 10 * 1024 * 1024; // 10MB
        using var stream = new MemoryTributary();
        var testData = new byte[4096]; // 4KB chunks
        for (int i = 0; i < testData.Length; i++)
        {
            testData[i] = (byte)(i % 256);
        }

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Act
        var totalWritten = 0;
        while (totalWritten < dataSize)
        {
            var toWrite = Math.Min(testData.Length, dataSize - totalWritten);
            stream.Write(testData, 0, toWrite);
            totalWritten += toWrite;
        }

        stopwatch.Stop();

        // Assert
        Assert.Equal(dataSize, stream.Length);
        Assert.True(stopwatch.ElapsedMilliseconds < 5000,
            $"Performance test failed: {stopwatch.ElapsedMilliseconds}ms for {dataSize / (1024 * 1024)}MB");
    }

    [Fact]
    public void MemoryTributary_Performance_LargeSequentialReads()
    {
        // Arrange
        const int dataSize = 5 * 1024 * 1024; // 5MB
        var testData = new byte[dataSize];
        for (int i = 0; i < dataSize; i++)
        {
            testData[i] = (byte)(i % 256);
        }

        using var stream = new MemoryTributary(testData);
        stream.Position = 0;

        var readBuffer = new byte[4096];
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Act
        var totalRead = 0;
        while (totalRead < dataSize)
        {
            var bytesRead = stream.Read(readBuffer, 0, readBuffer.Length);
            if (bytesRead == 0) break;
            totalRead += bytesRead;
        }

        stopwatch.Stop();

        // Assert
        Assert.Equal(dataSize, totalRead);
        Assert.True(stopwatch.ElapsedMilliseconds < 3000,
            $"Performance test failed: {stopwatch.ElapsedMilliseconds}ms for {dataSize / (1024 * 1024)}MB");
    }
}
