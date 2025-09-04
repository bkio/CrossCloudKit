// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Buffers;
using System.Runtime.CompilerServices;
// ReSharper disable MemberCanBePrivate.Global

namespace CrossCloudKit.Utilities.Common;

/// <summary>
/// MemoryTributary is a re-implementation of MemoryStream that uses a dynamic list of byte arrays as a backing store,
/// instead of a single byte array, avoiding allocation failures for large streams that require contiguous memory.
/// </summary>
public sealed class MemoryTributary : Stream
{
    private const int DefaultBlockSize = 1_048_576; // 1MB
    private const int DefaultBufferSize = 4096;

    private readonly int _blockSize;
    private readonly List<byte[]> _blocks;
    private long _length;
    private long _position;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="MemoryTributary"/> class with default block size.
    /// </summary>
    public MemoryTributary() : this(DefaultBlockSize)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MemoryTributary"/> class with a specific block size.
    /// </summary>
    /// <param name="blockSize">The size of each memory block</param>
    public MemoryTributary(int blockSize)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(blockSize, 0);

        _blockSize = blockSize;
        _blocks = [];
        _position = 0;
        _length = 0;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MemoryTributary"/> class with initial data.
    /// </summary>
    /// <param name="source">The initial data to populate the stream with</param>
    /// <param name="blockSize">The size of each memory block (default: 1MB)</param>
    public MemoryTributary(ReadOnlySpan<byte> source, int blockSize = DefaultBlockSize) : this(blockSize)
    {
        Write(source);
        _position = 0;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MemoryTributary"/> class with initial data.
    /// </summary>
    /// <param name="source">The initial data to populate the stream with</param>
    /// <param name="blockSize">The size of each memory block (default: 1MB)</param>
    public MemoryTributary(byte[] source, int blockSize = DefaultBlockSize) : this(source.AsSpan(), blockSize)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MemoryTributary"/> class with a specified initial length.
    /// </summary>
    /// <param name="initialLength">The initial length of the stream</param>
    /// <param name="blockSize">The size of each memory block (default: 1MB)</param>
    public MemoryTributary(long initialLength, int blockSize = DefaultBlockSize) : this(blockSize)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(initialLength);

        SetLength(initialLength);
        _position = initialLength;
        _ = GetBlock(BlockId); // Ensure memory allocation
        _position = 0;
    }

    public override bool CanRead => !_disposed;
    public override bool CanSeek => !_disposed;
    public override bool CanWrite => !_disposed;
    public override long Length => _length;

    public override long Position
    {
        get => _position;
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            _position = value;
        }
    }

    /// <summary>
    /// The id of the block currently addressed by Position
    /// </summary>
    private long BlockId => _position / _blockSize;

    /// <summary>
    /// The offset of the byte currently addressed by Position, into the block that contains it
    /// </summary>
    private int BlockOffset => (int)(_position % _blockSize);

    /// <summary>
    /// Gets the block of memory currently addressed by Position, allocating if necessary
    /// </summary>
    private byte[] GetBlock(long blockId)
    {
        while (_blocks.Count <= blockId)
        {
            _blocks.Add(GC.AllocateUninitializedArray<byte>(_blockSize));
        }
        return _blocks[(int)blockId];
    }

    public override void Flush()
    {
        // No-op for memory stream
    }

    public override Task FlushAsync(CancellationToken cancellationToken)
    {
        return cancellationToken.IsCancellationRequested
            ? Task.FromCanceled(cancellationToken)
            : Task.CompletedTask;
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        return Read(buffer.AsSpan(offset, count));
    }

    public override int Read(Span<byte> buffer)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var remaining = Math.Min(buffer.Length, _length - _position);
        if (remaining <= 0) return 0;

        var totalRead = 0;
        var span = buffer[..(int)remaining];

        while (!span.IsEmpty)
        {
            var currentBlock = GetBlock(BlockId);
            var blockOffset = BlockOffset;
            var bytesToRead = Math.Min(span.Length, _blockSize - blockOffset);

            currentBlock.AsSpan(blockOffset, bytesToRead).CopyTo(span);

            totalRead += bytesToRead;
            _position += bytesToRead;
            span = span[bytesToRead..];
        }

        return totalRead;
    }

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        return await ReadAsync(buffer.AsMemory(offset, count), cancellationToken).ConfigureAwait(false);
    }

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        return cancellationToken.IsCancellationRequested
            ? ValueTask.FromCanceled<int>(cancellationToken)
            : ValueTask.FromResult(Read(buffer.Span));
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var newPosition = origin switch
        {
            SeekOrigin.Begin => offset,
            SeekOrigin.Current => _position + offset,
            SeekOrigin.End => _length + offset,
            _ => throw new ArgumentOutOfRangeException(nameof(origin))
        };

        ArgumentOutOfRangeException.ThrowIfNegative(newPosition);
        _position = newPosition;
        return _position;
    }

    public override void SetLength(long value)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentOutOfRangeException.ThrowIfNegative(value);

        _length = value;
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        Write(buffer.AsSpan(offset, count));
    }

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (buffer.IsEmpty) return;

        var initialPosition = _position;
        try
        {
            var remaining = buffer;
            while (!remaining.IsEmpty)
            {
                var currentBlock = GetBlock(BlockId);
                var blockOffset = BlockOffset;
                var bytesToWrite = Math.Min(remaining.Length, _blockSize - blockOffset);

                EnsureCapacity(_position + bytesToWrite);
                remaining[..bytesToWrite].CopyTo(currentBlock.AsSpan(blockOffset));

                _position += bytesToWrite;
                remaining = remaining[bytesToWrite..];
            }
        }
        catch
        {
            _position = initialPosition;
            throw;
        }
    }

    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        await WriteAsync(buffer.AsMemory(offset, count), cancellationToken).ConfigureAwait(false);
    }

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromCanceled(cancellationToken);

        try
        {
            Write(buffer.Span);
            return ValueTask.CompletedTask;
        }
        catch (Exception ex)
        {
            return ValueTask.FromException(ex);
        }
    }

    public override int ReadByte()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_position >= _length) return -1;

        var value = GetBlock(BlockId)[BlockOffset];
        _position++;
        return value;
    }

    public override void WriteByte(byte value)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        EnsureCapacity(_position + 1);
        GetBlock(BlockId)[BlockOffset] = value;
        _position++;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureCapacity(long intendedLength)
    {
        if (intendedLength > _length)
            _length = intendedLength;
    }

    protected override void Dispose(bool disposing)
    {
        if (!_disposed && disposing)
        {
            _blocks.Clear();
            _disposed = true;
        }
        base.Dispose(disposing);
    }

    /// <summary>
    /// Returns the entire content of the stream as a byte array.
    /// </summary>
    /// <remarks>
    /// This method may fail if the stream is too large to fit in a single byte array.
    /// Consider using methods that operate on streams directly when possible.
    /// </remarks>
    /// <returns>A byte array containing the current data in the stream</returns>
    public byte[] ToArray()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_length == 0) return [];
        if (_length > Array.MaxLength) throw new OutOfMemoryException("Stream is too large to convert to array");

        var originalPosition = _position;
        try
        {
            var result = GC.AllocateUninitializedArray<byte>((int)_length);
            _position = 0;
            _ = Read(result);
            return result;
        }
        finally
        {
            _position = originalPosition;
        }
    }

    /// <summary>
    /// Reads data from the specified source stream into this instance at the current position.
    /// </summary>
    /// <param name="source">The stream containing the data to copy</param>
    /// <param name="length">The number of bytes to copy</param>
    public void ReadFrom(Stream source, long length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        ObjectDisposedException.ThrowIf(_disposed, this);

        var buffer = ArrayPool<byte>.Shared.Rent(DefaultBufferSize);
        try
        {
            while (length > 0)
            {
                var toRead = (int)Math.Min(buffer.Length, length);
                var bytesRead = source.Read(buffer, 0, toRead);
                if (bytesRead == 0) break;

                Write(buffer, 0, bytesRead);
                length -= bytesRead;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// Asynchronously reads data from the specified source stream into this instance at the current position.
    /// </summary>
    /// <param name="source">The stream containing the data to copy</param>
    /// <param name="length">The number of bytes to copy</param>
    /// <param name="cancellationToken">A cancellation token to observe</param>
    public async Task ReadFromAsync(Stream source, long length, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        ObjectDisposedException.ThrowIf(_disposed, this);

        var buffer = ArrayPool<byte>.Shared.Rent(DefaultBufferSize);
        try
        {
            while (length > 0)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var toRead = (int)Math.Min(buffer.Length, length);
                var bytesRead = await source.ReadAsync(buffer.AsMemory(0, toRead), cancellationToken).ConfigureAwait(false);
                if (bytesRead == 0) break;

                await WriteAsync(buffer.AsMemory(0, bytesRead), cancellationToken).ConfigureAwait(false);
                length -= bytesRead;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// Writes the entire contents of this stream to the specified destination stream.
    /// </summary>
    /// <param name="destination">The stream to write the content to</param>
    public void WriteTo(Stream destination)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var originalPosition = _position;
        try
        {
            _position = 0;
            CopyTo(destination);
        }
        finally
        {
            _position = originalPosition;
        }
    }

    /// <summary>
    /// Asynchronously writes the entire contents of this stream to the specified destination stream.
    /// </summary>
    /// <param name="destination">The stream to write the content to</param>
    /// <param name="cancellationToken">A cancellation token to observe</param>
    public async Task WriteToAsync(Stream destination, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var originalPosition = _position;
        try
        {
            _position = 0;
            await CopyToAsync(destination, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _position = originalPosition;
        }
    }
}
