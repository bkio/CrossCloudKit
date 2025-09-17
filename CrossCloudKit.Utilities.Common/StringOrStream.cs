// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Text;
// ReSharper disable MemberCanBePrivate.Global

namespace CrossCloudKit.Utilities.Common;

/// <summary>
/// Represents the content type of a StringOrStream instance.
/// </summary>
public enum StringOrStreamKind
{
    /// <summary>
    /// The content is stored as a string.
    /// </summary>
    String,

    /// <summary>
    /// The content is stored as a stream.
    /// </summary>
    Stream
}

/// <summary>
/// Represents a discriminated union that can contain either a string or a stream.
/// This type provides safe, type-checked access to the underlying content and supports
/// both synchronous and asynchronous operations.
/// </summary>
public sealed class StringOrStream : IDisposable, IAsyncDisposable
{
    private readonly string? _stringValue;
    private readonly Stream? _streamValue;
    private readonly long _streamLength;
    private readonly Func<ValueTask>? _cleanupAction;
    private readonly Encoding _encoding;
    private bool _disposed;

    /// <summary>
    /// Gets the kind of content stored in this instance.
    /// </summary>
    public StringOrStreamKind Kind { get; }

    /// <summary>
    /// Gets the length of the content.
    /// For strings, this returns the character count.
    /// For streams, this returns the byte count.
    /// </summary>
    public long Length => Kind switch
    {
        StringOrStreamKind.String => _stringValue.NotNull().Length,
        StringOrStreamKind.Stream => _streamLength,
        _ => throw new InvalidOperationException($"Unknown content kind: {Kind}")
    };

    /// <summary>
    /// Creates a StringOrStream containing string content.
    /// </summary>
    /// <param name="content">The string content</param>
    /// <param name="encoding">The encoding to use for stream conversions (default: UTF-8)</param>
    /// <exception cref="ArgumentNullException">Thrown when content is null</exception>
    public StringOrStream(string content, Encoding? encoding = null)
    {
        Kind = StringOrStreamKind.String;
        _stringValue = content;
        _encoding = encoding ?? Encoding.UTF8;
    }

    /// <summary>
    /// Creates a StringOrStream containing stream content.
    /// </summary>
    /// <param name="stream">The stream content</param>
    /// <param name="length">The length of meaningful data in the stream</param>
    /// <param name="encoding">The encoding to use for string conversions (default: UTF-8)</param>
    /// <exception cref="ArgumentNullException">Thrown when stream is null</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when length is negative</exception>
    public StringOrStream(Stream stream, long length, Encoding? encoding = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);

        Kind = StringOrStreamKind.Stream;
        _streamValue = stream;
        _streamLength = length;
        _encoding = encoding ?? Encoding.UTF8;
    }

    /// <summary>
    /// Creates a StringOrStream containing stream content with a cleanup action.
    /// </summary>
    /// <param name="stream">The stream content</param>
    /// <param name="length">The length of meaningful data in the stream</param>
    /// <param name="cleanupAction">Action to execute when disposing</param>
    /// <param name="encoding">The encoding to use for string conversions (default: UTF-8)</param>
    /// <exception cref="ArgumentNullException">Thrown when stream is null</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when length is negative</exception>
    public StringOrStream(Stream stream, long length, Func<ValueTask> cleanupAction, Encoding? encoding = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);

        Kind = StringOrStreamKind.Stream;
        _streamValue = stream;
        _streamLength = length;
        _cleanupAction = cleanupAction;
        _encoding = encoding ?? Encoding.UTF8;
    }

    /// <summary>
    /// Creates a StringOrStream containing stream content with a synchronous cleanup action.
    /// </summary>
    /// <param name="stream">The stream content</param>
    /// <param name="length">The length of meaningful data in the stream</param>
    /// <param name="cleanupAction">Synchronous action to execute when disposing</param>
    /// <param name="encoding">The encoding to use for string conversions (default: UTF-8)</param>
    /// <exception cref="ArgumentNullException">Thrown when stream is null</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when length is negative</exception>
    public StringOrStream(Stream stream, long length, Action cleanupAction, Encoding? encoding = null)
        : this(stream, length, () => { cleanupAction(); return ValueTask.CompletedTask; }, encoding)
    {
        ArgumentNullException.ThrowIfNull(cleanupAction);
    }

    /// <summary>
    /// Gets the content as a string.
    /// If the content is a stream, it will be read and converted to a string using the specified encoding.
    /// </summary>
    /// <returns>The string representation of the content</returns>
    /// <exception cref="ObjectDisposedException">Thrown when the instance has been disposed</exception>
    public string AsString()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        return Kind switch
        {
            StringOrStreamKind.String => _stringValue.NotNull(),
            StringOrStreamKind.Stream => ReadStreamAsString(),
            _ => throw new InvalidOperationException($"Unknown content kind: {Kind}")
        };
    }

    /// <summary>
    /// Asynchronously gets the content as a string.
    /// If the content is a stream, it will be read and converted to a string using the specified encoding.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token to observe</param>
    /// <returns>The string representation of the content</returns>
    /// <exception cref="ObjectDisposedException">Thrown when the instance has been disposed</exception>
    public async Task<string> AsStringAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        return Kind switch
        {
            StringOrStreamKind.String => _stringValue.NotNull(),
            StringOrStreamKind.Stream => await ReadStreamAsStringAsync(cancellationToken),
            _ => throw new InvalidOperationException($"Unknown content kind: {Kind}")
        };
    }

    /// <summary>
    /// Gets the content as a stream.
    /// If the content is a string, it will be converted to a stream using the specified encoding.
    /// </summary>
    /// <returns>A stream containing the content</returns>
    /// <exception cref="ObjectDisposedException">Thrown when the instance has been disposed</exception>
    /// <remarks>
    /// When converting from string to stream, the returned stream is a new MemoryTributary instance
    /// that the caller is responsible for disposing.
    /// </remarks>
    public Stream AsStream()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        return Kind switch
        {
            StringOrStreamKind.String => CreateStreamFromString,
            StringOrStreamKind.Stream => _streamValue.NotNull(),
            _ => throw new InvalidOperationException($"Unknown content kind: {Kind}")
        };
    }

    /// <summary>
    /// Attempts to get the string content if the kind is String.
    /// </summary>
    /// <param name="value">The string value if successful</param>
    /// <returns>True if the content is a string, false otherwise</returns>
    public bool TryGetString(out string? value)
    {
        if (!_disposed && Kind == StringOrStreamKind.String)
        {
            value = _stringValue;
            return true;
        }
        value = null;
        return false;
    }

    /// <summary>
    /// Attempts to get the stream content if the kind is Stream.
    /// </summary>
    /// <param name="stream">The stream value if successful</param>
    /// <param name="length">The stream length if successful</param>
    /// <returns>True if the content is a stream, false otherwise</returns>
    public bool TryGetStream(out Stream? stream, out long length)
    {
        if (!_disposed && Kind == StringOrStreamKind.Stream)
        {
            stream = _streamValue;
            length = _streamLength;
            return true;
        }
        stream = null;
        length = 0;
        return false;
    }

    /// <summary>
    /// Executes the appropriate action based on the content kind.
    /// </summary>
    /// <param name="onString">Action to execute if content is a string</param>
    /// <param name="onStream">Action to execute if content is a stream</param>
    /// <exception cref="ObjectDisposedException">Thrown when the instance has been disposed</exception>
    public void Match(Action<string>? onString = null, Action<Stream, long>? onStream = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        switch (Kind)
        {
            case StringOrStreamKind.String:
                onString?.Invoke(_stringValue.NotNull());
                break;
            case StringOrStreamKind.Stream:
                onStream?.Invoke(_streamValue.NotNull(), _streamLength);
                break;
        }
    }

    /// <summary>
    /// Returns a result by executing the appropriate function based on the content kind.
    /// </summary>
    /// <typeparam name="T">The return type</typeparam>
    /// <param name="onString">Function to execute if content is a string</param>
    /// <param name="onStream">Function to execute if content is a stream</param>
    /// <returns>The result of the executed function</returns>
    /// <exception cref="ObjectDisposedException">Thrown when the instance has been disposed</exception>
    public T Match<T>(Func<string, T> onString, Func<Stream, long, T> onStream)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        return Kind switch
        {
            StringOrStreamKind.String => onString(_stringValue.NotNull()),
            StringOrStreamKind.Stream => onStream(_streamValue.NotNull(), _streamLength),
            _ => throw new InvalidOperationException($"Unknown content kind: {Kind}")
        };
    }

    /// <summary>
    /// Asynchronously executes the appropriate function based on the content kind.
    /// </summary>
    /// <typeparam name="T">The return type</typeparam>
    /// <param name="onString">Function to execute if content is a string</param>
    /// <param name="onStream">Function to execute if content is a stream</param>
    /// <returns>The result of the executed function</returns>
    /// <exception cref="ObjectDisposedException">Thrown when the instance has been disposed</exception>
    public async Task<T> MatchAsync<T>(
        Func<string, Task<T>> onString,
        Func<Stream, long, Task<T>> onStream)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        return Kind switch
        {
            StringOrStreamKind.String => await onString(_stringValue.NotNull()),
            StringOrStreamKind.Stream => await onStream(_streamValue.NotNull(), _streamLength),
            _ => throw new InvalidOperationException($"Unknown content kind: {Kind}")
        };
    }

    /// <summary>
    /// Copies the content to the specified destination stream.
    /// </summary>
    /// <param name="destination">The destination stream</param>
    /// <exception cref="ArgumentNullException">Thrown when destination is null</exception>
    /// <exception cref="ObjectDisposedException">Thrown when the instance has been disposed</exception>
    public void CopyTo(Stream destination)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        switch (Kind)
        {
            case StringOrStreamKind.String:
                using (var sourceStream = CreateStreamFromString)
                {
                    sourceStream.CopyTo(destination);
                }
                break;
            case StringOrStreamKind.Stream:
                var originalPosition = _streamValue.NotNull().Position;
                try
                {
                    _streamValue.NotNull().Position = 0;
                    _streamValue.NotNull().CopyTo(destination);
                }
                finally
                {
                    _streamValue.NotNull().Position = originalPosition;
                }
                break;
        }
    }

    /// <summary>
    /// Asynchronously copies the content to the specified destination stream.
    /// </summary>
    /// <param name="destination">The destination stream</param>
    /// <param name="cancellationToken">A cancellation token to observe</param>
    /// <exception cref="ArgumentNullException">Thrown when destination is null</exception>
    /// <exception cref="ObjectDisposedException">Thrown when the instance has been disposed</exception>
    public async Task CopyToAsync(Stream destination, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        switch (Kind)
        {
            case StringOrStreamKind.String:
                await using (var sourceStream = CreateStreamFromString)
                {
                    await sourceStream.CopyToAsync(destination, cancellationToken);
                }
                break;
            case StringOrStreamKind.Stream:
                var originalPosition = _streamValue.NotNull().Position;
                try
                {
                    _streamValue.NotNull().Position = 0;
                    await _streamValue.NotNull().CopyToAsync(destination, cancellationToken);
                }
                finally
                {
                    _streamValue.NotNull().Position = originalPosition;
                }
                break;
        }
    }

    public override string ToString() => AsString();

    private Stream CreateStreamFromString
    {
        get
        {
            var bytes = _encoding.GetBytes(_stringValue.NotNull());
            return new MemoryTributary(bytes);
        }
    }

    private string ReadStreamAsString()
    {
        var originalPosition = _streamValue.NotNull().Position;
        try
        {
            _streamValue.NotNull().Position = 0;
            using var reader = new StreamReader(_streamValue.NotNull(), _encoding, leaveOpen: true);
            return reader.ReadToEnd();
        }
        finally
        {
            _streamValue.NotNull().Position = originalPosition;
        }
    }

    private async Task<string> ReadStreamAsStringAsync(CancellationToken cancellationToken)
    {
        var originalPosition = _streamValue.NotNull().Position;
        try
        {
            _streamValue.NotNull().Position = 0;
            using var reader = new StreamReader(_streamValue.NotNull(), _encoding, leaveOpen: true);
            return await reader.ReadToEndAsync(cancellationToken);
        }
        finally
        {
            _streamValue.NotNull().Position = originalPosition;
        }
    }

    /// <summary>
    /// Releases all resources used by the StringOrStream.
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            if (_cleanupAction is not null)
            {
                // For synchronous disposal, we need to handle async cleanup actions
                try
                {
                    _cleanupAction().AsTask().GetAwaiter().GetResult();
                }
                catch
                {
                    // Swallow exceptions in finalizer context
                }
            }
            _disposed = true;
        }
    }

    /// <summary>
    /// Asynchronously releases all resources used by the StringOrStream.
    /// </summary>
    /// <returns>A ValueTask representing the asynchronous dispose operation</returns>
    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            if (_cleanupAction is not null)
            {
                await _cleanupAction();
            }
            _disposed = true;
        }
    }

    // Implicit conversion operators for convenience
    public static implicit operator StringOrStream(string content) => new(content);
}
