// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Xunit;

namespace CrossCloudKit.Utilities.Common.Tests;

public class StringOrStreamTests
{
    [Fact]
    public void StringOrStream_StringConstructor_InitializesCorrectly()
    {
        // Arrange
        const string testContent = "Hello, World!";

        // Act
        using var stringOrStream = new StringOrStream(testContent);

        // Assert
        Assert.Equal(StringOrStreamKind.String, stringOrStream.Kind);
        Assert.Equal(testContent.Length, stringOrStream.Length);
    }

    [Fact]
    public void StringOrStream_StringConstructor_WithCustomEncoding_InitializesCorrectly()
    {
        // Arrange
        const string testContent = "Hello, World!";
        var encoding = System.Text.Encoding.ASCII;

        // Act
        using var stringOrStream = new StringOrStream(testContent, encoding);

        // Assert
        Assert.Equal(StringOrStreamKind.String, stringOrStream.Kind);
        Assert.Equal(testContent.Length, stringOrStream.Length);
    }

    [Fact]
    public void StringOrStream_StreamConstructor_InitializesCorrectly()
    {
        // Arrange
        var testData = new byte[] { 1, 2, 3, 4, 5 };
        using var stream = new MemoryStream(testData);

        // Act
        using var stringOrStream = new StringOrStream(stream, testData.Length);

        // Assert
        Assert.Equal(StringOrStreamKind.Stream, stringOrStream.Kind);
        Assert.Equal(testData.Length, stringOrStream.Length);
    }

    [Fact]
    public void StringOrStream_StreamConstructor_WithCustomEncoding_InitializesCorrectly()
    {
        // Arrange
        var testData = new byte[] { 1, 2, 3, 4, 5 };
        using var stream = new MemoryStream(testData);
        var encoding = System.Text.Encoding.ASCII;

        // Act
        using var stringOrStream = new StringOrStream(stream, testData.Length, encoding);

        // Assert
        Assert.Equal(StringOrStreamKind.Stream, stringOrStream.Kind);
        Assert.Equal(testData.Length, stringOrStream.Length);
    }

    [Fact]
    public void StringOrStream_StreamConstructor_WithAsyncCleanup_InitializesCorrectly()
    {
        // Arrange
        var testData = new byte[] { 1, 2, 3, 4, 5 };
        using var stream = new MemoryStream(testData);
        var cleanupCalled = false;

        // Act
        using var stringOrStream = new StringOrStream(stream, testData.Length, () =>
        {
            cleanupCalled = true;
            return ValueTask.CompletedTask;
        });

        // Assert
        Assert.Equal(StringOrStreamKind.Stream, stringOrStream.Kind);
        Assert.Equal(testData.Length, stringOrStream.Length);
        Assert.False(cleanupCalled); // Should not be called until disposal
    }

    [Fact]
    public void StringOrStream_StreamConstructor_WithSyncCleanup_InitializesCorrectly()
    {
        // Arrange
        var testData = new byte[] { 1, 2, 3, 4, 5 };
        using var stream = new MemoryStream(testData);
        var cleanupCalled = false;

        // Act
        using var stringOrStream = new StringOrStream(stream, testData.Length, () => cleanupCalled = true);

        // Assert
        Assert.Equal(StringOrStreamKind.Stream, stringOrStream.Kind);
        Assert.Equal(testData.Length, stringOrStream.Length);
        Assert.False(cleanupCalled); // Should not be called until disposal
    }

    [Fact]
    public void StringOrStream_StreamConstructor_WithNegativeLength_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        using var stream = new MemoryStream();

        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => new StringOrStream(stream, -1));
    }

    [Fact]
    public void StringOrStream_AsString_WithStringContent_ReturnsString()
    {
        // Arrange
        const string testContent = "Test string content";
        using var stringOrStream = new StringOrStream(testContent);

        // Act
        var result = stringOrStream.AsString();

        // Assert
        Assert.Equal(testContent, result);
    }

    [Fact]
    public void StringOrStream_AsString_WithStreamContent_ReturnsConvertedString()
    {
        // Arrange
        const string originalContent = "Stream content";
        var bytes = System.Text.Encoding.UTF8.GetBytes(originalContent);
        using var stream = new MemoryStream(bytes);
        using var stringOrStream = new StringOrStream(stream, bytes.Length);

        // Act
        var result = stringOrStream.AsString();

        // Assert
        Assert.Equal(originalContent, result);
    }

    [Fact]
    public async Task StringOrStream_AsStringAsync_WithStringContent_ReturnsString()
    {
        // Arrange
        const string testContent = "Test string content async";
        await using var stringOrStream = new StringOrStream(testContent);

        // Act
        var result = await stringOrStream.AsStringAsync();

        // Assert
        Assert.Equal(testContent, result);
    }

    [Fact]
    public async Task StringOrStream_AsStringAsync_WithStreamContent_ReturnsConvertedString()
    {
        // Arrange
        const string originalContent = "Stream content async";
        var bytes = System.Text.Encoding.UTF8.GetBytes(originalContent);
        using var stream = new MemoryStream(bytes);
        await using var stringOrStream = new StringOrStream(stream, bytes.Length);

        // Act
        var result = await stringOrStream.AsStringAsync();

        // Assert
        Assert.Equal(originalContent, result);
    }

    [Fact]
    public async Task StringOrStream_AsStringAsync_WithCancellation_ThrowsTaskCanceledException()
    {
        // Arrange
        const string originalContent = "Stream content for cancellation";
        var bytes = System.Text.Encoding.UTF8.GetBytes(originalContent);
        using var stream = new MemoryStream(bytes);
        await using var stringOrStream = new StringOrStream(stream, bytes.Length);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Act & Assert
        await Assert.ThrowsAsync<TaskCanceledException>(() =>
            stringOrStream.AsStringAsync(cts.Token));
    }

    [Fact]
    public void StringOrStream_AsStream_WithStringContent_ReturnsStream()
    {
        // Arrange
        const string testContent = "Test string for stream conversion";
        using var stringOrStream = new StringOrStream(testContent);

        // Act
        using var resultStream = stringOrStream.AsStream();

        // Assert
        Assert.NotNull(resultStream);
        Assert.True(resultStream.CanRead);

        // Read the stream content to verify
        using var reader = new StreamReader(resultStream, System.Text.Encoding.UTF8);
        var readContent = reader.ReadToEnd();
        Assert.Equal(testContent, readContent);
    }

    [Fact]
    public void StringOrStream_AsStream_WithStreamContent_ReturnsOriginalStream()
    {
        // Arrange
        var testData = "Hello"u8.ToArray(); // "Hello" in UTF-8
        using var originalStream = new MemoryStream(testData);
        using var stringOrStream = new StringOrStream(originalStream, testData.Length);

        // Act
        var resultStream = stringOrStream.AsStream();

        // Assert
        Assert.Same(originalStream, resultStream);
    }

    [Fact]
    public void StringOrStream_TryGetString_WithStringContent_ReturnsTrue()
    {
        // Arrange
        const string testContent = "Test string content";
        using var stringOrStream = new StringOrStream(testContent);

        // Act
        var success = stringOrStream.TryGetString(out var value);

        // Assert
        Assert.True(success);
        Assert.Equal(testContent, value);
    }

    [Fact]
    public void StringOrStream_TryGetString_WithStreamContent_ReturnsFalse()
    {
        // Arrange
        using var stream = new MemoryStream([1, 2, 3]);
        using var stringOrStream = new StringOrStream(stream, 3);

        // Act
        var success = stringOrStream.TryGetString(out var value);

        // Assert
        Assert.False(success);
        Assert.Null(value);
    }

    [Fact]
    public void StringOrStream_TryGetStream_WithStreamContent_ReturnsTrue()
    {
        // Arrange
        var testData = new byte[] { 1, 2, 3, 4, 5 };
        using var stream = new MemoryStream(testData);
        using var stringOrStream = new StringOrStream(stream, testData.Length);

        // Act
        var success = stringOrStream.TryGetStream(out var resultStream, out var length);

        // Assert
        Assert.True(success);
        Assert.Same(stream, resultStream);
        Assert.Equal(testData.Length, length);
    }

    [Fact]
    public void StringOrStream_TryGetStream_WithStringContent_ReturnsFalse()
    {
        // Arrange
        using var stringOrStream = new StringOrStream("test content");

        // Act
        var success = stringOrStream.TryGetStream(out var resultStream, out var length);

        // Assert
        Assert.False(success);
        Assert.Null(resultStream);
        Assert.Equal(0, length);
    }

    [Fact]
    public void StringOrStream_Match_WithStringContent_ExecutesStringAction()
    {
        // Arrange
        const string testContent = "Match test content";
        using var stringOrStream = new StringOrStream(testContent);
        string? capturedString = null;
        var streamActionCalled = false;

        // Act
        stringOrStream.Match(
            onString: s => capturedString = s,
            onStream: (_, _) => streamActionCalled = true
        );

        // Assert
        Assert.Equal(testContent, capturedString);
        Assert.False(streamActionCalled);
    }

    [Fact]
    public void StringOrStream_Match_WithStreamContent_ExecutesStreamAction()
    {
        // Arrange
        var testData = new byte[] { 1, 2, 3, 4, 5 };
        using var stream = new MemoryStream(testData);
        using var stringOrStream = new StringOrStream(stream, testData.Length);
        var stringActionCalled = false;
        Stream? capturedStream = null;
        long capturedLength = 0;

        // Act
        stringOrStream.Match(
            onString: _ => stringActionCalled = true,
            onStream: (s, l) => { capturedStream = s; capturedLength = l; }
        );

        // Assert
        Assert.False(stringActionCalled);
        Assert.Same(stream, capturedStream);
        Assert.Equal(testData.Length, capturedLength);
    }

    [Fact]
    public void StringOrStream_MatchWithReturn_WithStringContent_ReturnsStringResult()
    {
        // Arrange
        const string testContent = "Return match test";
        using var stringOrStream = new StringOrStream(testContent);

        // Act
        var result = stringOrStream.Match(
            onString: s => $"String: {s.Length} chars",
            onStream: (_, l) => $"Stream: {l} bytes"
        );

        // Assert
        Assert.Equal($"String: {testContent.Length} chars", result);
    }

    [Fact]
    public void StringOrStream_MatchWithReturn_WithStreamContent_ReturnsStreamResult()
    {
        // Arrange
        var testData = new byte[] { 1, 2, 3, 4, 5 };
        using var stream = new MemoryStream(testData);
        using var stringOrStream = new StringOrStream(stream, testData.Length);

        // Act
        var result = stringOrStream.Match(
            onString: s => $"String: {s.Length} chars",
            onStream: (_, l) => $"Stream: {l} bytes"
        );

        // Assert
        Assert.Equal($"Stream: {testData.Length} bytes", result);
    }

    [Fact]
    public async Task StringOrStream_MatchAsync_WithStringContent_ReturnsStringResult()
    {
        // Arrange
        const string testContent = "Async match test";
        await using var stringOrStream = new StringOrStream(testContent);

        // Act
        var result = await stringOrStream.MatchAsync(
            onString: async s =>
            {
                await Task.Delay(1);
                return $"Async String: {s.Length} chars";
            },
            onStream: async (_, l) =>
            {
                await Task.Delay(1);
                return $"Async Stream: {l} bytes";
            }
        );

        // Assert
        Assert.Equal($"Async String: {testContent.Length} chars", result);
    }

    [Fact]
    public async Task StringOrStream_MatchAsync_WithStreamContent_ReturnsStreamResult()
    {
        // Arrange
        var testData = new byte[] { 1, 2, 3, 4, 5 };
        using var stream = new MemoryStream(testData);
        await using var stringOrStream = new StringOrStream(stream, testData.Length);

        // Act
        var result = await stringOrStream.MatchAsync(
            onString: async s =>
            {
                await Task.Delay(1);
                return $"Async String: {s.Length} chars";
            },
            onStream: async (_, l) =>
            {
                await Task.Delay(1);
                return $"Async Stream: {l} bytes";
            }
        );

        // Assert
        Assert.Equal($"Async Stream: {testData.Length} bytes", result);
    }

    [Fact]
    public void StringOrStream_CopyTo_WithStringContent_CopiesCorrectly()
    {
        // Arrange
        const string testContent = "Copy test content";
        using var stringOrStream = new StringOrStream(testContent);
        using var destination = new MemoryStream();

        // Act
        stringOrStream.CopyTo(destination);

        // Assert
        destination.Position = 0;
        using var reader = new StreamReader(destination, System.Text.Encoding.UTF8);
        var copiedContent = reader.ReadToEnd();
        Assert.Equal(testContent, copiedContent);
    }

    [Fact]
    public void StringOrStream_CopyTo_WithStreamContent_CopiesCorrectly()
    {
        // Arrange
        var testData = "Hello"u8.ToArray(); // "Hello" in UTF-8
        using var source = new MemoryStream(testData);
        using var stringOrStream = new StringOrStream(source, testData.Length);
        using var destination = new MemoryStream();

        // Act
        stringOrStream.CopyTo(destination);

        // Assert
        Assert.Equal(testData, destination.ToArray());
    }

    [Fact]
    public async Task StringOrStream_CopyToAsync_WithStringContent_CopiesCorrectly()
    {
        // Arrange
        const string testContent = "Async copy test content";
        await using var stringOrStream = new StringOrStream(testContent);
        using var destination = new MemoryStream();

        // Act
        await stringOrStream.CopyToAsync(destination);

        // Assert
        destination.Position = 0;
        using var reader = new StreamReader(destination, System.Text.Encoding.UTF8);
        var copiedContent = await reader.ReadToEndAsync();
        Assert.Equal(testContent, copiedContent);
    }

    [Fact]
    public async Task StringOrStream_CopyToAsync_WithStreamContent_CopiesCorrectly()
    {
        // Arrange
        var testData = "Hello"u8.ToArray(); // "Hello" in UTF-8
        using var source = new MemoryStream(testData);
        await using var stringOrStream = new StringOrStream(source, testData.Length);
        using var destination = new MemoryStream();

        // Act
        await stringOrStream.CopyToAsync(destination);

        // Assert
        Assert.Equal(testData, destination.ToArray());
    }

    [Fact]
    public async Task StringOrStream_CopyToAsync_WithCancellation_ThrowsTaskCanceledException()
    {
        // Arrange
        const string testContent = "Cancellation test content";
        await using var stringOrStream = new StringOrStream(testContent);
        using var destination = new MemoryStream();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Act & Assert
        await Assert.ThrowsAsync<TaskCanceledException>(() =>
            stringOrStream.CopyToAsync(destination, cts.Token));
    }

    [Fact]
    public void StringOrStream_ToString_WithStringContent_ReturnsString()
    {
        // Arrange
        const string testContent = "ToString test content";
        using var stringOrStream = new StringOrStream(testContent);

        // Act
        var result = stringOrStream.ToString();

        // Assert
        Assert.Equal(testContent, result);
    }

    [Fact]
    public void StringOrStream_ToString_WithStreamContent_ReturnsConvertedString()
    {
        // Arrange
        const string originalContent = "ToString stream content";
        var bytes = System.Text.Encoding.UTF8.GetBytes(originalContent);
        using var stream = new MemoryStream(bytes);
        using var stringOrStream = new StringOrStream(stream, bytes.Length);

        // Act
        var result = stringOrStream.ToString();

        // Assert
        Assert.Equal(originalContent, result);
    }

    [Fact]
    public void StringOrStream_ImplicitConversion_FromString_WorksCorrectly()
    {
        // Arrange
        const string testContent = "Implicit conversion test";

        // Act
        StringOrStream stringOrStream = testContent;

        // Assert
        using (stringOrStream)
        {
            Assert.Equal(StringOrStreamKind.String, stringOrStream.Kind);
            Assert.Equal(testContent, stringOrStream.AsString());
        }
    }

    [Fact]
    public void StringOrStream_Dispose_CallsCleanupAction()
    {
        // Arrange
        var testData = new byte[] { 1, 2, 3 };
        using var stream = new MemoryStream(testData);
        var cleanupCalled = false;
        var stringOrStream = new StringOrStream(stream, testData.Length, () => cleanupCalled = true);

        // Act
        stringOrStream.Dispose();

        // Assert
        Assert.True(cleanupCalled);
    }

    [Fact]
    public async Task StringOrStream_DisposeAsync_CallsAsyncCleanupAction()
    {
        // Arrange
        var testData = new byte[] { 1, 2, 3 };
        using var stream = new MemoryStream(testData);
        var cleanupCalled = false;
        var stringOrStream = new StringOrStream(stream, testData.Length, () =>
        {
            cleanupCalled = true;
            return ValueTask.CompletedTask;
        });

        // Act
        await stringOrStream.DisposeAsync();

        // Assert
        Assert.True(cleanupCalled);
    }

    [Fact]
    public async Task StringOrStream_AfterDispose_ThrowsObjectDisposedException()
    {
        // Arrange
        const string testContent = "Disposal test";
        var stringOrStream = new StringOrStream(testContent);
        await stringOrStream.DisposeAsync();

        // Act & Assert
        Assert.Throws<ObjectDisposedException>(() => stringOrStream.AsString());
        Assert.Throws<ObjectDisposedException>(() => stringOrStream.AsStream());
        await Assert.ThrowsAsync<ObjectDisposedException>(() => stringOrStream.AsStringAsync());
    }

    [Fact]
    public void StringOrStream_TryMethods_AfterDispose_ReturnFalse()
    {
        // Arrange
        const string testContent = "Try methods disposal test";
        var stringOrStream = new StringOrStream(testContent);
        stringOrStream.Dispose();

        // Act
        var stringSuccess = stringOrStream.TryGetString(out var stringValue);
        var streamSuccess = stringOrStream.TryGetStream(out var streamValue, out var length);

        // Assert
        Assert.False(stringSuccess);
        Assert.Null(stringValue);
        Assert.False(streamSuccess);
        Assert.Null(streamValue);
        Assert.Equal(0, length);
    }

    [Fact]
    public void StringOrStream_WithDifferentEncodings_HandlesCorrectly()
    {
        // Arrange
        const string testContent = "Encoding test: éñçödîñg";
        var utf8StringOrStream = new StringOrStream(testContent, System.Text.Encoding.UTF8);
        var asciiStringOrStream = new StringOrStream(testContent, System.Text.Encoding.ASCII);

        try
        {
            // Act
            using var utf8Stream = utf8StringOrStream.AsStream();
            using var asciiStream = asciiStringOrStream.AsStream();

            // Assert - Both should create streams but with different byte representations
            Assert.NotNull(utf8Stream);
            Assert.NotNull(asciiStream);
            Assert.True(utf8Stream.Length > 0);
            Assert.True(asciiStream.Length > 0);
        }
        finally
        {
            utf8StringOrStream.Dispose();
            asciiStringOrStream.Dispose();
        }
    }

    [Fact]
    public void StringOrStream_StreamPositionPreservation_WorksCorrectly()
    {
        // Arrange
        const string originalContent = "Position preservation test";
        var bytes = System.Text.Encoding.UTF8.GetBytes(originalContent);
        using var stream = new MemoryStream(bytes);
        stream.Position = 5; // Move to middle
        using var stringOrStream = new StringOrStream(stream, bytes.Length);

        // Act - Operations that should preserve position
        var result1 = stringOrStream.AsString();
        var result2 = stringOrStream.ToString();

        // Assert
        Assert.Equal(originalContent, result1);
        Assert.Equal(originalContent, result2);
        Assert.Equal(5, stream.Position); // Position should be preserved
    }

    [Fact]
    public async Task StringOrStream_StreamPositionPreservationAsync_WorksCorrectly()
    {
        // Arrange
        const string originalContent = "Async position preservation test";
        var bytes = System.Text.Encoding.UTF8.GetBytes(originalContent);
        using var stream = new MemoryStream(bytes);
        stream.Position = 8; // Move to middle
        await using var stringOrStream = new StringOrStream(stream, bytes.Length);

        // Act
        var result = await stringOrStream.AsStringAsync();

        // Assert
        Assert.Equal(originalContent, result);
        Assert.Equal(8, stream.Position); // Position should be preserved
    }

    [Fact]
    public void StringOrStream_WithUnicodeContent_HandlesCorrectly()
    {
        // Arrange
        const string unicodeContent = "Unicode test: 你好世界 🌍 Здравствуй мир";
        using var stringOrStream = new StringOrStream(unicodeContent);

        // Act
        var asString = stringOrStream.AsString();
        using var asStream = stringOrStream.AsStream();

        // Assert
        Assert.Equal(unicodeContent, asString);
        Assert.NotNull(asStream);

        // Convert stream back to string to verify
        asStream.Position = 0;
        using var reader = new StreamReader(asStream, System.Text.Encoding.UTF8);
        var streamContent = reader.ReadToEnd();
        Assert.Equal(unicodeContent, streamContent);
    }

    [Fact]
    public void StringOrStream_WithLargeContent_HandlesCorrectly()
    {
        // Arrange
        var largeContent = new string('A', 100000); // 100KB string
        using var stringOrStream = new StringOrStream(largeContent);

        // Act
        var asString = stringOrStream.AsString();
        using var asStream = stringOrStream.AsStream();

        // Assert
        Assert.Equal(largeContent, asString);
        Assert.NotNull(asStream);
        Assert.Equal(largeContent.Length, stringOrStream.Length);
    }

    [Fact]
    public void StringOrStream_WithEmptyString_HandlesCorrectly()
    {
        // Arrange
        const string emptyContent = "";
        using var stringOrStream = new StringOrStream(emptyContent);

        // Act
        var asString = stringOrStream.AsString();
        using var asStream = stringOrStream.AsStream();

        // Assert
        Assert.Equal(emptyContent, asString);
        Assert.NotNull(asStream);
        Assert.Equal(0, stringOrStream.Length);
    }

    [Fact]
    public void StringOrStream_WithEmptyStream_HandlesCorrectly()
    {
        // Arrange
        using var emptyStream = new MemoryStream();
        using var stringOrStream = new StringOrStream(emptyStream, 0);

        // Act
        var asString = stringOrStream.AsString();
        var retrievedStream = stringOrStream.AsStream();

        // Assert
        Assert.Equal("", asString);
        Assert.Same(emptyStream, retrievedStream);
        Assert.Equal(0, stringOrStream.Length);
    }

    [Fact]
    public void StringOrStream_Performance_ManyOperations()
    {
        // Arrange
        const int operationCount = 1000;
        const string testContent = "Performance test content";

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Act
        for (int i = 0; i < operationCount; i++)
        {
            using var stringOrStream = new StringOrStream(testContent);
            var _ = stringOrStream.AsString();
            using var stream = stringOrStream.AsStream();
            var __ = stringOrStream.ToString();
        }

        stopwatch.Stop();

        // Assert
        Assert.True(stopwatch.ElapsedMilliseconds < 1000,
            $"Performance test failed: took {stopwatch.ElapsedMilliseconds}ms for {operationCount} operations");
    }

    [Fact]
    public async Task StringOrStream_ConcurrentAccess_WorksCorrectly()
    {
        // Arrange
        const string testContent = "Concurrent access test";
        await using var stringOrStream = new StringOrStream(testContent);
        const int taskCount = 10;

        // Act
        var tasks = Enumerable.Range(0, taskCount).Select(async _ =>
        {
            var result1 = await stringOrStream.AsStringAsync();
            var result2 = await stringOrStream.AsStringAsync();
            var result3 = stringOrStream.ToString();

            return new[] { result1, result2, result3 };
        }).ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert
        foreach (var taskResults in results)
        {
            Assert.All(taskResults, result => Assert.Equal(testContent, result));
        }
    }

    [Fact]
    public void StringOrStream_IntegrationTest_ComplexScenario()
    {
        // Arrange - Test multiple operations in sequence
        const string originalContent = "Integration test content";

        // Test 1: String to StringOrStream to Stream to StringOrStream to String
        using var stringOrStream1 = new StringOrStream(originalContent);
        using var intermediateStream = stringOrStream1.AsStream();

        // Read stream content to get bytes
        var streamBytes = new byte[intermediateStream.Length];
        intermediateStream.ReadExactly(streamBytes);

        using var newStream = new MemoryStream(streamBytes);
        using var stringOrStream2 = new StringOrStream(newStream, streamBytes.Length);
        var finalContent = stringOrStream2.AsString();

        // Assert
        Assert.Equal(originalContent, finalContent);

        // Test 2: Copy operations
        using var destination1 = new MemoryStream();
        using var destination2 = new MemoryStream();

        stringOrStream1.CopyTo(destination1);
        stringOrStream2.CopyTo(destination2);

        Assert.Equal(destination1.ToArray(), destination2.ToArray());

        // Test 3: Match operations
        var result1 = stringOrStream1.Match(
            onString: s => s.Length,
            onStream: (_, l) => (int)l
        );

        var result2 = stringOrStream2.Match(
            onString: s => s.Length,
            onStream: (_, l) => (int)l
        );

        Assert.Equal(result1, result2);
    }
}
