// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Text;
using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Memory.Basic;
using CrossCloudKit.PubSub.Basic;
using FluentAssertions;
using Microsoft.Extensions.Caching.Distributed;
using Xunit;

namespace CrossCloudKit.Interfaces.Tests;

public class MemoryServiceDistributedCacheTests : IAsyncLifetime
{
    private readonly IMemoryService _memoryService;
    private readonly IPubSubService _pubSubService;
    private readonly MemoryServiceDistributedCache _distributedCache;
    private readonly List<string> _testKeys = new();

    public MemoryServiceDistributedCacheTests()
    {
        _pubSubService = new PubSubServiceBasic();
        _memoryService = new MemoryServiceBasic(_pubSubService);
        _distributedCache = new MemoryServiceDistributedCache(_memoryService);
    }

    public async Task InitializeAsync()
    {
        // Wait a bit for services to initialize
        await Task.Delay(100);
    }

    public async Task DisposeAsync()
    {
        try
        {
            // Clean up all test data
            foreach (var key in _testKeys)
            {
                try
                {
                    await _distributedCache.RemoveAsync(key);
                }
                catch
                {
                    // Ignore cleanup errors
                }
            }
        }
        catch
        {
            // Ignore cleanup errors
        }

        if (_memoryService is IAsyncDisposable asyncDisposableMemory)
            await asyncDisposableMemory.DisposeAsync();

        if (_pubSubService is IAsyncDisposable asyncDisposablePubSub)
            await asyncDisposablePubSub.DisposeAsync();
    }

    private void TrackTestKey(string key)
    {
        _testKeys.Add(key);
    }

    [Fact]
    public async Task GetAsync_WithValidKey_ReturnsData()
    {
        // Arrange
        var key = "test-key";
        TrackTestKey(key);
        var expectedData = Encoding.UTF8.GetBytes("test-data");
        var options = new DistributedCacheEntryOptions();

        // Store the data first
        await _distributedCache.SetAsync(key, expectedData, options);

        // Act
        var result = await _distributedCache.GetAsync(key);

        // Assert
        result.Should().NotBeNull();
        result.Should().Equal(expectedData);
    }

    [Fact]
    public async Task GetAsync_WithNullKey_ReturnsNull()
    {
        // Act
        var result = await _distributedCache.GetAsync(null!);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public async Task GetAsync_WithNonExistentKey_ReturnsNull()
    {
        // Arrange
        var key = "non-existent-key";
        TrackTestKey(key);

        // Act
        var result = await _distributedCache.GetAsync(key);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public async Task SetAsync_WithNoExpiration_StoresDataWithInfiniteTtl()
    {
        // Arrange
        var key = "test-key";
        TrackTestKey(key);
        var value = "test-data"u8.ToArray();
        var options = new DistributedCacheEntryOptions();

        // Act
        await _distributedCache.SetAsync(key, value, options);

        // Assert - Should be able to retrieve the data
        var retrievedData = await _distributedCache.GetAsync(key);
        retrievedData.Should().NotBeNull();
        retrievedData.Should().Equal(value);

        // Verify that no expiration was set by checking data is still accessible after a brief delay
        await Task.Delay(100);
        var stillAccessible = await _distributedCache.GetAsync(key);
        stillAccessible.Should().NotBeNull();
    }

    [Fact]
    public async Task SetAsync_WithSlidingExpiration_StoresDataWithTtl()
    {
        // Arrange
        var key = "test-key";
        TrackTestKey(key);
        var value = Encoding.UTF8.GetBytes("test-data");
        var slidingExpiration = TimeSpan.FromSeconds(3); // Use shorter time for testing
        var options = new DistributedCacheEntryOptions
        {
            SlidingExpiration = slidingExpiration
        };

        // Act
        await _distributedCache.SetAsync(key, value, options);

        // Assert - Should be able to retrieve the data initially
        var retrievedData = await _distributedCache.GetAsync(key);
        retrievedData.Should().NotBeNull();
        retrievedData.Should().Equal(value);

        // Wait for expiration and verify data is no longer accessible
        await Task.Delay(slidingExpiration.Add(TimeSpan.FromSeconds(1)));
        var expiredData = await _distributedCache.GetAsync(key);
        expiredData.Should().BeNull();
    }

    [Fact]
    public async Task SetAsync_WithAbsoluteExpiration_StoresDataWithCalculatedTtl()
    {
        // Arrange
        var key = "test-key";
        TrackTestKey(key);
        var value = Encoding.UTF8.GetBytes("test-data");
        var absoluteExpiration = DateTimeOffset.UtcNow.AddSeconds(3); // Use shorter time for testing
        var options = new DistributedCacheEntryOptions
        {
            AbsoluteExpiration = absoluteExpiration
        };

        // Act
        await _distributedCache.SetAsync(key, value, options);

        // Assert - Should be able to retrieve the data initially
        var retrievedData = await _distributedCache.GetAsync(key);
        retrievedData.Should().NotBeNull();
        retrievedData.Should().Equal(value);

        // Wait for expiration and verify data is no longer accessible
        await Task.Delay(TimeSpan.FromSeconds(4));
        var expiredData = await _distributedCache.GetAsync(key);
        expiredData.Should().BeNull();
    }

    [Fact]
    public async Task SetAsync_WithAbsoluteExpirationRelativeToNow_StoresDataWithTtl()
    {
        // Arrange
        var key = "test-key";
        TrackTestKey(key);
        var value = Encoding.UTF8.GetBytes("test-data");
        var relativeExpiration = TimeSpan.FromSeconds(3); // Use shorter time for testing
        var options = new DistributedCacheEntryOptions
        {
            AbsoluteExpirationRelativeToNow = relativeExpiration
        };

        // Act
        await _distributedCache.SetAsync(key, value, options);

        // Assert - Should be able to retrieve the data initially
        var retrievedData = await _distributedCache.GetAsync(key);
        retrievedData.Should().NotBeNull();
        retrievedData.Should().Equal(value);

        // Wait for expiration and verify data is no longer accessible
        await Task.Delay(relativeExpiration.Add(TimeSpan.FromSeconds(1)));
        var expiredData = await _distributedCache.GetAsync(key);
        expiredData.Should().BeNull();
    }

    [Fact]
    public async Task RefreshAsync_WithInfiniteTtl_DoesNotSetExpiration()
    {
        // Arrange
        var key = "test-key";
        TrackTestKey(key);
        var value = Encoding.UTF8.GetBytes("test-data");
        var options = new DistributedCacheEntryOptions(); // No expiration = infinite TTL

        // Set the data with infinite TTL
        await _distributedCache.SetAsync(key, value, options);

        // Act - Refresh should not affect infinite TTL items
        await _distributedCache.RefreshAsync(key);

        // Assert - Data should still be accessible (no expiration set)
        var refreshedData = await _distributedCache.GetAsync(key);
        refreshedData.Should().NotBeNull();
        refreshedData.Should().Equal(value);
    }

    [Fact]
    public async Task RefreshAsync_WithNullKey_DoesNothing()
    {
        // Act & Assert - Should not throw an exception
        await _distributedCache.RefreshAsync(null!);

        // Test passes if no exception is thrown
        Assert.True(true);
    }

    [Fact]
    public async Task RemoveAsync_WithValidKey_DeletesAllKeys()
    {
        // Arrange
        var key = "test-key";
        TrackTestKey(key);
        var value = Encoding.UTF8.GetBytes("test-data");
        var options = new DistributedCacheEntryOptions();

        // First set some data
        await _distributedCache.SetAsync(key, value, options);

        // Verify data exists
        var beforeRemove = await _distributedCache.GetAsync(key);
        beforeRemove.Should().NotBeNull();

        // Act
        await _distributedCache.RemoveAsync(key);

        // Assert - Data should no longer be accessible
        var afterRemove = await _distributedCache.GetAsync(key);
        afterRemove.Should().BeNull();
    }

    [Fact]
    public async Task RemoveAsync_WithNullKey_DoesNothing()
    {
        // Act & Assert - Should not throw an exception
        await _distributedCache.RemoveAsync(null!);

        // Test passes if no exception is thrown
        Assert.True(true);
    }

    [Fact]
    public void Get_CallsGetAsync()
    {
        // Arrange
        var key = "test-key";
        TrackTestKey(key);
        var expectedData = Encoding.UTF8.GetBytes("test-data");
        var options = new DistributedCacheEntryOptions();

        // Store the data first
        _distributedCache.SetAsync(key, expectedData, options).GetAwaiter().GetResult();

        // Act
        var result = _distributedCache.Get(key);

        // Assert
        result.Should().NotBeNull();
        result.Should().Equal(expectedData);
    }

    [Fact]
    public void Set_CallsSetAsync()
    {
        // Arrange
        var key = "test-key";
        TrackTestKey(key);
        var value = Encoding.UTF8.GetBytes("test-data");
        var options = new DistributedCacheEntryOptions();

        // Act
        _distributedCache.Set(key, value, options);

        // Assert - Verify data was stored by retrieving it
        var retrievedData = _distributedCache.Get(key);
        retrievedData.Should().NotBeNull();
        retrievedData.Should().Equal(value);
    }

    [Fact]
    public void Refresh_CallsRefreshAsync()
    {
        // Arrange
        var key = "test-key";
        TrackTestKey(key);
        var value = Encoding.UTF8.GetBytes("test-data");
        var options = new DistributedCacheEntryOptions
        {
            SlidingExpiration = TimeSpan.FromMinutes(30)
        };

        // Store data with sliding expiration
        _distributedCache.Set(key, value, options);

        // Act - Should not throw an exception
        _distributedCache.Refresh(key);

        // Assert - Verify data is still accessible
        var refreshedData = _distributedCache.Get(key);
        refreshedData.Should().NotBeNull();
        refreshedData.Should().Equal(value);
    }

    [Fact]
    public void Remove_CallsRemoveAsync()
    {
        // Arrange
        var key = "test-key";
        TrackTestKey(key);
        var value = Encoding.UTF8.GetBytes("test-data");
        var options = new DistributedCacheEntryOptions();

        // Store data first
        _distributedCache.Set(key, value, options);

        // Verify data exists
        var beforeRemove = _distributedCache.Get(key);
        beforeRemove.Should().NotBeNull();

        // Act
        _distributedCache.Remove(key);

        // Assert - Data should no longer be accessible
        var afterRemove = _distributedCache.Get(key);
        afterRemove.Should().BeNull();
    }

    [Fact]
    public async Task SetAsync_HandlesException_Gracefully()
    {
        // Arrange - Use a disposed memory service to simulate exception scenario
        var disposedMemoryService = new MemoryServiceBasic();
        await disposedMemoryService.DisposeAsync();
        var distributedCache = new MemoryServiceDistributedCache(disposedMemoryService);

        var key = "test-key";
        var value = Encoding.UTF8.GetBytes("test-data");
        var options = new DistributedCacheEntryOptions();

        // Act & Assert - Should not throw exception, should handle gracefully
        await distributedCache.SetAsync(key, value, options);

        // If we get here without exception, the test passes
        Assert.True(true);
    }

    [Fact]
    public async Task GetAsync_HandlesException_ReturnsNull()
    {
        // Arrange - Use a disposed memory service to simulate exception scenario
        var disposedMemoryService = new MemoryServiceBasic();
        await disposedMemoryService.DisposeAsync();
        var distributedCache = new MemoryServiceDistributedCache(disposedMemoryService);

        var key = "test-key";

        // Act
        var result = await distributedCache.GetAsync(key);

        // Assert - Should return null when exception occurs
        result.Should().BeNull();
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("\t\n")]
    public async Task GetAsync_WithWhitespaceKey_ReturnsNull(string key)
    {
        // Act
        var result = await _distributedCache.GetAsync(key);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public async Task SetAsync_WithNullValue_DoesNothing()
    {
        // Arrange
        var key = "test-key";
        TrackTestKey(key);
        var options = new DistributedCacheEntryOptions();

        // Act
        await _distributedCache.SetAsync(key, null, options);

        // Assert - Should not store null value, key should not exist
        var result = await _distributedCache.GetAsync(key);
        result.Should().BeNull();
    }
}
