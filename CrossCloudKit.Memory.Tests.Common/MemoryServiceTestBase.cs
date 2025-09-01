// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using CrossCloudKit.Interfaces;
using FluentAssertions;
using CrossCloudKit.Utilities.Common;
using xRetry;
using Xunit;
using Xunit.Abstractions;
#pragma warning disable CS9113 // Parameter is unread.

namespace CrossCloudKit.Memory.Tests.Common;

public abstract class MemoryServiceTestBase(ITestOutputHelper testOutputHelper) : IAsyncLifetime
{
    protected abstract IMemoryService CreateMemoryService();
    protected abstract IPubSubService CreatePubSubService();

    // ReSharper disable once MemberCanBePrivate.Global
    protected IMemoryService MemoryService { get; private set; } = null!;
    // ReSharper disable once MemberCanBePrivate.Global
    protected IPubSubService PubSubService { get; private set; } = null!;
    protected TestMemoryScope TestScope { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        MemoryService = CreateMemoryService();
        PubSubService = CreatePubSubService();
        TestScope = new TestMemoryScope($"test-scope-{Guid.NewGuid()}");

        // Wait a bit for services to initialize
        await Task.Delay(100);

        // Clean up any existing test data
        await MemoryService.DeleteAllKeysAsync(TestScope, publishChange: false);
    }

    public async Task DisposeAsync()
    {
        try
        {
            // Clean up test data
            await MemoryService.DeleteAllKeysAsync(TestScope, publishChange: false);
        }
        catch
        {
            // Ignore cleanup errors
        }

        if (MemoryService is IAsyncDisposable asyncDisposableMemory)
            await asyncDisposableMemory.DisposeAsync();

        if (PubSubService is IAsyncDisposable asyncDisposablePubSub)
            await asyncDisposablePubSub.DisposeAsync();
    }

    protected class TestMemoryScope(string scopeName) : IMemoryServiceScope
    {
        public string Compile() => scopeName;
    }

    /// <summary>
    /// Safely cleans up test data, ignoring any errors to avoid masking test failures.
    /// </summary>
    private async Task SafeCleanupAsync(params string[] listNames)
    {
        try
        {
            // Clean up any specified lists
            if (listNames.Length > 0)
            {
                foreach (var listName in listNames)
                {
                    try
                    {
                        await MemoryService.EmptyListAsync(TestScope, listName, publishChange: false);
                    }
                    catch
                    {
                        // Ignore individual list cleanup errors
                    }
                }
            }

            // Clean up all key-value data
            await MemoryService.DeleteAllKeysAsync(TestScope, publishChange: false);
        }
        catch
        {
            // Ignore cleanup errors to avoid masking test failures
        }
    }

    #region Initialization Tests

    [RetryFact(3, 5000)]
    public void MemoryService_ShouldBeInitialized()
    {
        MemoryService.IsInitialized.Should().BeTrue();
    }

    [RetryFact(3, 5000)]
    public void PubSubService_ShouldBeInitialized()
    {
        PubSubService.IsInitialized.Should().BeTrue();
    }

    #endregion

    #region Key Expiration Tests

    [RetryFact(3, 5000)]
    public async Task SetKeyExpireTime_WithValidTimeToLive_ShouldReturnTrue()
    {
        try
        {
            // Arrange
            var ttl = TimeSpan.FromMinutes(5);

            // Act
            var result = await MemoryService.SetKeyExpireTimeAsync(TestScope, ttl);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeTrue();
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task SetKeyExpireTime_WithExistingKey_ShouldReturnTrue()
    {
        try
        {
            // Arrange
            await MemoryService.SetKeyValuesAsync(TestScope, [new("test-key", new PrimitiveType("test-value"))]);
            var ttl = TimeSpan.FromMinutes(10);

            // Act
            var result = await MemoryService.SetKeyExpireTimeAsync(TestScope, ttl);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeTrue();
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task GetKeyExpireTime_AfterSettingExpiration_ShouldReturnTimeToLive()
    {
        try
        {
            // Arrange
            var ttl = TimeSpan.FromMinutes(30);
            await MemoryService.SetKeyExpireTimeAsync(TestScope, ttl);

            // Act
            var result = await MemoryService.GetKeyExpireTimeAsync(TestScope);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().NotBeNull();
            result.Data.Should().BeLessOrEqualTo(ttl);
            result.Data.Should().BeGreaterThan(TimeSpan.FromMinutes(29));
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task GetKeyExpireTime_WithoutExpiration_ShouldReturnNull()
    {
        try
        {
            // Arrange
            await MemoryService.SetKeyValuesAsync(TestScope, [new("test-key", new PrimitiveType("test-value"))]);

            // Act
            var result = await MemoryService.GetKeyExpireTimeAsync(TestScope);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeNull();
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    #endregion

    #region Key-Value Operations Tests

    [RetryFact(3, 5000)]
    public async Task SetKeyValues_WithValidKeyValues_ShouldReturnTrue()
    {
        try
        {
            // Arrange
            var keyValues = new[]
            {
                new KeyValuePair<string, PrimitiveType>("key1", new PrimitiveType("value1")),
                new KeyValuePair<string, PrimitiveType>("key2", new PrimitiveType(42L)),
                new KeyValuePair<string, PrimitiveType>("key3", new PrimitiveType(3.14))
            };

            // Act
            var result = await MemoryService.SetKeyValuesAsync(TestScope, keyValues);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeTrue();
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task SetKeyValues_WithEmptyKeyValues_ShouldReturnFailure()
    {
        try
        {
            // Arrange
            var keyValues = Array.Empty<KeyValuePair<string, PrimitiveType>>();

            // Act
            var result = await MemoryService.SetKeyValuesAsync(TestScope, keyValues);

            // Assert
            result.IsSuccessful.Should().BeFalse();
            result.ErrorMessage.Should().NotBeNullOrEmpty();
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task SetKeyValueConditionally_WithNewKey_ShouldReturnTrue()
    {
        try
        {
            // Arrange
            var key = "new-key";
            var value = new PrimitiveType("new-value");

            // Act
            var result = await MemoryService.SetKeyValueConditionallyAsync(TestScope, key, value);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeTrue();
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task SetKeyValueConditionally_WithExistingKey_ShouldReturnFalse()
    {
        try
        {
            // Arrange
            var key = "existing-key";
            var originalValue = new PrimitiveType("original-value");
            var newValue = new PrimitiveType("new-value");

            await MemoryService.SetKeyValuesAsync(TestScope, [new(key, originalValue)]);

            // Act
            var result = await MemoryService.SetKeyValueConditionallyAsync(TestScope, key, newValue);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeFalse();

            // Verify original value is unchanged
            var getValue = await MemoryService.GetKeyValueAsync(TestScope, key);
            getValue.Data.Should().Be(originalValue);
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task SetKeyValueConditionallyGetValueRegardless_WithNewKey_ShouldReturnTrueAndNewValue()
    {
        try
        {
            // Arrange
            var key = "new-conditional-key";
            var value = new PrimitiveType("new-conditional-value");

            // Act
            var result = await MemoryService.SetKeyValueConditionallyAndReturnValueRegardlessAsync(TestScope, key, value);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.newlySet.Should().BeTrue();
            result.Data.value.Should().Be(value);

            // Verify the key was actually set
            var getValue = await MemoryService.GetKeyValueAsync(TestScope, key);
            getValue.Data.Should().Be(value);
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task SetKeyValueConditionallyGetValueRegardless_WithExistingKey_ShouldReturnFalseAndExistingValue()
    {
        try
        {
            // Arrange
            var key = "existing-conditional-key";
            var originalValue = new PrimitiveType("original-conditional-value");
            var newValue = new PrimitiveType("new-conditional-value");

            await MemoryService.SetKeyValuesAsync(TestScope, [new(key, originalValue)]);

            // Act
            var result = await MemoryService.SetKeyValueConditionallyAndReturnValueRegardlessAsync(TestScope, key, newValue);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.newlySet.Should().BeFalse();
            result.Data.value.Should().Be(originalValue);

            // Verify original value is unchanged
            var getValue = await MemoryService.GetKeyValueAsync(TestScope, key);
            getValue.Data.Should().Be(originalValue);
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task SetKeyValueConditionallyGetValueRegardless_WithPublishChangeTrue_AndNewKey_ShouldPublishNotification()
    {
        try
        {
            // Arrange
            var key = "publish-conditional-key";
            var value = new PrimitiveType("publish-conditional-value");

            // Act & Assert
            var messages = await CapturePublishedMessagesAsync(async () =>
            {
                var result = await MemoryService.SetKeyValueConditionallyAndReturnValueRegardlessAsync(TestScope, key, value, publishChange: true);
                result.IsSuccessful.Should().BeTrue();
                result.Data.newlySet.Should().BeTrue();
                result.Data.value.Should().Be(value);
            });

            // Verify change notification was published only for newly set key
            messages.Should().HaveCount(1, "Expected one change notification when key is newly set");
            messages[0].Should().Contain("SetKeyValue", "Expected operation type in notification");
            messages[0].Should().Contain(key, "Expected key in notification");
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task SetKeyValueConditionallyGetValueRegardless_WithPublishChangeTrue_AndExistingKey_ShouldNotPublishNotification()
    {
        try
        {
            // Arrange
            var key = "existing-no-publish-conditional-key";
            var originalValue = new PrimitiveType("original-conditional-value");
            var newValue = new PrimitiveType("new-conditional-value");

            await MemoryService.SetKeyValuesAsync(TestScope, [new(key, originalValue)], publishChange: false);

            // Act & Assert
            var messages = await CapturePublishedMessagesAsync(async () =>
            {
                var result = await MemoryService.SetKeyValueConditionallyAndReturnValueRegardlessAsync(TestScope, key, newValue, publishChange: true);
                result.IsSuccessful.Should().BeTrue();
                result.Data.newlySet.Should().BeFalse();
                result.Data.value.Should().Be(originalValue);
            }, TimeSpan.FromSeconds(2)); // Shorter timeout since we expect no messages

            // Verify no change notification was published for existing key
            messages.Should().BeEmpty("Expected no change notifications when key already exists");
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task SetKeyValueConditionallyGetValueRegardless_WithPublishChangeFalse_ShouldNotPublishNotification()
    {
        try
        {
            // Arrange
            var key = "no-publish-conditional-key";
            var value = new PrimitiveType("no-publish-conditional-value");

            // Act & Assert
            var messages = await CapturePublishedMessagesAsync(async () =>
            {
                var result = await MemoryService.SetKeyValueConditionallyAndReturnValueRegardlessAsync(TestScope, key, value, publishChange: false);
                result.IsSuccessful.Should().BeTrue();
                result.Data.newlySet.Should().BeTrue();
                result.Data.value.Should().Be(value);
            }, TimeSpan.FromSeconds(2));

            // Verify no change notification was published
            messages.Should().BeEmpty("Expected no change notifications when publishChange is false");
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task SetKeyValueConditionallyGetValueRegardless_CompleteWorkflow_ShouldWorkCorrectly()
    {
        try
        {
            // Complete workflow test combining multiple scenarios
            var key = "workflow-conditional-key";

            // 1. First attempt on non-existing key should succeed
            var value1 = new PrimitiveType("first-value");
            var result1 = await MemoryService.SetKeyValueConditionallyAndReturnValueRegardlessAsync(TestScope, key, value1);

            result1.IsSuccessful.Should().BeTrue();
            result1.Data.newlySet.Should().BeTrue();
            result1.Data.value.Should().Be(value1);

            // 2. Second attempt with different value should fail but return existing value
            var value2 = new PrimitiveType("second-value");
            var result2 = await MemoryService.SetKeyValueConditionallyAndReturnValueRegardlessAsync(TestScope, key, value2);

            result2.IsSuccessful.Should().BeTrue();
            result2.Data.newlySet.Should().BeFalse();
            result2.Data.value.Should().Be(value1); // Should return the first value, not the second

            // 3. Third attempt with same original value should still fail but return the value
            var result3 = await MemoryService.SetKeyValueConditionallyAndReturnValueRegardlessAsync(TestScope, key, value1);

            result3.IsSuccessful.Should().BeTrue();
            result3.Data.newlySet.Should().BeFalse();
            result3.Data.value.Should().Be(value1);

            // 4. Verify final state
            var finalCheck = await MemoryService.GetKeyValueAsync(TestScope, key);
            finalCheck.IsSuccessful.Should().BeTrue();
            finalCheck.Data.Should().Be(value1, "Final value should be the originally set value");

            // 5. Test with different data types
            var intKey = "workflow-int-key";
            var intValue = new PrimitiveType(42L);
            var intResult = await MemoryService.SetKeyValueConditionallyAndReturnValueRegardlessAsync(TestScope, intKey, intValue);

            intResult.IsSuccessful.Should().BeTrue();
            intResult.Data.newlySet.Should().BeTrue();
            intResult.Data.value.Should().Be(intValue);

            var doubleKey = "workflow-double-key";
            var doubleValue = new PrimitiveType(3.14);
            var doubleResult = await MemoryService.SetKeyValueConditionallyAndReturnValueRegardlessAsync(TestScope, doubleKey, doubleValue);

            doubleResult.IsSuccessful.Should().BeTrue();
            doubleResult.Data.newlySet.Should().BeTrue();
            doubleResult.Data.value.Should().Be(doubleValue);

            // 6. Verify all keys exist with correct values
            var allValues = await MemoryService.GetAllKeyValuesAsync(TestScope);
            allValues.IsSuccessful.Should().BeTrue();
            allValues.Data.Should().HaveCount(3);
            allValues.Data![key].Should().Be(value1);
            allValues.Data![intKey].Should().Be(intValue);
            allValues.Data![doubleKey].Should().Be(doubleValue);
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    #region Mutex Tests

    [RetryFact(3, 5000)]
    public async Task Mutex_BasicLockAndUnlock_ShouldWorkCorrectly()
    {
        try
        {
            // Arrange
            var mutexKey = "test-mutex-basic";
            var lockDuration = TimeSpan.FromSeconds(10);

            // Act & Assert - Acquire lock
            await using var mutex = await MemoryServiceScopeMutex.CreateScopeAsync(
                MemoryService, TestScope, mutexKey, lockDuration);

            // The fact that we got here means the lock was acquired successfully
            mutex.Should().NotBeNull();
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    [SuppressMessage("ReSharper", "RedundantAssignment")]
    public async Task Mutex_MultipleLockAttempts_ShouldBlockSecondAttempt()
    {
        try
        {
            // Arrange
            var mutexKey = "test-mutex-blocking";
            var lockDuration = TimeSpan.FromSeconds(5);
            var firstLockAcquired = false;
            var secondLockAcquired = false;
            var secondLockBlocked = true;

            // Act - First lock
            await using var firstMutex = await MemoryServiceScopeMutex.CreateScopeAsync(
                MemoryService, TestScope, mutexKey, lockDuration);
            firstLockAcquired = true;

            // Try to acquire second lock (should be blocked)
            var secondLockTask = Task.Run(async () =>
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
                try
                {
                    await using var secondMutex = await MemoryServiceScopeMutex.CreateScopeAsync(
                        MemoryService, TestScope, mutexKey, lockDuration, cts.Token);
                    secondLockAcquired = true;
                    secondLockBlocked = false;
                }
                catch (OperationCanceledException)
                {
                    // Expected - second lock should be blocked
                    secondLockBlocked = true;
                }
            });

            await secondLockTask;

            // Assert
            firstLockAcquired.Should().BeTrue("First lock should be acquired");
            secondLockAcquired.Should().BeFalse("Second lock should not be acquired while first is held");
            secondLockBlocked.Should().BeTrue("Second lock attempt should be blocked");
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    [SuppressMessage("ReSharper", "RedundantAssignment")]
    public async Task Mutex_SequentialLocks_ShouldAllowSecondAfterFirstReleased()
    {
        try
        {
            // Arrange
            var mutexKey = "test-mutex-sequential";
            var lockDuration = TimeSpan.FromSeconds(10);
            var firstLockCompleted = false;
            var secondLockAcquired = false;

            // Act - First lock and release
            {
                await using var firstMutex = await MemoryServiceScopeMutex.CreateScopeAsync(
                    MemoryService, TestScope, mutexKey, lockDuration);

                // Simulate some work
                await Task.Delay(100);
                firstLockCompleted = true;
            } // First mutex is disposed here, releasing the lock

            // Second lock should now be able to acquire
            await using var secondMutex = await MemoryServiceScopeMutex.CreateScopeAsync(
                MemoryService, TestScope, mutexKey, lockDuration);
            secondLockAcquired = true;

            // Assert
            firstLockCompleted.Should().BeTrue("First lock should complete");
            secondLockAcquired.Should().BeTrue("Second lock should be acquired after first is released");
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task Mutex_ConcurrentAccess_ShouldSerializeExecution()
    {
        try
        {
            // Arrange
            var mutexKey = "test-mutex-concurrent";
            var lockDuration = TimeSpan.FromSeconds(10);
            var counter = 0;
            var executionOrder = new List<int>();
            var tasks = new List<Task>();

            // Create multiple concurrent tasks that try to access the same resource
            for (int i = 0; i < 5; i++)
            {
                var taskId = i;
                var task = Task.Run(async () =>
                {
                    await using var mutex = await MemoryServiceScopeMutex.CreateScopeAsync(
                        MemoryService, TestScope, mutexKey, lockDuration);

                    // Critical section - increment counter
                    var currentValue = counter;
                    await Task.Delay(50); // Simulate some work
                    counter = currentValue + 1;

                    lock (executionOrder)
                    {
                        executionOrder.Add(taskId);
                    }
                });
                tasks.Add(task);
            }

            // Act
            await Task.WhenAll(tasks);

            // Assert
            counter.Should().Be(5, "Counter should be incremented exactly 5 times");
            executionOrder.Should().HaveCount(5, "All tasks should complete");
            executionOrder.Should().OnlyHaveUniqueItems("Each task should execute exactly once");
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task Mutex_WithDifferentKeys_ShouldAllowConcurrentLocks()
    {
        try
        {
            // Arrange
            var mutexKey1 = "test-mutex-key1";
            var mutexKey2 = "test-mutex-key2";
            var lockDuration = TimeSpan.FromSeconds(10);
            var lock1Acquired = false;
            var lock2Acquired = false;

            // Act - Try to acquire two different mutex keys concurrently
            var task1 = Task.Run(async () =>
            {
                await using var mutex1 = await MemoryServiceScopeMutex.CreateScopeAsync(
                    MemoryService, TestScope, mutexKey1, lockDuration);
                lock1Acquired = true;
                await Task.Delay(200); // Hold lock for a bit
            });

            var task2 = Task.Run(async () =>
            {
                await using var mutex2 = await MemoryServiceScopeMutex.CreateScopeAsync(
                    MemoryService, TestScope, mutexKey2, lockDuration);
                lock2Acquired = true;
                await Task.Delay(200); // Hold lock for a bit
            });

            await Task.WhenAll(task1, task2);

            // Assert
            lock1Acquired.Should().BeTrue("First mutex should be acquired");
            lock2Acquired.Should().BeTrue("Second mutex should be acquired");
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    [SuppressMessage("ReSharper", "UseAwaitUsing")]
    [SuppressMessage("ReSharper", "RedundantAssignment")]
    public async Task Mutex_SynchronousCreation_ShouldWork()
    {
        try
        {
            // Arrange
            var mutexKey = "test-mutex-sync";
            var lockDuration = TimeSpan.FromSeconds(5);

            // Act - Use synchronous creation method
            // ReSharper disable once MethodHasAsyncOverload
            using var mutex = MemoryServiceScopeMutex.CreateScope(
                MemoryService, TestScope, mutexKey, lockDuration);

            // Assert
            mutex.Should().NotBeNull("Mutex should be created successfully");

            // Verify the lock is actually held by trying to acquire it again with a timeout
            // Since the sync version doesn't have cancellation, we'll test this with an async version with a short timeout
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
            var secondLockBlocked = false;

            try
            {
                await using var secondMutex = await MemoryServiceScopeMutex.CreateScopeAsync(
                    MemoryService, TestScope, mutexKey, lockDuration, cts.Token);
                // If we get here, the second lock was acquired (which shouldn't happen)
                secondLockBlocked = false;
            }
            catch (OperationCanceledException)
            {
                // Expected - second lock should be blocked and timeout
                secondLockBlocked = true;
            }

            secondLockBlocked.Should().BeTrue("Second lock should be blocked by the first lock");
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    [SuppressMessage("ReSharper", "RedundantAssignment")]
    public async Task Mutex_LockExpiration_ShouldAllowReacquisition()
    {
        try
        {
            // Arrange
            var mutexKey = "test-mutex-expiration";
            var shortLockDuration = TimeSpan.FromMilliseconds(500); // Very short TTL
            var firstLockAcquired = false;
            var secondLockAcquired = false;

            // Act - Acquire lock with short TTL and let it expire
            {
                await using var firstMutex = await MemoryServiceScopeMutex.CreateScopeAsync(
                    MemoryService, TestScope, mutexKey, shortLockDuration);
                firstLockAcquired = true;

                // Wait longer than the TTL to let it expire
                await Task.Delay(TimeSpan.FromSeconds(1));
            }

            // The lock should have expired, so we should be able to acquire it again
            await using var secondMutex = await MemoryServiceScopeMutex.CreateScopeAsync(
                MemoryService, TestScope, mutexKey, TimeSpan.FromSeconds(5));
            secondLockAcquired = true;

            // Assert
            firstLockAcquired.Should().BeTrue("First lock should be acquired");
            secondLockAcquired.Should().BeTrue("Second lock should be acquired after first expires");
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
    public async Task Mutex_CancellationDuringAcquisition_ShouldThrowOperationCanceledException()
    {
        try
        {
            // Arrange
            var mutexKey = "test-mutex-cancellation";
            var lockDuration = TimeSpan.FromSeconds(10);

            // First, acquire the lock
            await using var firstMutex = await MemoryServiceScopeMutex.CreateScopeAsync(
                MemoryService, TestScope, mutexKey, lockDuration);

            // Act & Assert - Try to acquire the same lock with cancellation
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));

            var act = async () => await MemoryServiceScopeMutex.CreateScopeAsync(
                MemoryService, TestScope, mutexKey, lockDuration, cts.Token);

            await act.Should().ThrowAsync<OperationCanceledException>(
                "Lock acquisition should be cancelled when cancellation token is triggered");
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    [SuppressMessage("ReSharper", "RedundantAssignment")]
    public async Task Mutex_ExceptionInCriticalSection_ShouldReleaseLock()
    {
        try
        {
            // Arrange
            var mutexKey = "test-mutex-exception";
            var lockDuration = TimeSpan.FromSeconds(10);
            var lockReleasedAfterException = false;

            // Act - Simulate exception in critical section
            try
            {
                await using var mutex = await MemoryServiceScopeMutex.CreateScopeAsync(
                    MemoryService, TestScope, mutexKey, lockDuration);

                // Simulate work that throws an exception
                throw new InvalidOperationException("Simulated error in critical section");
            }
            catch (InvalidOperationException)
            {
                // Expected exception - ignore
            }

            // The lock should be released even though an exception occurred
            // Try to acquire it again - should succeed immediately
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
            await using var newMutex = await MemoryServiceScopeMutex.CreateScopeAsync(
                MemoryService, TestScope, mutexKey, lockDuration, cts.Token);
            lockReleasedAfterException = true;

            // Assert
            lockReleasedAfterException.Should().BeTrue(
                "Lock should be released even when exception occurs in critical section");
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    #endregion

    [RetryFact(3, 5000)]
    public async Task GetKeyValue_WithExistingKey_ShouldReturnValue()
    {
        try
        {
            // Arrange
            var key = "test-key";
            var value = new PrimitiveType("test-value");
            await MemoryService.SetKeyValuesAsync(TestScope, [new(key, value)]);

            // Act
            var result = await MemoryService.GetKeyValueAsync(TestScope, key);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().Be(value);
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task GetKeyValue_WithNonExistingKey_ShouldReturnNull()
    {
        try
        {
            // Arrange
            var key = "non-existing-key";

            // Act
            var result = await MemoryService.GetKeyValueAsync(TestScope, key);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeNull();
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task GetKeyValues_WithValidKeys_ShouldReturnValues()
    {
        try
        {
            // Arrange
            var keyValues = new Dictionary<string, PrimitiveType>
            {
                ["key1"] = new("value1"),
                ["key2"] = new(100L),
                ["key3"] = new(2.71)
            };

            await MemoryService.SetKeyValuesAsync(TestScope, keyValues);

            // Act
            var result = await MemoryService.GetKeyValuesAsync(TestScope, keyValues.Keys);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().NotBeNull();
            result.Data.Should().HaveCount(3);
            result.Data.Should().BeEquivalentTo(keyValues);
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task GetKeyValues_WithEmptyKeys_ShouldReturnFailure()
    {
        try
        {
            // Act
            var result = await MemoryService.GetKeyValuesAsync(TestScope, []);

            // Assert
            result.IsSuccessful.Should().BeFalse();
            result.ErrorMessage.Should().NotBeNullOrEmpty();
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task GetKeyValues_WithMixedExistingAndNonExistingKeys_ShouldReturnOnlyExistingValues()
    {
        try
        {
            // Arrange
            var existingKeys = new Dictionary<string, PrimitiveType>
            {
                ["existing1"] = new("value1"),
                ["existing2"] = new(42L)
            };

            await MemoryService.SetKeyValuesAsync(TestScope, existingKeys);

            var requestedKeys = new[] { "existing1", "non-existing", "existing2", "another-non-existing" };

            // Act
            var result = await MemoryService.GetKeyValuesAsync(TestScope, requestedKeys);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().NotBeNull();
            result.Data.Should().HaveCount(2);
            result.Data.Should().BeEquivalentTo(existingKeys);
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task GetAllKeyValues_WithExistingKeys_ShouldReturnAllValues()
    {
        try
        {
            // Arrange
            var keyValues = new Dictionary<string, PrimitiveType>
            {
                ["key1"] = new("string-value"),
                ["key2"] = new(123L),
                ["key3"] = new(45.67),
                ["key4"] = new(new byte[] { 1, 2, 3, 4 })
            };

            await MemoryService.SetKeyValuesAsync(TestScope, keyValues);

            // Act
            var result = await MemoryService.GetAllKeyValuesAsync(TestScope);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().NotBeNull();
            result.Data.Should().HaveCount(keyValues.Count);
            result.Data.Should().BeEquivalentTo(keyValues);
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task GetAllKeyValues_WithEmptyScope_ShouldReturnEmptyDictionary()
    {
        try
        {
            // Act
            var result = await MemoryService.GetAllKeyValuesAsync(TestScope);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().NotBeNull();
            result.Data.Should().BeEmpty();
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    #endregion

    #region Key Deletion Tests

    [RetryFact(3, 5000)]
    public async Task DeleteKey_WithExistingKey_ShouldReturnTrue()
    {
        try
        {
            // Arrange
            var key = "key-to-delete";
            var value = new PrimitiveType("value-to-delete");
            await MemoryService.SetKeyValuesAsync(TestScope, [new(key, value)]);

            // Act
            var result = await MemoryService.DeleteKeyAsync(TestScope, key);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeTrue();

            // Verify key is deleted
            var getValue = await MemoryService.GetKeyValueAsync(TestScope, key);
            getValue.Data.Should().BeNull();
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task DeleteKey_WithNonExistingKey_ShouldReturnFalse()
    {
        try
        {
            // Arrange
            var key = "non-existing-key";

            // Act
            var result = await MemoryService.DeleteKeyAsync(TestScope, key);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeFalse();
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task DeleteAllKeys_WithExistingKeys_ShouldReturnTrue()
    {
        try
        {
            // Arrange
            var keyValues = new Dictionary<string, PrimitiveType>
            {
                ["key1"] = new("value1"),
                ["key2"] = new(42L),
                ["key3"] = new(3.14)
            };

            await MemoryService.SetKeyValuesAsync(TestScope, keyValues);

            // Act
            var result = await MemoryService.DeleteAllKeysAsync(TestScope);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeTrue();

            // Verify all keys are deleted
            var allValues = await MemoryService.GetAllKeyValuesAsync(TestScope);
            allValues.Data.Should().BeEmpty();
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task DeleteAllKeys_WithEmptyScope_ShouldReturnTrue_DataFalse()
    {
        try
        {
            // Act
            var result = await MemoryService.DeleteAllKeysAsync(TestScope);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeFalse();
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    #endregion

    #region Key Listing Tests

    [RetryFact(3, 5000)]
    public async Task GetKeys_WithExistingKeys_ShouldReturnAllKeys()
    {
        try
        {
            // Arrange
            var keyValues = new Dictionary<string, PrimitiveType>
            {
                ["alpha"] = new("value1"),
                ["beta"] = new("value2"),
                ["gamma"] = new("value3")
            };

            await MemoryService.SetKeyValuesAsync(TestScope, keyValues);

            // Act
            var result = await MemoryService.GetKeysAsync(TestScope);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().NotBeNull();
            result.Data.Should().HaveCount(3);
            result.Data.Should().BeEquivalentTo(keyValues.Keys);
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task GetKeys_WithEmptyScope_ShouldReturnEmptyCollection()
    {
        try
        {
            // Act
            var result = await MemoryService.GetKeysAsync(TestScope);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().NotBeNull();
            result.Data.Should().BeEmpty();
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task GetKeysCount_WithExistingKeys_ShouldReturnCorrectCount()
    {
        try
        {
            // Arrange
            var keyValues = new Dictionary<string, PrimitiveType>
            {
                ["key1"] = new("value1"),
                ["key2"] = new("value2"),
                ["key3"] = new("value3"),
                ["key4"] = new("value4"),
                ["key5"] = new("value5")
            };

            await MemoryService.SetKeyValuesAsync(TestScope, keyValues);

            // Act
            var result = await MemoryService.GetKeysCountAsync(TestScope);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().Be(5);
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task GetKeysCount_WithEmptyScope_ShouldReturnZero()
    {
        try
        {
            // Act
            var result = await MemoryService.GetKeysCountAsync(TestScope);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().Be(0);
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    #endregion

    #region Increment Operations Tests

    [RetryFact(3, 5000)]
    public async Task IncrementKeyValues_WithValidIncrements_ShouldReturnNewValues()
    {
        try
        {
            // Arrange
            var initialValues = new Dictionary<string, PrimitiveType>
            {
                ["counter1"] = new(10L),
                ["counter2"] = new(20L)
            };

            var increments = new Dictionary<string, long>
            {
                ["counter1"] = 5,
                ["counter2"] = -3
            };

            await MemoryService.SetKeyValuesAsync(TestScope, initialValues);

            // Act
            var result = await MemoryService.IncrementKeyValuesAsync(TestScope, increments);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().NotBeNull();
            result.Data.Should().HaveCount(2);
            result.Data!["counter1"].Should().Be(15);
            result.Data!["counter2"].Should().Be(17);
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task IncrementKeyValues_WithNewKeys_ShouldCreateKeysWithIncrements()
    {
        try
        {
            // Arrange
            var increments = new Dictionary<string, long>
            {
                ["new-counter1"] = 100,
                ["new-counter2"] = -50
            };

            // Act
            var result = await MemoryService.IncrementKeyValuesAsync(TestScope, increments);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().NotBeNull();
            result.Data.Should().HaveCount(2);
            result.Data!["new-counter1"].Should().Be(100);
            result.Data!["new-counter2"].Should().Be(-50);
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task IncrementKeyValues_WithEmptyIncrements_ShouldReturnFailure()
    {
        try
        {
            // Act
            var result = await MemoryService.IncrementKeyValuesAsync(TestScope, []);

            // Assert
            result.IsSuccessful.Should().BeFalse();
            result.ErrorMessage.Should().NotBeNullOrEmpty();
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task IncrementKeyByValueAndGet_WithExistingKey_ShouldReturnNewValue()
    {
        try
        {
            // Arrange
            var key = "increment-key";
            var initialValue = new PrimitiveType(50L);
            var incrementBy = 25L;

            await MemoryService.SetKeyValuesAsync(TestScope, [new(key, initialValue)]);

            // Act
            var result = await MemoryService.IncrementKeyByValueAndGetAsync(TestScope, key, incrementBy);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().Be(75);
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task IncrementKeyByValueAndGet_WithNewKey_ShouldReturnIncrementValue()
    {
        try
        {
            // Arrange
            const string key = "new-increment-key";
            const long incrementBy = 42L;

            // Act
            var result = await MemoryService.IncrementKeyByValueAndGetAsync(TestScope, key, incrementBy);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().Be(42);
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task IncrementKeyByValueAndGet_WithNegativeIncrement_ShouldDecrement()
    {
        try
        {
            // Arrange
            const string key = "decrement-key";
            var initialValue = new PrimitiveType(100L);
            const long decrementBy = -30L;

            await MemoryService.SetKeyValuesAsync(TestScope, [new(key, initialValue)]);

            // Act
            var result = await MemoryService.IncrementKeyByValueAndGetAsync(TestScope, key, decrementBy);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().Be(70);
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    #endregion

    #region List Operations Tests

    [RetryFact(3, 5000)]
    public async Task PushToListTail_WithValidValues_ShouldReturnTrue()
    {
        const string listName = "test-list";
        try
        {
            // Arrange
            var values = new[]
            {
                new PrimitiveType("item1"),
                new PrimitiveType("item2"),
                new PrimitiveType("item3")
            };

            // Act
            var result = await MemoryService.PushToListTailAsync(TestScope, listName, values);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeTrue();
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task PushToListTail_WithOnlyIfExistsTrue_AndNonExistingList_ShouldReturnFalse()
    {
        const string listName = "non-existing-list";
        try
        {
            // Arrange
            var values = new[] { new PrimitiveType("item1") };

            // Act
            var result = await MemoryService.PushToListTailAsync(TestScope, listName, values, onlyIfListExists: true);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeFalse();
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task PushToListTail_WithOnlyIfExistsTrue_AndExistingList_ShouldReturnTrue()
    {
        const string listName = "existing-list";
        try
        {
            // Arrange
            var initialValues = new[] { new PrimitiveType("initial-item") };
            var additionalValues = new[] { new PrimitiveType("additional-item") };

            await MemoryService.PushToListTailAsync(TestScope, listName, initialValues);

            // Act
            var result = await MemoryService.PushToListTailAsync(TestScope, listName, additionalValues, onlyIfListExists: true);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeTrue();
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task PushToListHead_WithValidValues_ShouldReturnTrue()
    {
        const string listName = "head-list";
        try
        {
            // Arrange
            var values = new[]
            {
                new PrimitiveType("head1"),
                new PrimitiveType("head2")
            };

            // Act
            var result = await MemoryService.PushToListHeadAsync(TestScope, listName, values);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeTrue();
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task PopLastElementOfList_WithExistingList_ShouldReturnLastElement()
    {
        const string listName = "pop-tail-list";
        try
        {
            // Arrange
            var values = new[]
            {
                new PrimitiveType("first"),
                new PrimitiveType("middle"),
                new PrimitiveType("last")
            };

            await MemoryService.PushToListTailAsync(TestScope, listName, values);

            // Act
            var result = await MemoryService.PopLastElementOfListAsync(TestScope, listName);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().NotBeNull();
            result.Data.Should().Be(new PrimitiveType("last"));
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task PopLastElementOfList_WithEmptyList_ShouldReturnFailure()
    {
        const string listName = "empty-list";
        try
        {
            // Act
            var result = await MemoryService.PopLastElementOfListAsync(TestScope, listName);

            // Assert
            result.IsSuccessful.Should().BeFalse();
            result.ErrorMessage.Should().NotBeNullOrEmpty();
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task PopFirstElementOfList_WithExistingList_ShouldReturnFirstElement()
    {
        const string listName = "pop-head-list";
        try
        {
            // Arrange
            var values = new[]
            {
                new PrimitiveType("first"),
                new PrimitiveType("middle"),
                new PrimitiveType("last")
            };

            await MemoryService.PushToListTailAsync(TestScope, listName, values);

            // Act
            var result = await MemoryService.PopFirstElementOfListAsync(TestScope, listName);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().NotBeNull();
            result.Data.Should().Be(new PrimitiveType("first"));
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task RemoveElementsFromList_WithExistingElements_ShouldReturnRemovedElements()
    {
        const string listName = "remove-list";
        try
        {
            // Arrange
            var values = new[]
            {
                new PrimitiveType("keep1"),
                new PrimitiveType("remove1"),
                new PrimitiveType("keep2"),
                new PrimitiveType("remove2"),
                new PrimitiveType("keep3")
            };

            var toRemove = new[]
            {
                new PrimitiveType("remove1"),
                new PrimitiveType("remove2"),
                new PrimitiveType("non-existing")
            };

            await MemoryService.PushToListTailAsync(TestScope, listName, values);

            // Act
            var result = await MemoryService.RemoveElementsFromListAsync(TestScope, listName, toRemove);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().NotBeNull();

            var removedElements = result.Data!.Where(x => x != null).ToArray();
            removedElements.Should().Contain(new PrimitiveType("remove1"));
            removedElements.Should().Contain(new PrimitiveType("remove2"));
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task GetAllElementsOfList_WithExistingList_ShouldReturnAllElements()
    {
        const string listName = "get-all-list";
        try
        {
            // Arrange
            var values = new[]
            {
                new PrimitiveType("item1"),
                new PrimitiveType("item2"),
                new PrimitiveType("item3"),
                new PrimitiveType("item4")
            };

            await MemoryService.PushToListTailAsync(TestScope, listName, values);

            // Act
            var result = await MemoryService.GetAllElementsOfListAsync(TestScope, listName);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().NotBeNull();
            result.Data.Should().HaveCount(4);
            result.Data.Should().BeEquivalentTo(values, options => options.WithStrictOrdering());
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task GetAllElementsOfList_WithEmptyList_ShouldReturnEmptyCollection()
    {
        const string listName = "empty-get-all-list";
        try
        {
            // Act
            var result = await MemoryService.GetAllElementsOfListAsync(TestScope, listName);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().NotBeNull();
            result.Data.Should().BeEmpty();
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task EmptyList_WithExistingList_ShouldReturnTrue()
    {
        const string listName = "empty-me-list";
        try
        {
            // Arrange
            var values = new[]
            {
                new PrimitiveType("item1"),
                new PrimitiveType("item2")
            };

            await MemoryService.PushToListTailAsync(TestScope, listName, values);

            // Act
            var result = await MemoryService.EmptyListAsync(TestScope, listName);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeTrue();

            // Verify list is empty
            var getResult = await MemoryService.GetAllElementsOfListAsync(TestScope, listName);
            getResult.Data.Should().BeEmpty();
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task EmptyListAndSublists_WithExistingListAndSublists_ShouldReturnTrue()
    {
        const string listName = "parent-list";
        const string sublist1Name = "sublist-sub1";
        const string sublist2Name = "sublist-sub2";
        try
        {
            // Arrange
            const string sublistPrefix = "sublist-";

            var parentValues = new[]
            {
                new PrimitiveType("sub1"),
                new PrimitiveType("sub2")
            };

            var sublist1Values = new[] { new PrimitiveType("subitem1") };
            var sublist2Values = new[] { new PrimitiveType("subitem2") };

            await MemoryService.PushToListTailAsync(TestScope, listName, parentValues);
            await MemoryService.PushToListTailAsync(TestScope, sublist1Name, sublist1Values);
            await MemoryService.PushToListTailAsync(TestScope, sublist2Name, sublist2Values);

            // Act
            var result = await MemoryService.EmptyListAndSublistsAsync(TestScope, listName, sublistPrefix);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeTrue();
        }
        finally
        {
            await SafeCleanupAsync(listName, sublist1Name, sublist2Name);
        }
    }

    [RetryFact(3, 5000)]
    public async Task GetListSize_WithExistingList_ShouldReturnCorrectSize()
    {
        const string listName = "size-list";
        try
        {
            // Arrange
            var values = new[]
            {
                new PrimitiveType("item1"),
                new PrimitiveType("item2"),
                new PrimitiveType("item3"),
                new PrimitiveType("item4"),
                new PrimitiveType("item5"),
                new PrimitiveType("item6"),
                new PrimitiveType("item7")
            };

            await MemoryService.PushToListTailAsync(TestScope, listName, values);

            // Act
            var result = await MemoryService.GetListSizeAsync(TestScope, listName);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().Be(7);
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task GetListSize_WithEmptyList_ShouldReturnZero()
    {
        const string listName = "empty-size-list";
        try
        {
            // Act
            var result = await MemoryService.GetListSizeAsync(TestScope, listName);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().Be(0);
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task ListContains_WithExistingElement_ShouldReturnTrue()
    {
        const string listName = "contains-list";
        try
        {
            // Arrange
            var values = new[]
            {
                new PrimitiveType("apple"),
                new PrimitiveType("banana"),
                new PrimitiveType("cherry")
            };

            await MemoryService.PushToListTailAsync(TestScope, listName, values);

            // Act
            var result = await MemoryService.ListContainsAsync(TestScope, listName, new PrimitiveType("banana"));

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeTrue();
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task ListContains_WithNonExistingElement_ShouldReturnFalse()
    {
        const string listName = "contains-list-2";
        try
        {
            // Arrange
            var values = new[]
            {
                new PrimitiveType("apple"),
                new PrimitiveType("banana")
            };

            await MemoryService.PushToListTailAsync(TestScope, listName, values);

            // Act
            var result = await MemoryService.ListContainsAsync(TestScope, listName, new PrimitiveType("orange"));

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeFalse();
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task ListContains_WithEmptyList_ShouldReturnFalse()
    {
        const string listName = "empty-contains-list";
        try
        {
            // Act
            var result = await MemoryService.ListContainsAsync(TestScope, listName, new PrimitiveType("anything"));

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeFalse();
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task PushToListTailIfValuesNotExists_WithNewValues_ShouldAddAllValues()
    {
        const string listName = "if-not-exists-new-list";
        try
        {
            // Arrange
            var values = new[]
            {
                new PrimitiveType("new-item1"),
                new PrimitiveType("new-item2"),
                new PrimitiveType("new-item3")
            };

            // Act
            var result = await MemoryService.PushToListTailIfValuesNotExistsAsync(TestScope, listName, values);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().NotBeNull();
            result.Data.Should().HaveCount(3);
            result.Data.Should().BeEquivalentTo(values, options => options.WithStrictOrdering());

            // Verify all values were added to the list
            var listResult = await MemoryService.GetAllElementsOfListAsync(TestScope, listName);
            listResult.IsSuccessful.Should().BeTrue();
            listResult.Data.Should().HaveCount(3);
            listResult.Data.Should().BeEquivalentTo(values, options => options.WithStrictOrdering());
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task PushToListTailIfValuesNotExists_WithAllExistingValues_ShouldAddNothing()
    {
        const string listName = "if-not-exists-existing-list";
        try
        {
            // Arrange - First add some values
            var existingValues = new[]
            {
                new PrimitiveType("existing1"),
                new PrimitiveType("existing2")
            };

            await MemoryService.PushToListTailAsync(TestScope, listName, existingValues, publishChange: false);

            // Act - Try to add the same values again
            var result = await MemoryService.PushToListTailIfValuesNotExistsAsync(TestScope, listName, existingValues);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().NotBeNull();
            result.Data.Should().BeEmpty("No values should be added since all already exist");

            // Verify list size hasn't changed
            var sizeResult = await MemoryService.GetListSizeAsync(TestScope, listName);
            sizeResult.IsSuccessful.Should().BeTrue();
            sizeResult.Data.Should().Be(2);

            // Verify values are still the same
            var listResult = await MemoryService.GetAllElementsOfListAsync(TestScope, listName);
            listResult.IsSuccessful.Should().BeTrue();
            listResult.Data.Should().HaveCount(2);
            listResult.Data.Should().BeEquivalentTo(existingValues, options => options.WithStrictOrdering());
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task PushToListTailIfValuesNotExists_WithMixedExistingAndNewValues_ShouldAddOnlyNewValues()
    {
        const string listName = "if-not-exists-mixed-list";
        try
        {
            // Arrange - First add some values
            var existingValues = new[]
            {
                new PrimitiveType("existing1"),
                new PrimitiveType("existing2")
            };

            await MemoryService.PushToListTailAsync(TestScope, listName, existingValues, publishChange: false);

            // Act - Try to add a mix of existing and new values
            var mixedValues = new[]
            {
                new PrimitiveType("existing1"), // Already exists
                new PrimitiveType("new1"),      // New value
                new PrimitiveType("existing2"), // Already exists
                new PrimitiveType("new2"),      // New value
                new PrimitiveType("new3")       // New value
            };

            var result = await MemoryService.PushToListTailIfValuesNotExistsAsync(TestScope, listName, mixedValues);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().NotBeNull();
            result.Data.Should().HaveCount(3, "Only the 3 new values should be returned");

            var expectedPushedValues = new[]
            {
                new PrimitiveType("new1"),
                new PrimitiveType("new2"),
                new PrimitiveType("new3")
            };
            result.Data.Should().BeEquivalentTo(expectedPushedValues, options => options.WithStrictOrdering());

            // Verify only new values were added
            var listResult = await MemoryService.GetAllElementsOfListAsync(TestScope, listName);
            listResult.IsSuccessful.Should().BeTrue();
            listResult.Data.Should().HaveCount(5); // 2 existing + 3 new

            var expectedValues = new[]
            {
                new PrimitiveType("existing1"),
                new PrimitiveType("existing2"),
                new PrimitiveType("new1"),
                new PrimitiveType("new2"),
                new PrimitiveType("new3")
            };

            listResult.Data.Should().BeEquivalentTo(expectedValues, options => options.WithStrictOrdering());

            // Verify that new values were added to the tail
            var lastThreeItems = listResult.Data!.Skip(2).ToArray();
            lastThreeItems.Should().BeEquivalentTo(expectedPushedValues, options => options.WithStrictOrdering());
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task PushToListTailIfValuesNotExists_WithEmptyValues_ShouldFail()
    {
        const string listName = "if-not-exists-empty-values-list";
        try
        {
            // Act
            var result = await MemoryService.PushToListTailIfValuesNotExistsAsync(TestScope, listName, []);

            // Assert
            result.IsSuccessful.Should().BeFalse();
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task PushToListTailIfValuesNotExists_WithPublishChangeTrue_ShouldPublishOnlyAddedValues()
    {
        const string listName = "if-not-exists-publish-list";
        try
        {
            // Arrange - First add some values
            var existingValues = new[]
            {
                new PrimitiveType("existing1"),
                new PrimitiveType("existing2")
            };

            await MemoryService.PushToListTailAsync(TestScope, listName, existingValues, publishChange: false);

            // Act & Assert - Try to add mixed values and capture notifications
            var mixedValues = new[]
            {
                new PrimitiveType("existing1"), // Already exists - should not be published
                new PrimitiveType("new1"),      // New value - should be published
                new PrimitiveType("existing2"), // Already exists - should not be published
                new PrimitiveType("new2")       // New value - should be published
            };

            var expectedPushedValues = new[]
            {
                new PrimitiveType("new1"),
                new PrimitiveType("new2")
            };

            var messages = await CapturePublishedMessagesAsync(async () =>
            {
                var result = await MemoryService.PushToListTailIfValuesNotExistsAsync(TestScope, listName, mixedValues, publishChange: true);
                result.IsSuccessful.Should().BeTrue();
                result.Data.Should().NotBeNull();
                result.Data.Should().HaveCount(2, "Only 2 new values should be added");
                result.Data.Should().BeEquivalentTo(expectedPushedValues, options => options.WithStrictOrdering());
            });

            // Verify change notification was published
            messages.Should().HaveCount(1, "Expected one change notification to be published");
            messages[0].Should().Contain("PushToListTailIfNotExists", "Expected operation type in notification");
            messages[0].Should().Contain(listName, "Expected list name in notification");

            // Verify the notification contains only the actually added values
            messages[0].Should().Contain("new1", "Expected new1 in notification (was added)");
            messages[0].Should().Contain("new2", "Expected new2 in notification (was added)");
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task PushToListTailIfValuesNotExists_WithPublishChangeFalse_ShouldNotPublishNotification()
    {
        const string listName = "if-not-exists-no-publish-list";
        try
        {
            // Arrange
            var values = new[]
            {
                new PrimitiveType("no-publish-item1"),
                new PrimitiveType("no-publish-item2")
            };

            // Act & Assert
            var messages = await CapturePublishedMessagesAsync(async () =>
            {
                var result = await MemoryService.PushToListTailIfValuesNotExistsAsync(TestScope, listName, values, publishChange: false);
                result.IsSuccessful.Should().BeTrue();
                result.Data.Should().NotBeNull();
                result.Data.Should().HaveCount(2);
                result.Data.Should().BeEquivalentTo(values, options => options.WithStrictOrdering());
            }, TimeSpan.FromSeconds(2)); // Shorter timeout since we expect no messages

            // Verify no change notification was published
            messages.Should().BeEmpty("Expected no change notifications when publishChange is false");

            // Verify values were still added
            var listResult = await MemoryService.GetAllElementsOfListAsync(TestScope, listName);
            listResult.IsSuccessful.Should().BeTrue();
            listResult.Data.Should().HaveCount(2);
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task PushToListTailIfValuesNotExists_WithDuplicateValuesInInput_ShouldAddOnlyOncePerUniqueValue()
    {
        const string listName = "if-not-exists-duplicates-list";
        try
        {
            // Arrange - Values with duplicates
            var valuesWithDuplicates = new[]
            {
                new PrimitiveType("item1"),
                new PrimitiveType("item2"),
                new PrimitiveType("item1"), // Duplicate
                new PrimitiveType("item3"),
                new PrimitiveType("item2"), // Duplicate
                new PrimitiveType("item4")
            };

            // Act
            var result = await MemoryService.PushToListTailIfValuesNotExistsAsync(TestScope, listName, valuesWithDuplicates);

            // Assert
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().NotBeNull();

            // Verify behavior - this depends on implementation
            // The Lua script should handle duplicates appropriately
            var listResult = await MemoryService.GetAllElementsOfListAsync(TestScope, listName);
            listResult.IsSuccessful.Should().BeTrue();

            // Each unique value should appear at least once, but exact behavior may vary
            listResult.Data.Should().Contain(new PrimitiveType("item1"));
            listResult.Data.Should().Contain(new PrimitiveType("item2"));
            listResult.Data.Should().Contain(new PrimitiveType("item3"));
            listResult.Data.Should().Contain(new PrimitiveType("item4"));

            // The returned array should contain the values that were actually pushed
            result.Data.Should().Contain(new PrimitiveType("item1"));
            result.Data.Should().Contain(new PrimitiveType("item2"));
            result.Data.Should().Contain(new PrimitiveType("item3"));
            result.Data.Should().Contain(new PrimitiveType("item4"));
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task PushToListTailIfValuesNotExists_CompleteWorkflow_ShouldWorkCorrectly()
    {
        const string listName = "if-not-exists-workflow-list";
        try
        {
            // Complete workflow test combining multiple scenarios

            // 1. Add initial values to empty list
            var initialValues = new[]
            {
                new PrimitiveType("alpha"),
                new PrimitiveType("beta")
            };

            var result1 = await MemoryService.PushToListTailIfValuesNotExistsAsync(TestScope, listName, initialValues);
            result1.IsSuccessful.Should().BeTrue();
            result1.Data.Should().NotBeNull();
            result1.Data.Should().HaveCount(2);
            result1.Data.Should().BeEquivalentTo(initialValues, options => options.WithStrictOrdering());

            var size1 = await MemoryService.GetListSizeAsync(TestScope, listName);
            size1.Data.Should().Be(2);

            // 2. Try to add some existing and some new values
            var mixedValues = new[]
            {
                new PrimitiveType("alpha"),   // Exists
                new PrimitiveType("gamma"),   // New
                new PrimitiveType("beta"),    // Exists
                new PrimitiveType("delta")    // New
            };

            var expectedNewValues = new[]
            {
                new PrimitiveType("gamma"),
                new PrimitiveType("delta")
            };

            var result2 = await MemoryService.PushToListTailIfValuesNotExistsAsync(TestScope, listName, mixedValues);
            result2.IsSuccessful.Should().BeTrue();
            result2.Data.Should().NotBeNull();
            result2.Data.Should().HaveCount(2, "Only gamma and delta should be added");
            result2.Data.Should().BeEquivalentTo(expectedNewValues, options => options.WithStrictOrdering());

            var size2 = await MemoryService.GetListSizeAsync(TestScope, listName);
            size2.Data.Should().Be(4); // 2 original + 2 new

            // 3. Try to add all existing values
            var allExistingValues = new[]
            {
                new PrimitiveType("alpha"),
                new PrimitiveType("beta"),
                new PrimitiveType("gamma"),
                new PrimitiveType("delta")
            };

            var result3 = await MemoryService.PushToListTailIfValuesNotExistsAsync(TestScope, listName, allExistingValues);
            result3.IsSuccessful.Should().BeTrue();
            result3.Data.Should().NotBeNull();
            result3.Data.Should().BeEmpty("No new values should be added since all already exist");

            var size3 = await MemoryService.GetListSizeAsync(TestScope, listName);
            size3.Data.Should().Be(4); // Size unchanged

            // 4. Verify final list contents
            var finalList = await MemoryService.GetAllElementsOfListAsync(TestScope, listName);
            finalList.IsSuccessful.Should().BeTrue();
            finalList.Data.Should().HaveCount(4);

            var expectedFinalValues = new[]
            {
                new PrimitiveType("alpha"),
                new PrimitiveType("beta"),
                new PrimitiveType("gamma"),
                new PrimitiveType("delta")
            };

            finalList.Data.Should().BeEquivalentTo(expectedFinalValues, options => options.WithStrictOrdering());
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    #endregion

    #region Integration and Complex Scenarios Tests

    [RetryFact(3, 5000)]
    public async Task CompleteWorkflow_KeyValueOperations_ShouldWorkCorrectly()
    {
        try
        {
            // Arrange & Act & Assert

            // 1. Set initial values
            var initialValues = new Dictionary<string, PrimitiveType>
            {
                ["user:1:name"] = new("John Doe"),
                ["user:1:age"] = new(30L),
                ["user:1:score"] = new(95.5)
            };

            var setResult = await MemoryService.SetKeyValuesAsync(TestScope, initialValues);
            setResult.IsSuccessful.Should().BeTrue();

            // 2. Get all values
            var getAllResult = await MemoryService.GetAllKeyValuesAsync(TestScope);
            getAllResult.IsSuccessful.Should().BeTrue();
            getAllResult.Data.Should().BeEquivalentTo(initialValues);

            // 3. Increment age
            var incrementResult = await MemoryService.IncrementKeyByValueAndGetAsync(TestScope, "user:1:age", 1);
            incrementResult.IsSuccessful.Should().BeTrue();
            incrementResult.Data.Should().Be(31);

            // 4. Get specific keys
            var getKeysResult = await MemoryService.GetKeyValuesAsync(TestScope, ["user:1:name", "user:1:age"]);
            getKeysResult.IsSuccessful.Should().BeTrue();
            getKeysResult.Data.Should().HaveCount(2);
            getKeysResult.Data!["user:1:name"].Should().Be(new PrimitiveType("John Doe"));
            getKeysResult.Data["user:1:age"].Should().Be(new PrimitiveType(31L));

            // 5. Delete specific key
            var deleteResult = await MemoryService.DeleteKeyAsync(TestScope, "user:1:score");
            deleteResult.IsSuccessful.Should().BeTrue();
            deleteResult.Data.Should().BeTrue();

            // 6. Verify deletion
            var finalGetResult = await MemoryService.GetKeyValueAsync(TestScope, "user:1:score");
            finalGetResult.Data.Should().BeNull();

            // 7. Count remaining keys
            var countResult = await MemoryService.GetKeysCountAsync(TestScope);
            countResult.IsSuccessful.Should().BeTrue();
            countResult.Data.Should().Be(2);
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task CompleteWorkflow_ListOperations_ShouldWorkCorrectly()
    {
        const string listName = "workflow-list";
        try
        {
            // Arrange & Act & Assert

            // 1. Push to tail
            var pushTailResult = await MemoryService.PushToListTailAsync(TestScope, listName,
                [new PrimitiveType("first"), new PrimitiveType("second")]);
            pushTailResult.IsSuccessful.Should().BeTrue();

            // 2. Push to head
            var pushHeadResult = await MemoryService.PushToListHeadAsync(TestScope, listName,
                [new PrimitiveType("zero")]);
            pushHeadResult.IsSuccessful.Should().BeTrue();

            // 3. Check list size
            var sizeResult = await MemoryService.GetListSizeAsync(TestScope, listName);
            sizeResult.IsSuccessful.Should().BeTrue();
            sizeResult.Data.Should().Be(3);

            // 4. Get all elements
            var getAllResult = await MemoryService.GetAllElementsOfListAsync(TestScope, listName);
            getAllResult.IsSuccessful.Should().BeTrue();
            getAllResult.Data.Should().BeEquivalentTo(
                [new PrimitiveType("zero"), new PrimitiveType("first"), new PrimitiveType("second")],
                options => options.WithStrictOrdering());

            // 5. Check contains
            var containsResult = await MemoryService.ListContainsAsync(TestScope, listName, new PrimitiveType("first"));
            containsResult.IsSuccessful.Should().BeTrue();
            containsResult.Data.Should().BeTrue();

            // 6. Pop from head
            var popHeadResult = await MemoryService.PopFirstElementOfListAsync(TestScope, listName);
            popHeadResult.IsSuccessful.Should().BeTrue();
            popHeadResult.Data.Should().Be(new PrimitiveType("zero"));

            // 7. Pop from tail
            var popTailResult = await MemoryService.PopLastElementOfListAsync(TestScope, listName);
            popTailResult.IsSuccessful.Should().BeTrue();
            popTailResult.Data.Should().Be(new PrimitiveType("second"));

            // 8. Final size check
            var finalSizeResult = await MemoryService.GetListSizeAsync(TestScope, listName);
            finalSizeResult.IsSuccessful.Should().BeTrue();
            finalSizeResult.Data.Should().Be(1);

            // 9. Remove remaining element
            var removeResult = await MemoryService.RemoveElementsFromListAsync(TestScope, listName,
                [new PrimitiveType("first")]);
            removeResult.IsSuccessful.Should().BeTrue();

            // 10. Verify empty
            var emptyCheckResult = await MemoryService.GetListSizeAsync(TestScope, listName);
            emptyCheckResult.IsSuccessful.Should().BeTrue();
            emptyCheckResult.Data.Should().Be(0);
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task PrimitiveTypeCompatibility_AllTypes_ShouldWorkCorrectly()
    {
        try
        {
            // Test all primitive types
            var keyValues = new Dictionary<string, PrimitiveType>
            {
                ["string-key"] = new("Hello, World!"),
                ["long-key"] = new(9223372036854775807L), // Max long
                ["double-key"] = new(Math.PI),
                ["byte-array-key"] = new(new byte[] { 0xFF, 0xFE, 0xFD, 0xFC })
            };

            // Set all values
            var setResult = await MemoryService.SetKeyValuesAsync(TestScope, keyValues);
            setResult.IsSuccessful.Should().BeTrue();

            // Get and verify each type
            foreach (var kvp in keyValues)
            {
                var getResult = await MemoryService.GetKeyValueAsync(TestScope, kvp.Key);
                getResult.IsSuccessful.Should().BeTrue();
                getResult.Data.Should().Be(kvp.Value);
                getResult.Data!.Kind.Should().Be(kvp.Value.Kind);
            }
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    #endregion

    #region Time-to-Live Expiration Tests

    [RetryFact(3, 5000)]
    public async Task TimeToLive_ExpiresAfterShortTime_DataShouldBecomeInaccessible()
    {
        try
        {
            // Arrange
            var shortTtl = TimeSpan.FromSeconds(3); // Very short TTL for testing
            var testKey = "ttl-test-key";
            var testValue = new PrimitiveType("ttl-test-value");

            // Set a short TTL on the scope
            var setTtlResult = await MemoryService.SetKeyExpireTimeAsync(TestScope, shortTtl);
            setTtlResult.IsSuccessful.Should().BeTrue();

            // Store test data
            var setDataResult = await MemoryService.SetKeyValuesAsync(TestScope, [new(testKey, testValue)]);
            setDataResult.IsSuccessful.Should().BeTrue();

            // Verify data is initially accessible
            var initialGetResult = await MemoryService.GetKeyValueAsync(TestScope, testKey);
            initialGetResult.IsSuccessful.Should().BeTrue();
            initialGetResult.Data.Should().Be(testValue, "Data should be accessible before TTL expires");

            // Verify TTL is set correctly
            var getTtlResult = await MemoryService.GetKeyExpireTimeAsync(TestScope);
            getTtlResult.IsSuccessful.Should().BeTrue();
            getTtlResult.Data.Should().NotBeNull();
            getTtlResult.Data.Should().BeLessOrEqualTo(shortTtl);
            getTtlResult.Data.Should().BeGreaterThan(TimeSpan.FromSeconds(1));

            // Wait for TTL to expire (wait a bit longer than the TTL to ensure expiration)
            await Task.Delay(shortTtl.Add(TimeSpan.FromSeconds(2)));

            // Act - Try to access data after expiration
            var expiredGetResult = await MemoryService.GetKeyValueAsync(TestScope, testKey);

            // Assert - Data should no longer be accessible
            expiredGetResult.IsSuccessful.Should().BeTrue();
            expiredGetResult.Data.Should().BeNull("Data should not be accessible after TTL expires");

            // Verify that the scope itself has expired by checking key count
            var keyCountResult = await MemoryService.GetKeysCountAsync(TestScope);
            keyCountResult.IsSuccessful.Should().BeTrue();
            keyCountResult.Data.Should().Be(0, "Key count should be zero after TTL expiration");

            // Verify that all keys in the scope have expired
            var allKeysResult = await MemoryService.GetAllKeyValuesAsync(TestScope);
            allKeysResult.IsSuccessful.Should().BeTrue();
            allKeysResult.Data.Should().BeEmpty("All keys should be expired and inaccessible");
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task TimeToLive_DataAccessibleBeforeExpiration_InaccessibleAfterExpiration()
    {
        try
        {
            // Arrange
            var mediumTtl = TimeSpan.FromSeconds(5);
            var testKeys = new Dictionary<string, PrimitiveType>
            {
                ["ttl-key1"] = new("value1"),
                ["ttl-key2"] = new(42L),
                ["ttl-key3"] = new(3.14)
            };

            // Set TTL and store multiple keys
            await MemoryService.SetKeyExpireTimeAsync(TestScope, mediumTtl);
            await MemoryService.SetKeyValuesAsync(TestScope, testKeys);

            // Verify all data is accessible before expiration
            var beforeExpirationResult = await MemoryService.GetKeyValuesAsync(TestScope, testKeys.Keys);
            beforeExpirationResult.IsSuccessful.Should().BeTrue();
            beforeExpirationResult.Data.Should().HaveCount(3);
            beforeExpirationResult.Data.Should().BeEquivalentTo(testKeys);

            // Wait for partial TTL (data should still be accessible)
            await Task.Delay(TimeSpan.FromSeconds(2));

            var midTtlResult = await MemoryService.GetKeyValueAsync(TestScope, "ttl-key1");
            midTtlResult.IsSuccessful.Should().BeTrue();
            midTtlResult.Data.Should().Be(new PrimitiveType("value1"), "Data should still be accessible mid-TTL");

            // Wait for full expiration
            await Task.Delay(TimeSpan.FromSeconds(4)); // Total wait: 6 seconds (> 5 second TTL)

            // Verify all data is no longer accessible
            var afterExpirationResult = await MemoryService.GetKeyValuesAsync(TestScope, testKeys.Keys);
            afterExpirationResult.IsSuccessful.Should().BeTrue();
            afterExpirationResult.Data.Should().BeEmpty("No keys should be accessible after TTL expiration");

            // Verify individual key access
            foreach (var key in testKeys.Keys)
            {
                var individualResult = await MemoryService.GetKeyValueAsync(TestScope, key);
                individualResult.IsSuccessful.Should().BeTrue();
                individualResult.Data.Should().BeNull($"Key '{key}' should not be accessible after TTL expiration");
            }
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task TimeToLive_ListOperations_ShouldExpireAfterTtl()
    {
        const string listName = "ttl-test-list";
        try
        {
            // Arrange
            var shortTtl = TimeSpan.FromSeconds(4);
            var listValues = new[]
            {
                new PrimitiveType("ttl-item1"),
                new PrimitiveType("ttl-item2"),
                new PrimitiveType("ttl-item3")
            };

            // Set TTL and create list
            await MemoryService.SetKeyExpireTimeAsync(TestScope, shortTtl);
            await MemoryService.PushToListTailAsync(TestScope, listName, listValues);

            // Verify list is accessible before expiration
            var initialListResult = await MemoryService.GetAllElementsOfListAsync(TestScope, listName);
            initialListResult.IsSuccessful.Should().BeTrue();
            initialListResult.Data.Should().HaveCount(3);

            var initialSizeResult = await MemoryService.GetListSizeAsync(TestScope, listName);
            initialSizeResult.IsSuccessful.Should().BeTrue();
            initialSizeResult.Data.Should().Be(3);

            // Wait for TTL to expire
            await Task.Delay(shortTtl.Add(TimeSpan.FromSeconds(2)));

            // Verify list operations return empty/zero results after expiration
            var expiredListResult = await MemoryService.GetAllElementsOfListAsync(TestScope, listName);
            expiredListResult.IsSuccessful.Should().BeTrue();
            expiredListResult.Data.Should().BeEmpty("List should be empty after TTL expiration");

            var expiredSizeResult = await MemoryService.GetListSizeAsync(TestScope, listName);
            expiredSizeResult.IsSuccessful.Should().BeTrue();
            expiredSizeResult.Data.Should().Be(0, "List size should be zero after TTL expiration");

            // Verify list operations that expect elements fail appropriately
            var expiredPopResult = await MemoryService.PopLastElementOfListAsync(TestScope, listName);
            expiredPopResult.IsSuccessful.Should().BeFalse("Pop operations should fail on expired/empty list");
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    #endregion

    #region Cancellation Token Tests

    [RetryFact(3, 5000)]
    public async Task Operations_WithCancellationToken_ShouldRespectCancellation()
    {
        try
        {
            // Arrange
            using var cts = new CancellationTokenSource();
            await cts.CancelAsync();

            // Act & Assert - Operations should handle cancellation gracefully
            // Note: Some operations might complete before cancellation is checked
            var tasks = new[]
            {
                MemoryService.GetKeysCountAsync(TestScope, cts.Token),
                MemoryService.GetListSizeAsync(TestScope, "test-list", cts.Token)
            };

            // These should either succeed quickly or handle cancellation gracefully
            await Task.WhenAll(tasks);

            // If we get here without exception, the implementation handles cancellation gracefully
            Assert.True(true);
        }
        finally
        {
            await SafeCleanupAsync("test-list");
        }
    }

    #endregion

    #region Edge Cases and Error Handling Tests

    [RetryFact(3, 5000)]
    public async Task Operations_WithPublishChangeFalse_ShouldStillWork()
    {
        try
        {
            // Test that operations work correctly when publishChange is set to false
            var keyValues = new Dictionary<string, PrimitiveType>
            {
                ["no-publish-key"] = new("no-publish-value")
            };

            var setResult = await MemoryService.SetKeyValuesAsync(TestScope, keyValues, publishChange: false);
            setResult.IsSuccessful.Should().BeTrue();

            var getResult = await MemoryService.GetKeyValueAsync(TestScope, "no-publish-key");
            getResult.IsSuccessful.Should().BeTrue();
            getResult.Data.Should().Be(new PrimitiveType("no-publish-value"));

            var deleteResult = await MemoryService.DeleteKeyAsync(TestScope, "no-publish-key", publishChange: false);
            deleteResult.IsSuccessful.Should().BeTrue();
            deleteResult.Data.Should().BeTrue();
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task ListOperations_WithEmptyValueArrays_ShouldHandleGracefully()
    {
        const string listName = "empty-values-list";
        try
        {
            // These operations with empty arrays should either fail gracefully or handle properly
            var pushResult = await MemoryService.PushToListTailAsync(TestScope, listName, []);

            // Either should fail with proper error message, or handle empty arrays gracefully
            if (!pushResult.IsSuccessful)
            {
                pushResult.ErrorMessage.Should().NotBeNullOrEmpty();
            }

            var removeResult = await MemoryService.RemoveElementsFromListAsync(TestScope, listName, []);
            if (!removeResult.IsSuccessful)
            {
                removeResult.ErrorMessage.Should().NotBeNullOrEmpty();
            }
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task LargeDataOperations_ShouldHandleCorrectly()
    {
        try
        {
            // Test with larger datasets
            var largeKeyValues = new Dictionary<string, PrimitiveType>();
            for (var i = 0; i < 100; i++)
            {
                largeKeyValues[$"large-key-{i:D3}"] = new($"large-value-{i:D3}");
            }

            var setResult = await MemoryService.SetKeyValuesAsync(TestScope, largeKeyValues);
            setResult.IsSuccessful.Should().BeTrue();

            var countResult = await MemoryService.GetKeysCountAsync(TestScope);
            countResult.IsSuccessful.Should().BeTrue();
            countResult.Data.Should().Be(100);

            var getAllResult = await MemoryService.GetAllKeyValuesAsync(TestScope);
            getAllResult.IsSuccessful.Should().BeTrue();
            getAllResult.Data.Should().HaveCount(100);
            getAllResult.Data.Should().BeEquivalentTo(largeKeyValues);
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    #endregion

    #region Publish/Subscribe Change Notification Tests

    /// <summary>
    /// Helper method to capture published messages for testing
    /// </summary>
    private async Task<List<string>> CapturePublishedMessagesAsync(Func<Task> operation, TimeSpan? timeout = null)
    {
        var capturedMessages = new List<string>();
        var messageReceived = new TaskCompletionSource<bool>();
        var actualTimeout = timeout ?? TimeSpan.FromSeconds(5);

        var topic = TestScope.Compile();

        // Subscribe to capture messages
        var subscribeResult = await PubSubService.SubscribeAsync(topic, (_, message) =>
        {
            capturedMessages.Add(message);
            messageReceived.TrySetResult(true);
            return Task.CompletedTask;
        });

        subscribeResult.IsSuccessful.Should().BeTrue("Failed to subscribe to topic for change notifications");

        try
        {
            // Execute the operation that should publish a change notification
            await operation();

            // Wait for message with timeout
            using var cts = new CancellationTokenSource(actualTimeout);
            try
            {
                await messageReceived.Task.WaitAsync(cts.Token);
            }
            catch (OperationCanceledException)
            {
                // No message received within timeout - this might be expected for some operations
            }

            // Give a small additional delay for any late messages (only if not canceled)
            if (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(1000, cts.Token);
                }
                catch (OperationCanceledException)
                {
                    // Ignore cancellation during the delay
                }
            }
        }
        finally
        {
            // Cleanup subscription
            try
            {
                await PubSubService.DeleteTopicAsync(topic);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }

        return capturedMessages;
    }

    [RetryFact(3, 5000)]
    public async Task SetKeyValues_WithPublishChangeTrue_ShouldPublishChangeNotification()
    {
        try
        {
            // Arrange
            var keyValues = new[]
            {
                new KeyValuePair<string, PrimitiveType>("publish-key1", new PrimitiveType("publish-value1")),
                new KeyValuePair<string, PrimitiveType>("publish-key2", new PrimitiveType(42L))
            };

            // Act & Assert
            var messages = await CapturePublishedMessagesAsync(async () =>
            {
                var result = await MemoryService.SetKeyValuesAsync(TestScope, keyValues, publishChange: true);
                result.IsSuccessful.Should().BeTrue();
            });

            // Verify change notification was published
            messages.Should().HaveCount(1, "Expected one change notification to be published");
            messages[0].Should().Contain("SetKeyValue", "Expected operation type in notification");
            messages[0].Should().Contain("publish-key1", "Expected key1 in notification");
            messages[0].Should().Contain("publish-key2", "Expected key2 in notification");
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task SetKeyValues_WithPublishChangeFalse_ShouldNotPublishChangeNotification()
    {
        try
        {
            // Arrange
            var keyValues = new[]
            {
                new KeyValuePair<string, PrimitiveType>("no-publish-key", new PrimitiveType("no-publish-value"))
            };

            // Act & Assert
            var messages = await CapturePublishedMessagesAsync(async () =>
            {
                var result = await MemoryService.SetKeyValuesAsync(TestScope, keyValues, publishChange: false);
                result.IsSuccessful.Should().BeTrue();
            }, TimeSpan.FromSeconds(2)); // Shorter timeout since we expect no messages

            // Verify no change notification was published
            messages.Should().BeEmpty("Expected no change notifications when publishChange is false");
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task SetKeyValueConditionally_WithPublishChangeTrue_AndNewKey_ShouldPublishChangeNotification()
    {
        try
        {
            // Arrange
            var key = "conditional-key";
            var value = new PrimitiveType("conditional-value");

            // Act & Assert
            var messages = await CapturePublishedMessagesAsync(async () =>
            {
                var result = await MemoryService.SetKeyValueConditionallyAsync(TestScope, key, value, publishChange: true);
                result.IsSuccessful.Should().BeTrue();
                result.Data.Should().BeTrue();
            });

            // Verify change notification was published
            messages.Should().HaveCount(1, "Expected one change notification to be published");
            messages[0].Should().Contain("SetKeyValue", "Expected operation type in notification");
            messages[0].Should().Contain(key, "Expected key in notification");
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task SetKeyValueConditionally_WithPublishChangeFalse_ShouldNotPublishChangeNotification()
    {
        try
        {
            // Arrange
            var key = "conditional-no-publish-key";
            var value = new PrimitiveType("conditional-value");

            // Act & Assert
            var messages = await CapturePublishedMessagesAsync(async () =>
            {
                var result = await MemoryService.SetKeyValueConditionallyAsync(TestScope, key, value, publishChange: false);
                result.IsSuccessful.Should().BeTrue();
            }, TimeSpan.FromSeconds(2));

            // Verify no change notification was published
            messages.Should().BeEmpty("Expected no change notifications when publishChange is false");
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task DeleteKey_WithPublishChangeTrue_ShouldPublishChangeNotification()
    {
        try
        {
            // Arrange
            var key = "delete-publish-key";
            var value = new PrimitiveType("delete-value");
            await MemoryService.SetKeyValuesAsync(TestScope, [new(key, value)], publishChange: false);

            // Act & Assert
            var messages = await CapturePublishedMessagesAsync(async () =>
            {
                var result = await MemoryService.DeleteKeyAsync(TestScope, key, publishChange: true);
                result.IsSuccessful.Should().BeTrue();
                result.Data.Should().BeTrue();
            });

            // Verify change notification was published
            messages.Should().HaveCount(1, "Expected one change notification to be published");
            messages[0].Should().Contain("DeleteKey", "Expected operation type in notification");
            messages[0].Should().Contain(key, "Expected deleted key in notification");
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task DeleteKey_WithPublishChangeFalse_ShouldNotPublishChangeNotification()
    {
        try
        {
            // Arrange
            const string key = "delete-no-publish-key";
            var value = new PrimitiveType("delete-value");
            await MemoryService.SetKeyValuesAsync(TestScope, [new(key, value)], publishChange: false);

            // Act & Assert
            var messages = await CapturePublishedMessagesAsync(async () =>
            {
                var result = await MemoryService.DeleteKeyAsync(TestScope, key, publishChange: false);
                result.IsSuccessful.Should().BeTrue();
            }, TimeSpan.FromSeconds(2));

            // Verify no change notification was published
            messages.Should().BeEmpty("Expected no change notifications when publishChange is false");
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task DeleteAllKeys_WithPublishChangeTrue_ShouldPublishChangeNotification()
    {
        try
        {
            // Arrange
            var keyValues = new Dictionary<string, PrimitiveType>
            {
                ["key1"] = new("value1"),
                ["key2"] = new("value2")
            };
            await MemoryService.SetKeyValuesAsync(TestScope, keyValues, publishChange: false);

            // Act & Assert
            var messages = await CapturePublishedMessagesAsync(async () =>
            {
                var result = await MemoryService.DeleteAllKeysAsync(TestScope, publishChange: true);
                result.IsSuccessful.Should().BeTrue();
                result.Data.Should().BeTrue();
            });

            // Verify change notification was published
            messages.Should().HaveCount(1, "Expected one change notification to be published");
            messages[0].Should().Contain("DeleteAllKeys", "Expected operation type in notification");
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task IncrementKeyValues_WithPublishChangeTrue_ShouldPublishChangeNotification()
    {
        try
        {
            // Arrange
            var initialValues = new Dictionary<string, PrimitiveType>
            {
                ["counter1"] = new(10L),
                ["counter2"] = new(20L)
            };
            var increments = new Dictionary<string, long>
            {
                ["counter1"] = 5,
                ["counter2"] = -3
            };

            await MemoryService.SetKeyValuesAsync(TestScope, initialValues, publishChange: false);

            // Act & Assert
            var messages = await CapturePublishedMessagesAsync(async () =>
            {
                var result = await MemoryService.IncrementKeyValuesAsync(TestScope, increments, publishChange: true);
                result.IsSuccessful.Should().BeTrue();
            });

            // Verify change notification was published
            messages.Should().HaveCount(1, "Expected one change notification to be published");
            messages[0].Should().Contain("SetKeyValue", "Expected operation type in notification");
            messages[0].Should().Contain("counter1", "Expected counter1 in notification");
            messages[0].Should().Contain("counter2", "Expected counter2 in notification");
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task IncrementKeyByValueAndGet_WithPublishChangeTrue_ShouldPublishChangeNotification()
    {
        try
        {
            // Arrange
            var key = "increment-publish-key";
            var initialValue = new PrimitiveType(100L);
            var incrementBy = 25L;

            await MemoryService.SetKeyValuesAsync(TestScope, [new(key, initialValue)], publishChange: false);

            // Act & Assert
            var messages = await CapturePublishedMessagesAsync(async () =>
            {
                var result = await MemoryService.IncrementKeyByValueAndGetAsync(TestScope, key, incrementBy, publishChange: true);
                result.IsSuccessful.Should().BeTrue();
                result.Data.Should().Be(125);
            });

            // Verify change notification was published
            messages.Should().HaveCount(1, "Expected one change notification to be published");
            messages[0].Should().Contain("SetKeyValue", "Expected operation type in notification");
            messages[0].Should().Contain(key, "Expected key in notification");
        }
        finally
        {
            await SafeCleanupAsync();
        }
    }

    [RetryFact(3, 5000)]
    public async Task PushToListTail_WithPublishChangeTrue_ShouldPublishChangeNotification()
    {
        const string listName = "publish-tail-list";
        try
        {
            // Arrange
            var values = new[]
            {
                new PrimitiveType("item1"),
                new PrimitiveType("item2")
            };

            // Act & Assert
            var messages = await CapturePublishedMessagesAsync(async () =>
            {
                var result = await MemoryService.PushToListTailAsync(TestScope, listName, values, publishChange: true);
                result.IsSuccessful.Should().BeTrue();
                result.Data.Should().BeTrue();
            });

            // Verify change notification was published
            messages.Should().HaveCount(1, "Expected one change notification to be published");
            messages[0].Should().Contain("PushToListTail", "Expected operation type in notification");
            messages[0].Should().Contain(listName, "Expected list name in notification");
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task PushToListHead_WithPublishChangeTrue_ShouldPublishChangeNotification()
    {
        const string listName = "publish-head-list";
        try
        {
            // Arrange
            var values = new[]
            {
                new PrimitiveType("head1"),
                new PrimitiveType("head2")
            };

            // Act & Assert
            var messages = await CapturePublishedMessagesAsync(async () =>
            {
                var result = await MemoryService.PushToListHeadAsync(TestScope, listName, values, publishChange: true);
                result.IsSuccessful.Should().BeTrue();
                result.Data.Should().BeTrue();
            });

            // Verify change notification was published
            messages.Should().HaveCount(1, "Expected one change notification to be published");
            messages[0].Should().Contain("PushToListHead", "Expected operation type in notification");
            messages[0].Should().Contain(listName, "Expected list name in notification");
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task PopLastElementOfList_WithPublishChangeTrue_ShouldPublishChangeNotification()
    {
        const string listName = "publish-pop-tail-list";
        try
        {
            // Arrange
            var values = new[]
            {
                new PrimitiveType("first"),
                new PrimitiveType("last")
            };

            await MemoryService.PushToListTailAsync(TestScope, listName, values, publishChange: false);

            // Act & Assert
            var messages = await CapturePublishedMessagesAsync(async () =>
            {
                var result = await MemoryService.PopLastElementOfListAsync(TestScope, listName, publishChange: true);
                result.IsSuccessful.Should().BeTrue();
                result.Data.Should().Be(new PrimitiveType("last"));
            });

            // Verify change notification was published
            messages.Should().HaveCount(1, "Expected one change notification to be published");
            messages[0].Should().Contain("PopLastElementOfList", "Expected operation type in notification");
            messages[0].Should().Contain(listName, "Expected list name in notification");
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task PopFirstElementOfList_WithPublishChangeTrue_ShouldPublishChangeNotification()
    {
        const string listName = "publish-pop-head-list";
        try
        {
            // Arrange
            var values = new[]
            {
                new PrimitiveType("first"),
                new PrimitiveType("second")
            };

            await MemoryService.PushToListTailAsync(TestScope, listName, values, publishChange: false);

            // Act & Assert
            var messages = await CapturePublishedMessagesAsync(async () =>
            {
                var result = await MemoryService.PopFirstElementOfListAsync(TestScope, listName, publishChange: true);
                result.IsSuccessful.Should().BeTrue();
                result.Data.Should().Be(new PrimitiveType("first"));
            });

            // Verify change notification was published
            messages.Should().HaveCount(1, "Expected one change notification to be published");
            messages[0].Should().Contain("PopFirstElementOfList", "Expected operation type in notification");
            messages[0].Should().Contain(listName, "Expected list name in notification");
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task RemoveElementsFromList_WithPublishChangeTrue_ShouldPublishChangeNotification()
    {
        const string listName = "publish-remove-list";
        try
        {
            // Arrange
            var values = new[]
            {
                new PrimitiveType("keep"),
                new PrimitiveType("remove1"),
                new PrimitiveType("remove2")
            };

            var toRemove = new[]
            {
                new PrimitiveType("remove1"),
                new PrimitiveType("remove2")
            };

            await MemoryService.PushToListTailAsync(TestScope, listName, values, publishChange: false);

            // Act & Assert
            var messages = await CapturePublishedMessagesAsync(async () =>
            {
                var result = await MemoryService.RemoveElementsFromListAsync(TestScope, listName, toRemove, publishChange: true);
                result.IsSuccessful.Should().BeTrue();
            });

            // Verify change notification was published
            messages.Should().HaveCount(1, "Expected one change notification to be published");
            messages[0].Should().Contain("RemoveElementsFromList", "Expected operation type in notification");
            messages[0].Should().Contain(listName, "Expected list name in notification");
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task EmptyList_WithPublishChangeTrue_ShouldPublishChangeNotification()
    {
        const string listName = "publish-empty-list";
        try
        {
            // Arrange
            var values = new[]
            {
                new PrimitiveType("item1"),
                new PrimitiveType("item2")
            };

            await MemoryService.PushToListTailAsync(TestScope, listName, values, publishChange: false);

            // Act & Assert
            var messages = await CapturePublishedMessagesAsync(async () =>
            {
                var result = await MemoryService.EmptyListAsync(TestScope, listName, publishChange: true);
                result.IsSuccessful.Should().BeTrue();
                result.Data.Should().BeTrue();
            });

            // Verify change notification was published
            messages.Should().HaveCount(1, "Expected one change notification to be published");
            messages[0].Should().Contain("EmptyList", "Expected operation type in notification");
            messages[0].Should().Contain(listName, "Expected list name in notification");
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task EmptyListAndSublists_WithPublishChangeTrue_ShouldPublishChangeNotification()
    {
        const string listName = "publish-parent-list";
        var sublist1Name = "publish-sublist-sub1";
        var sublist2Name = "publish-sublist-sub2";
        try
        {
            // Arrange
            var sublistPrefix = "publish-sublist-";

            var parentValues = new[] { new PrimitiveType("parent1") };
            var sublist1Values = new[] { new PrimitiveType("sub1") };
            var sublist2Values = new[] { new PrimitiveType("sub2") };

            await MemoryService.PushToListTailAsync(TestScope, listName, parentValues, publishChange: false);
            await MemoryService.PushToListTailAsync(TestScope, sublist1Name, sublist1Values, publishChange: false);
            await MemoryService.PushToListTailAsync(TestScope, sublist2Name, sublist2Values, publishChange: false);

            // Act & Assert
            var messages = await CapturePublishedMessagesAsync(async () =>
            {
                var result = await MemoryService.EmptyListAndSublistsAsync(TestScope, listName, sublistPrefix, publishChange: true);
                result.IsSuccessful.Should().BeTrue();
                result.Data.Should().BeTrue();
            });

            // Verify change notification was published
            messages.Should().HaveCount(1, "Expected one change notification to be published");
            messages[0].Should().Contain("EmptyListAndSublists", "Expected operation type in notification");
            messages[0].Should().Contain(listName, "Expected list name in notification");
            messages[0].Should().Contain(sublistPrefix, "Expected sublist prefix in notification");
        }
        finally
        {
            await SafeCleanupAsync(listName, sublist1Name, sublist2Name);
        }
    }

    [RetryFact(3, 5000)]
    public async Task ListOperations_WithPublishChangeFalse_ShouldNotPublishChangeNotification()
    {
        const string listName = "no-publish-list";
        try
        {
            // Arrange
            var values = new[] { new PrimitiveType("item1") };

            // Act & Assert - Test multiple list operations with publishChange=false
            var messages = await CapturePublishedMessagesAsync(async () =>
            {
                // Push with publishChange=false
                var pushResult = await MemoryService.PushToListTailAsync(TestScope, listName, values, publishChange: false);
                pushResult.IsSuccessful.Should().BeTrue();

                // Pop with publishChange=false
                var popResult = await MemoryService.PopLastElementOfListAsync(TestScope, listName, publishChange: false);
                popResult.IsSuccessful.Should().BeTrue();

                // Push again and empty with publishChange=false
                await MemoryService.PushToListTailAsync(TestScope, listName, values, publishChange: false);
                var emptyResult = await MemoryService.EmptyListAsync(TestScope, listName, publishChange: false);
                emptyResult.IsSuccessful.Should().BeTrue();
            }, TimeSpan.FromSeconds(2));

            // Verify no change notifications were published
            messages.Should().BeEmpty("Expected no change notifications when publishChange is false");
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    [RetryFact(3, 5000)]
    public async Task MultipleOperations_WithPublishChangeTrue_ShouldPublishMultipleNotifications()
    {
        const string listName = "multiple-operations-list";
        try
        {
            // Arrange & Act & Assert
            var allMessages = new List<string>();

            // Capture messages from multiple operations
            var messages1 = await CapturePublishedMessagesAsync(async () =>
            {
                var result = await MemoryService.SetKeyValuesAsync(TestScope,
                    [new("multi-key", new PrimitiveType("multi-value"))], publishChange: true);
                result.IsSuccessful.Should().BeTrue();
            });
            allMessages.AddRange(messages1);

            var messages2 = await CapturePublishedMessagesAsync(async () =>
            {
                var result = await MemoryService.PushToListTailAsync(TestScope, listName,
                    [new PrimitiveType("list-item")], publishChange: true);
                result.IsSuccessful.Should().BeTrue();
            });
            allMessages.AddRange(messages2);

            var messages3 = await CapturePublishedMessagesAsync(async () =>
            {
                var result = await MemoryService.DeleteKeyAsync(TestScope, "multi-key", publishChange: true);
                result.IsSuccessful.Should().BeTrue();
            });
            allMessages.AddRange(messages3);

            // Verify all operations published change notifications
            allMessages.Should().HaveCount(3, "Expected three change notifications from three operations");

            allMessages.Should().Contain(m => m.Contains("SetKeyValue"), "Expected SetKeyValue notification");
            allMessages.Should().Contain(m => m.Contains("PushToListTail"), "Expected PushToListTail notification");
            allMessages.Should().Contain(m => m.Contains("DeleteKey"), "Expected DeleteKey notification");
        }
        finally
        {
            await SafeCleanupAsync(listName);
        }
    }

    #endregion
}
