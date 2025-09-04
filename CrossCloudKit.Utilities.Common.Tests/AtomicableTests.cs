// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Globalization;
using Xunit;

namespace CrossCloudKit.Utilities.Common.Tests;

public class AtomicableTests
{
    [Fact]
    public void Atomicable_Constructor_WithInitialValue_SetsValueCorrectly()
    {
        // Arrange
        const int initialValue = 42;

        // Act
        var atomicable = new Atomicable<int>(initialValue);

        // Assert
        Assert.Equal(initialValue, atomicable.Value);
        Assert.Equal(initialValue, atomicable.GetValue());
    }

    [Theory]
    [InlineData(ThreadSafetyMode.SingleProducer)]
    [InlineData(ThreadSafetyMode.MultipleProducers)]
    public void Atomicable_Constructor_WithThreadSafetyMode_InitializesCorrectly(ThreadSafetyMode mode)
    {
        // Arrange
        const string initialValue = "test";

        // Act
        var atomicable = new Atomicable<string>(initialValue, mode);

        // Assert
        Assert.Equal(initialValue, atomicable.Value);
    }

    [Fact]
    public void Atomicable_SetValue_SingleProducer_UpdatesValueCorrectly()
    {
        // Arrange
        var atomicable = new Atomicable<int>(0);
        const int newValue = 100;

        // Act
        atomicable.SetValue(newValue);

        // Assert
        Assert.Equal(newValue, atomicable.GetValue());
    }

    [Fact]
    public void Atomicable_SetValue_MultipleProducers_UpdatesValueCorrectly()
    {
        // Arrange
        var atomicable = new Atomicable<int>(0, ThreadSafetyMode.MultipleProducers);
        const int newValue = 200;

        // Act
        atomicable.SetValue(newValue);

        // Assert
        Assert.Equal(newValue, atomicable.GetValue());
    }

    [Fact]
    public void Atomicable_ValueProperty_GetSet_WorksCorrectly()
    {
        // Arrange
        var atomicable = new Atomicable<double>(3.14);
        const double newValue = 2.71;

        // Act
        atomicable.Value = newValue;

        // Assert
        Assert.Equal(newValue, atomicable.Value);
    }

    [Fact]
    public void Atomicable_CompareAndSet_WithMatchingValue_ReturnsTrue()
    {
        // Arrange
        var atomicable = new Atomicable<int>(50);
        const int expectedValue = 50;
        const int newValue = 75;

        // Act
        var result = atomicable.CompareAndSet(expectedValue, newValue);

        // Assert
        Assert.True(result);
        Assert.Equal(newValue, atomicable.GetValue());
    }

    [Fact]
    public void Atomicable_CompareAndSet_WithNonMatchingValue_ReturnsFalse()
    {
        // Arrange
        var atomicable = new Atomicable<int>(50);
        const int expectedValue = 25; // Different from actual value
        const int newValue = 75;
        const int originalValue = 50;

        // Act
        var result = atomicable.CompareAndSet(expectedValue, newValue);

        // Assert
        Assert.False(result);
        Assert.Equal(originalValue, atomicable.GetValue()); // Value should remain unchanged
    }

    [Theory]
    [InlineData(ThreadSafetyMode.SingleProducer)]
    [InlineData(ThreadSafetyMode.MultipleProducers)]
    public void Atomicable_CompareAndSet_BothModes_WorkCorrectly(ThreadSafetyMode mode)
    {
        // Arrange
        var atomicable = new Atomicable<string>("initial", mode);
        const string expectedValue = "initial";
        const string newValue = "updated";

        // Act
        var result = atomicable.CompareAndSet(expectedValue, newValue);

        // Assert
        Assert.True(result);
        Assert.Equal(newValue, atomicable.GetValue());
    }

    [Fact]
    public void Atomicable_Exchange_ReturnsOriginalValue()
    {
        // Arrange
        const int originalValue = 10;
        const int newValue = 20;
        var atomicable = new Atomicable<int>(originalValue);

        // Act
        var returnedValue = atomicable.Exchange(newValue);

        // Assert
        Assert.Equal(originalValue, returnedValue);
        Assert.Equal(newValue, atomicable.GetValue());
    }

    [Theory]
    [InlineData(ThreadSafetyMode.SingleProducer)]
    [InlineData(ThreadSafetyMode.MultipleProducers)]
    public void Atomicable_Exchange_BothModes_WorkCorrectly(ThreadSafetyMode mode)
    {
        // Arrange
        const bool originalValue = true;
        const bool newValue = false;
        var atomicable = new Atomicable<bool>(originalValue, mode);

        // Act
        var returnedValue = atomicable.Exchange(newValue);

        // Assert
        Assert.Equal(originalValue, returnedValue);
        Assert.Equal(newValue, atomicable.GetValue());
    }

    [Fact]
    public void Atomicable_ToString_ReturnsCorrectStringRepresentation()
    {
        // Arrange
        const int value = 123;
        var atomicable = new Atomicable<int>(value);

        // Act
        var result = atomicable.ToString();

        // Assert
        Assert.Equal(value.ToString(), result);
    }

    [Fact]
    public void Atomicable_ToString_WithNullValue_ReturnsNull()
    {
        // Arrange
        var atomicable = new Atomicable<string?>(null);

        // Act
        var result = atomicable.ToString();

        // Assert
        Assert.Equal("null", result);
    }

    [Fact]
    public void Atomicable_ImplicitConversion_WorksCorrectly()
    {
        // Arrange
        const int expectedValue = 456;
        var atomicable = new Atomicable<int>(expectedValue);

        // Act
        int convertedValue = atomicable;

        // Assert
        Assert.Equal(expectedValue, convertedValue);
    }

    [Fact]
    public void Atomicable_ImplicitConversion_WithNull_ThrowsArgumentNullException()
    {
        // Arrange
        Atomicable<int>? nullAtomicable = null;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
        {
            // ReSharper disable once UnusedVariable
            int value = nullAtomicable!;
        });
    }

    [Fact]
    public async Task Atomicable_MultipleProducers_ThreadSafety_ConcurrentOperations()
    {
        // Arrange
        var atomicable = new Atomicable<int>(0, ThreadSafetyMode.MultipleProducers);
        const int numberOfTasks = 100;
        const int incrementsPerTask = 100;

        // Act
        var tasks = new List<Task>();
        for (int i = 0; i < numberOfTasks; i++)
        {
            tasks.Add(Task.Run(() =>
            {
                for (int j = 0; j < incrementsPerTask; j++)
                {
                    int currentValue;
                    int newValue;
                    do
                    {
                        currentValue = atomicable.GetValue();
                        newValue = currentValue + 1;
                    } while (!atomicable.CompareAndSet(currentValue, newValue));
                }
            }));
        }

        await Task.WhenAll(tasks);

        // Assert
        var expectedValue = numberOfTasks * incrementsPerTask;
        Assert.Equal(expectedValue, atomicable.GetValue());
    }

    [Fact]
    public void ReadOnlyAtomicable_Constructor_WithValidAtomicable_InitializesCorrectly()
    {
        // Arrange
        const int initialValue = 789;
        var atomicable = new Atomicable<int>(initialValue);

        // Act
        var readOnlyAtomicable = new ReadOnlyAtomicable<int>(atomicable);

        // Assert
        Assert.Equal(initialValue, readOnlyAtomicable.Value);
        Assert.Equal(initialValue, readOnlyAtomicable.GetValue());
    }

    [Fact]
    public void ReadOnlyAtomicable_Constructor_WithNull_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new ReadOnlyAtomicable<int>(null!));
    }

    [Fact]
    public void ReadOnlyAtomicable_Value_ReflectsUnderlyingAtomicableChanges()
    {
        // Arrange
        var atomicable = new Atomicable<string>("original");
        var readOnlyAtomicable = new ReadOnlyAtomicable<string>(atomicable);
        const string newValue = "modified";

        // Act
        atomicable.SetValue(newValue);

        // Assert
        Assert.Equal(newValue, readOnlyAtomicable.Value);
        Assert.Equal(newValue, readOnlyAtomicable.GetValue());
    }

    [Fact]
    public void ReadOnlyAtomicable_ToString_ReturnsCorrectStringRepresentation()
    {
        // Arrange
        const double value = 3.14159;
        var atomicable = new Atomicable<double>(value);
        var readOnlyAtomicable = new ReadOnlyAtomicable<double>(atomicable);

        // Act
        var result = readOnlyAtomicable.ToString();

        // Assert
        Assert.Equal(value.ToString(CultureInfo.InvariantCulture), result);
    }

    [Fact]
    public void ReadOnlyAtomicable_ImplicitConversion_WorksCorrectly()
    {
        // Arrange
        const long expectedValue = 9876543210L;
        var atomicable = new Atomicable<long>(expectedValue);
        var readOnlyAtomicable = new ReadOnlyAtomicable<long>(atomicable);

        // Act
        long convertedValue = readOnlyAtomicable;

        // Assert
        Assert.Equal(expectedValue, convertedValue);
    }

    [Fact]
    public void Atomicable_WithComplexType_WorksCorrectly()
    {
        // Arrange
        var initialList = new List<int> { 1, 2, 3 };
        var atomicable = new Atomicable<List<int>>(initialList);
        var newList = new List<int> { 4, 5, 6 };

        // Act
        atomicable.SetValue(newList);

        // Assert
        Assert.Equal(newList, atomicable.GetValue());
        Assert.NotEqual(initialList, atomicable.GetValue());
    }

    [Fact]
    public void Atomicable_CompareAndSet_WithComplexType_UsesEqualityComparer()
    {
        // Arrange
        var list1 = new List<int> { 1, 2, 3 };
        var list2 = new List<int> { 1, 2, 3 }; // Same content, different instance
        var list3 = new List<int> { 4, 5, 6 };
        var atomicable = new Atomicable<List<int>>(list1);

        // Act
        var result1 = atomicable.CompareAndSet(list2, list3); // Should fail - different instances
        var currentValueAfterFirst = atomicable.GetValue();
        var result2 = atomicable.CompareAndSet(list1, list3); // Should succeed - same instance

        // Assert
        Assert.False(result1);
        Assert.Same(list1, currentValueAfterFirst); // Still original instance

        Assert.True(result2);
        Assert.Same(list3, atomicable.GetValue()); // Now updated value
    }
}
