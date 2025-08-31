// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

// ReSharper disable UnusedType.Global
// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable ClassNeverInstantiated.Global
namespace CrossCloudKit.Utilities.Common;

/// <summary>
/// Specifies the thread safety requirements for atomic operations
/// </summary>
public enum ThreadSafetyMode
{
    /// <summary>
    /// Single producer, no synchronization overhead
    /// </summary>
    SingleProducer,
    /// <summary>
    /// Multiple producers, thread safety ensured through synchronization
    /// </summary>
    MultipleProducers
}

/// <summary>
/// Provides thread-safe access to a value of type T with configurable synchronization behavior.
/// Suitable for passing primitive values by reference and ensuring atomic operations.
/// </summary>
/// <typeparam name="T">The type of value to store atomically</typeparam>
/// <remarks>
/// Initializes a new instance of the <see cref="Atomicable{T}"/> class
/// </remarks>
/// <param name="initialValue">The initial value to store</param>
/// <param name="threadSafetyMode">The thread safety mode (default is SingleProducer)</param>
public sealed class Atomicable<T>(T initialValue, ThreadSafetyMode threadSafetyMode = ThreadSafetyMode.SingleProducer)
{
    private T _value = initialValue;
    private readonly ThreadSafetyMode _threadSafetyMode = threadSafetyMode;
    private readonly Lock _lockObject = new();

    /// <summary>
    /// Gets or sets the atomic value
    /// </summary>
    public T Value
    {
        get => GetValue();
        set => SetValue(value);
    }

    /// <summary>
    /// Gets the current value atomically
    /// </summary>
    /// <returns>The current value</returns>
    public T GetValue()
    {
        if (_threadSafetyMode != ThreadSafetyMode.MultipleProducers) return _value;
        lock (_lockObject)
        {
            return _value;
        }

    }

    /// <summary>
    /// Sets the value atomically
    /// </summary>
    /// <param name="newValue">The new value to set</param>
    public void SetValue(T newValue)
    {
        if (_threadSafetyMode == ThreadSafetyMode.MultipleProducers)
        {
            lock (_lockObject)
            {
                _value = newValue;
            }
        }
        else
        {
            _value = newValue;
        }
    }

    /// <summary>
    /// Atomically compares the current value with the expected value and,
    /// if they are equal, replaces the current value with the new value
    /// </summary>
    /// <param name="expectedValue">The value that is expected to be equal to the current value</param>
    /// <param name="newValue">The value to set if the comparison results in equality</param>
    /// <returns>true if the value was exchanged; otherwise, false</returns>
    public bool CompareAndSet(T expectedValue, T newValue)
    {
        if (_threadSafetyMode == ThreadSafetyMode.MultipleProducers)
        {
            lock (_lockObject)
            {
                if (!EqualityComparer<T>.Default.Equals(_value, expectedValue)) return false;
                _value = newValue;
                return true;
            }
        }

        if (!EqualityComparer<T>.Default.Equals(_value, expectedValue)) return false;
        _value = newValue;
        return true;
    }

    /// <summary>
    /// Atomically sets the value and returns the original value
    /// </summary>
    /// <param name="newValue">The new value to set</param>
    /// <returns>The original value before the exchange</returns>
    public T Exchange(T newValue)
    {
        if (_threadSafetyMode == ThreadSafetyMode.MultipleProducers)
        {
            lock (_lockObject)
            {
                var originalValue = _value;
                _value = newValue;
                return originalValue;
            }
        }
        else
        {
            var originalValue = _value;
            _value = newValue;
            return originalValue;
        }
    }

    /// <summary>
    /// Returns a string representation of the current value
    /// </summary>
    public override string ToString()
    {
        return GetValue()?.ToString() ?? "null";
    }

    /// <summary>
    /// Implicit conversion to the underlying type T
    /// </summary>
    public static implicit operator T(Atomicable<T> atomicable)
    {
        return atomicable == null ? throw new ArgumentNullException(nameof(atomicable)) : atomicable.GetValue();
    }
}

/// <summary>
/// Provides read-only access to an <see cref="Atomicable{T}"/> value.
/// This wrapper ensures that the value cannot be modified through this interface.
/// </summary>
/// <typeparam name="T">The type of value to provide read-only access to</typeparam>
/// <remarks>
/// Initializes a new instance of the <see cref="ReadOnlyAtomicable{T}"/> class
/// </remarks>
/// <param name="atomicable">The atomicable instance to provide read-only access to</param>
/// <exception cref="ArgumentNullException">Thrown when atomicable is null</exception>
public sealed class ReadOnlyAtomicable<T>(Atomicable<T> atomicable)
{
    private readonly Atomicable<T> _atomicable = atomicable ?? throw new ArgumentNullException(nameof(atomicable));

    /// <summary>
    /// Gets the current value
    /// </summary>
    public T Value => _atomicable.GetValue();

    /// <summary>
    /// Gets the current value
    /// </summary>
    /// <returns>The current value</returns>
    public T GetValue()
    {
        return _atomicable.GetValue();
    }

    /// <summary>
    /// Returns a string representation of the current value
    /// </summary>
    public override string ToString()
    {
        return _atomicable.ToString();
    }

    /// <summary>
    /// Implicit conversion to the underlying type T
    /// </summary>
    public static implicit operator T(ReadOnlyAtomicable<T> readOnlyAtomicable)
    {
        return readOnlyAtomicable.GetValue();
    }
}
