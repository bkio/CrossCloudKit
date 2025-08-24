// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Globalization;
using System.Text.Json.Serialization;
// ReSharper disable MemberCanBePrivate.Global

namespace Utilities.Common;

/// <summary>
/// Represents the different primitive types supported by the PrimitiveType class.
/// </summary>
public enum PrimitiveTypeKind
{
    String,
    Integer,
    Double,
    ByteArray
}

/// <summary>
/// Represents a discriminated union of primitive types (string, long, double, byte array).
/// This type is immutable and provides type-safe access to the underlying value.
/// </summary>
public sealed class PrimitiveType : IEquatable<PrimitiveType>
{
    public PrimitiveTypeKind Kind { get; }

    private readonly object _value;

    /// <summary>
    /// Copy constructor that creates a new instance from another PrimitiveType.
    /// </summary>
    /// <param name="other">The PrimitiveType instance to copy from</param>
    /// <exception cref="ArgumentNullException">Thrown when other is null</exception>
    public PrimitiveType(PrimitiveType other)
    {
        ArgumentNullException.ThrowIfNull(other);

        Kind = other.Kind;
        _value = other.Kind switch
        {
            PrimitiveTypeKind.ByteArray => ((byte[])other._value).ToArray(), // Create a copy of the byte array
            _ => other._value
        };
    }

    /// <summary>
    /// Creates a PrimitiveType containing a string value.
    /// </summary>
    /// <param name="value">The string value</param>
    public PrimitiveType(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        Kind = PrimitiveTypeKind.String;
        _value = value;
    }

    /// <summary>
    /// Creates a PrimitiveType containing a long integer value.
    /// </summary>
    /// <param name="value">The long integer value</param>
    public PrimitiveType(long value)
    {
        Kind = PrimitiveTypeKind.Integer;
        _value = value;
    }

    /// <summary>
    /// Creates a PrimitiveType containing a double value.
    /// </summary>
    /// <param name="value">The double value</param>
    public PrimitiveType(double value)
    {
        Kind = PrimitiveTypeKind.Double;
        _value = value;
    }

    /// <summary>
    /// Creates a PrimitiveType containing a byte array.
    /// </summary>
    /// <param name="value">The byte array value</param>
    /// <exception cref="ArgumentNullException">Thrown when value is null</exception>
    public PrimitiveType(byte[] value)
    {
        ArgumentNullException.ThrowIfNull(value);
        Kind = PrimitiveTypeKind.ByteArray;
        _value = value.ToArray(); // Create a copy to ensure immutability
    }

    /// <summary>
    /// Creates a PrimitiveType containing a byte array from a ReadOnlySpan.
    /// </summary>
    /// <param name="value">The byte span value</param>
    public PrimitiveType(ReadOnlySpan<byte> value)
    {
        Kind = PrimitiveTypeKind.ByteArray;
        _value = value.ToArray();
    }

    /// <summary>
    /// Gets the string value. Only valid when Kind is String.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when Kind is not String</exception>
    public string AsString => Kind == PrimitiveTypeKind.String
        ? (string)_value
        : throw new InvalidOperationException($"Cannot access string value when Kind is {Kind}");

    /// <summary>
    /// Gets the long integer value. Only valid when Kind is Integer.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when Kind is not Integer</exception>
    public long AsInteger => Kind == PrimitiveTypeKind.Integer
        ? (long)_value
        : throw new InvalidOperationException($"Cannot access integer value when Kind is {Kind}");

    /// <summary>
    /// Gets the double value. Only valid when Kind is Double.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when Kind is not Double</exception>
    public double AsDouble => Kind == PrimitiveTypeKind.Double
        ? (double)_value
        : throw new InvalidOperationException($"Cannot access double value when Kind is {Kind}");

    /// <summary>
    /// Gets a copy of the byte array value. Only valid when Kind is ByteArray.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when Kind is not ByteArray</exception>
    public byte[] AsByteArray => Kind == PrimitiveTypeKind.ByteArray
        ? ((byte[])_value).ToArray()
        : throw new InvalidOperationException($"Cannot access byte array value when Kind is {Kind}");

    /// <summary>
    /// Gets a read-only span of the byte array value. Only valid when Kind is ByteArray.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when Kind is not ByteArray</exception>
    public ReadOnlySpan<byte> AsByteSpan => Kind == PrimitiveTypeKind.ByteArray
        ? ((byte[])_value).AsSpan()
        : throw new InvalidOperationException($"Cannot access byte span value when Kind is {Kind}");

    /// <summary>
    /// Attempts to get the string value if Kind is String.
    /// </summary>
    /// <param name="value">The string value if successful</param>
    /// <returns>True if Kind is String, false otherwise</returns>
    public bool TryGetString(out string? value)
    {
        if (Kind == PrimitiveTypeKind.String)
        {
            value = (string)_value;
            return true;
        }
        value = null;
        return false;
    }

    /// <summary>
    /// Attempts to get the integer value if Kind is Integer.
    /// </summary>
    /// <param name="value">The integer value if successful</param>
    /// <returns>True if Kind is Integer, false otherwise</returns>
    public bool TryGetInteger(out long value)
    {
        if (Kind == PrimitiveTypeKind.Integer)
        {
            value = (long)_value;
            return true;
        }
        value = 0;
        return false;
    }

    /// <summary>
    /// Attempts to get the double value if Kind is Double.
    /// </summary>
    /// <param name="value">The double value if successful</param>
    /// <returns>True if Kind is Double, false otherwise</returns>
    public bool TryGetDouble(out double value)
    {
        if (Kind == PrimitiveTypeKind.Double)
        {
            value = (double)_value;
            return true;
        }
        value = 0.0;
        return false;
    }

    /// <summary>
    /// Attempts to get a copy of the byte array value if Kind is ByteArray.
    /// </summary>
    /// <param name="value">The byte array value if successful</param>
    /// <returns>True if Kind is ByteArray, false otherwise</returns>
    public bool TryGetByteArray(out byte[]? value)
    {
        if (Kind == PrimitiveTypeKind.ByteArray)
        {
            value = ((byte[])_value).ToArray();
            return true;
        }
        value = null;
        return false;
    }

    public override string ToString()
    {
        return Kind switch
        {
            PrimitiveTypeKind.String => (string)_value,
            PrimitiveTypeKind.Integer => ((long)_value).ToString(),
            PrimitiveTypeKind.Double => ((double)_value).ToString(CultureInfo.InvariantCulture),
            PrimitiveTypeKind.ByteArray => Convert.ToBase64String((byte[])_value),
            _ => throw new InvalidOperationException($"Unknown primitive type kind: {Kind}")
        };
    }

    public override bool Equals(object? obj) => obj is PrimitiveType other && Equals(other);

    public bool Equals(PrimitiveType? other)
    {
        if (other is null || Kind != other.Kind)
            return false;

        return Kind switch
        {
            PrimitiveTypeKind.String => string.Equals((string)_value, (string)other._value, StringComparison.Ordinal),
            PrimitiveTypeKind.Integer => (long)_value == (long)other._value,
            PrimitiveTypeKind.Double => Math.Abs((double)_value - (double)other._value) < 0.0000001,
            PrimitiveTypeKind.ByteArray => ((byte[])_value).AsSpan().SequenceEqual(((byte[])other._value).AsSpan()),
            _ => false
        };
    }

    public override int GetHashCode()
    {
        return Kind switch
        {
            PrimitiveTypeKind.String => HashCode.Combine(Kind, _value),
            PrimitiveTypeKind.Integer => HashCode.Combine(Kind, _value),
            PrimitiveTypeKind.Double => HashCode.Combine(Kind, _value),
            PrimitiveTypeKind.ByteArray => HashCode.Combine(Kind, Convert.ToBase64String((byte[])_value)),
            _ => HashCode.Combine(Kind)
        };
    }

    public static bool operator ==(PrimitiveType? left, PrimitiveType? right) =>
        ReferenceEquals(left, right) || (left?.Equals(right) == true);

    public static bool operator !=(PrimitiveType? left, PrimitiveType? right) => !(left == right);

    // Implicit conversion operators for convenience
    public static implicit operator PrimitiveType(string value) => new(value);
    public static implicit operator PrimitiveType(long value) => new(value);
    public static implicit operator PrimitiveType(double value) => new(value);
    public static implicit operator PrimitiveType(byte[] value) => new(value);

    /// <summary>
    /// Executes the appropriate action based on the Kind of the PrimitiveType.
    /// </summary>
    /// <param name="onString">Action to execute if Kind is String</param>
    /// <param name="onInteger">Action to execute if Kind is Integer</param>
    /// <param name="onDouble">Action to execute if Kind is Double</param>
    /// <param name="onByteArray">Action to execute if Kind is ByteArray</param>
    public void Match(
        Action<string>? onString = null,
        Action<long>? onInteger = null,
        Action<double>? onDouble = null,
        Action<ReadOnlySpan<byte>>? onByteArray = null)
    {
        switch (Kind)
        {
            case PrimitiveTypeKind.String:
                onString?.Invoke((string)_value);
                break;
            case PrimitiveTypeKind.Integer:
                onInteger?.Invoke((long)_value);
                break;
            case PrimitiveTypeKind.Double:
                onDouble?.Invoke((double)_value);
                break;
            case PrimitiveTypeKind.ByteArray:
                onByteArray?.Invoke(((byte[])_value).AsSpan());
                break;
        }
    }

    /// <summary>
    /// Returns a result by executing the appropriate function based on the Kind of the PrimitiveType.
    /// </summary>
    /// <typeparam name="T">The return type</typeparam>
    /// <param name="onString">Function to execute if Kind is String</param>
    /// <param name="onInteger">Function to execute if Kind is Integer</param>
    /// <param name="onDouble">Function to execute if Kind is Double</param>
    /// <param name="onByteArray">Function to execute if Kind is ByteArray</param>
    /// <returns>The result of the executed function</returns>
    public T Match<T>(
        Func<string, T> onString,
        Func<long, T> onInteger,
        Func<double, T> onDouble,
        Func<ReadOnlySpan<byte>, T> onByteArray)
    {
        ArgumentNullException.ThrowIfNull(onString);
        ArgumentNullException.ThrowIfNull(onInteger);
        ArgumentNullException.ThrowIfNull(onDouble);
        ArgumentNullException.ThrowIfNull(onByteArray);

        return Kind switch
        {
            PrimitiveTypeKind.String => onString((string)_value),
            PrimitiveTypeKind.Integer => onInteger((long)_value),
            PrimitiveTypeKind.Double => onDouble((double)_value),
            PrimitiveTypeKind.ByteArray => onByteArray(((byte[])_value).AsSpan()),
            _ => throw new InvalidOperationException($"Unknown primitive type kind: {Kind}")
        };
    }
}

/// <summary>
/// Serializable wrapper for PrimitiveType that can be used for JSON serialization.
/// Uses System.Text.Json for modern .NET serialization.
/// </summary>
public sealed class SerializablePrimitiveType
{
    [JsonPropertyName("kind")]
    public PrimitiveTypeKind Kind { get; set; }

    [JsonPropertyName("value")]
    public string Value { get; set; } = string.Empty;

    /// <summary>
    /// Default constructor for JSON deserialization.
    /// </summary>
    public SerializablePrimitiveType() { }

    /// <summary>
    /// Creates a SerializablePrimitiveType from a PrimitiveType.
    /// </summary>
    /// <param name="primitiveType">The PrimitiveType to serialize</param>
    public SerializablePrimitiveType(PrimitiveType primitiveType)
    {
        ArgumentNullException.ThrowIfNull(primitiveType);

        Kind = primitiveType.Kind;
        Value = primitiveType.Kind switch
        {
            PrimitiveTypeKind.String => primitiveType.AsString,
            PrimitiveTypeKind.Integer => primitiveType.AsInteger.ToString(),
            PrimitiveTypeKind.Double => primitiveType.AsDouble.ToString("R"), // Round-trip format
            PrimitiveTypeKind.ByteArray => Convert.ToBase64String(primitiveType.AsByteArray),
            _ => throw new InvalidOperationException($"Unknown primitive type kind: {primitiveType.Kind}")
        };
    }

    /// <summary>
    /// Converts this serializable wrapper back to a PrimitiveType.
    /// </summary>
    /// <returns>The deserialized PrimitiveType</returns>
    /// <exception cref="FormatException">Thrown when the Value cannot be parsed for the specified Kind</exception>
    public PrimitiveType ToPrimitiveType()
    {
        return Kind switch
        {
            PrimitiveTypeKind.String => new PrimitiveType(Value),
            PrimitiveTypeKind.Integer => new PrimitiveType(long.Parse(Value)),
            PrimitiveTypeKind.Double => new PrimitiveType(double.Parse(Value)),
            PrimitiveTypeKind.ByteArray => new PrimitiveType(Convert.FromBase64String(Value)),
            _ => throw new InvalidOperationException($"Unknown primitive type kind: {Kind}")
        };
    }

    /// <summary>
    /// Converts an array of PrimitiveType instances to an array of SerializablePrimitiveType instances.
    /// </summary>
    /// <param name="array">The array to convert</param>
    /// <returns>The converted array, or empty array if input is null or empty</returns>
    public static SerializablePrimitiveType[] FromPrimitiveTypes(PrimitiveType[]? array)
    {
        if (array is null || array.Length == 0)
            return [];

        return array.Select(p => new SerializablePrimitiveType(p)).ToArray();
    }

    /// <summary>
    /// Converts an array of SerializablePrimitiveType instances to an array of PrimitiveType instances.
    /// </summary>
    /// <param name="array">The array to convert</param>
    /// <returns>The converted array, or empty array if input is null or empty</returns>
    public static PrimitiveType[] ToPrimitiveTypes(SerializablePrimitiveType[]? array)
    {
        if (array is null || array.Length == 0)
            return [];

        return array.Select(s => s.ToPrimitiveType()).ToArray();
    }

    // Implicit conversion operators
    public static implicit operator SerializablePrimitiveType(PrimitiveType primitiveType) => new(primitiveType);
    public static implicit operator PrimitiveType(SerializablePrimitiveType serializableType) => serializableType.ToPrimitiveType();
}
