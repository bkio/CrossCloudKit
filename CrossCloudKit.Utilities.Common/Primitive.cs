// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Globalization;
using Newtonsoft.Json;

namespace CrossCloudKit.Utilities.Common;

/// <summary>
/// Represents the different primitive types supported by the Primitive class.
/// </summary>
public enum PrimitiveKind
{
    String,
    Integer,
    Double,
    Boolean,
    ByteArray
}

/// <summary>
/// Represents a discriminated union of primitive types (string, long, double, byte array).
/// This type is immutable and provides type-safe access to the underlying value.
/// </summary>
[JsonConverter(typeof(PrimitiveJsonConverter))]
public sealed class Primitive : IEquatable<Primitive>
{
    public PrimitiveKind Kind { get; }

    private readonly object _value;

    /// <summary>
    /// Copy constructor that creates a new instance from another Primitive.
    /// </summary>
    /// <param name="other">The Primitive instance to copy from</param>
    /// <exception cref="ArgumentNullException">Thrown when other is null</exception>
    public Primitive(Primitive other)
    {
        ArgumentNullException.ThrowIfNull(other);

        Kind = other.Kind;
        _value = other.Kind switch
        {
            PrimitiveKind.ByteArray => ((byte[])other._value).ToArray(), // Create a copy of the byte array
            _ => other._value
        };
    }

    /// <summary>
    /// Creates a Primitive containing a string value.
    /// </summary>
    /// <param name="value">The string value</param>
    /// <exception cref="ArgumentNullException">Thrown when value is null</exception>
    public Primitive(string value)
    {
        ArgumentNullException.ThrowIfNull(value);

        Kind = PrimitiveKind.String;
        _value = value;
    }

    /// <summary>
    /// Creates a Primitive containing a boolean value.
    /// </summary>
    /// <param name="value">The boolean value</param>
    /// <exception cref="ArgumentNullException">Thrown when value is null</exception>
    public Primitive(bool value)
    {
        Kind = PrimitiveKind.Boolean;
        _value = value;
    }

    /// <summary>
    /// Creates a Primitive containing a long integer value.
    /// </summary>
    /// <param name="value">The long integer value</param>
    public Primitive(long value)
    {
        Kind = PrimitiveKind.Integer;
        _value = value;
    }

    /// <summary>
    /// Creates a Primitive containing a long integer value.
    /// </summary>
    /// <param name="value">The long integer value</param>
    public Primitive(int value)
    {
        Kind = PrimitiveKind.Integer;
        _value = value;
    }

    /// <summary>
    /// Creates a Primitive containing a double value.
    /// </summary>
    /// <param name="value">The double value</param>
    public Primitive(double value)
    {
        Kind = PrimitiveKind.Double;
        _value = value;
    }

    /// <summary>
    /// Creates a Primitive containing a double value.
    /// </summary>
    /// <param name="value">The double value</param>
    public Primitive(float value)
    {
        Kind = PrimitiveKind.Double;
        _value = value;
    }

    /// <summary>
    /// Creates a Primitive containing a byte array.
    /// </summary>
    /// <param name="value">The byte array value</param>
    /// <exception cref="ArgumentNullException">Thrown when value is null</exception>
    public Primitive(byte[] value)
    {
        ArgumentNullException.ThrowIfNull(value);

        Kind = PrimitiveKind.ByteArray;
        _value = value.ToArray(); // Create a copy to ensure immutability
    }

    /// <summary>
    /// Creates a Primitive containing a byte array from a ReadOnlySpan.
    /// </summary>
    /// <param name="value">The byte span value</param>
    public Primitive(ReadOnlySpan<byte> value)
    {
        Kind = PrimitiveKind.ByteArray;
        _value = value.ToArray();
    }

    /// <summary>
    /// Gets the string value. Only valid when Kind is String.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when Kind is not String</exception>
    public string AsString => Kind == PrimitiveKind.String
        ? (string)_value
        : throw new InvalidOperationException($"Cannot access string value when Kind is {Kind}");

    /// <summary>
    /// Gets the long integer value. Only valid when Kind is Integer.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when Kind is not Integer</exception>
    public long AsInteger => Kind == PrimitiveKind.Integer
        ? (long)_value
        : throw new InvalidOperationException($"Cannot access integer value when Kind is {Kind}");

    /// <summary>
    /// Gets the long integer value. Only valid when Kind is Integer.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when Kind is not Integer</exception>
    public bool AsBoolean => Kind == PrimitiveKind.Boolean
        ? (bool)_value
        : throw new InvalidOperationException($"Cannot access boolean value when Kind is {Kind}");

    /// <summary>
    /// Gets the double value. Only valid when Kind is Double.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when Kind is not Double</exception>
    public double AsDouble => Kind == PrimitiveKind.Double
        ? (double)_value
        : throw new InvalidOperationException($"Cannot access double value when Kind is {Kind}");

    /// <summary>
    /// Gets a copy of the byte array value. Only valid when Kind is ByteArray.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when Kind is not ByteArray</exception>
    public byte[] AsByteArray => Kind == PrimitiveKind.ByteArray
        ? ((byte[])_value).ToArray()
        : throw new InvalidOperationException($"Cannot access byte array value when Kind is {Kind}");

    /// <summary>
    /// Gets a read-only span of the byte array value. Only valid when Kind is ByteArray.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when Kind is not ByteArray</exception>
    public ReadOnlySpan<byte> AsByteSpan => Kind == PrimitiveKind.ByteArray
        ? ((byte[])_value).AsSpan()
        : throw new InvalidOperationException($"Cannot access byte span value when Kind is {Kind}");

    /// <summary>
    /// Attempts to get the string value if Kind is String.
    /// </summary>
    /// <param name="value">The string value if successful</param>
    /// <returns>True if Kind is String, false otherwise</returns>
    public bool TryGetString(out string? value)
    {
        if (Kind == PrimitiveKind.String)
        {
            value = (string)_value;
            return true;
        }
        value = null;
        return false;
    }

    /// <summary>
    /// Attempts to get the boolean value if Kind is Boolean.
    /// </summary>
    /// <param name="value">The boolean value if successful</param>
    /// <returns>True if Kind is Boolean, false otherwise</returns>
    public bool TryGetBoolean(out bool value)
    {
        if (Kind == PrimitiveKind.Boolean)
        {
            value = (bool)_value;
            return true;
        }
        value = false;
        return false;
    }

    /// <summary>
    /// Attempts to get the integer value if Kind is Integer.
    /// </summary>
    /// <param name="value">The integer value if successful</param>
    /// <returns>True if Kind is Integer, false otherwise</returns>
    public bool TryGetInteger(out long value)
    {
        if (Kind == PrimitiveKind.Integer)
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
        if (Kind == PrimitiveKind.Double)
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
        if (Kind == PrimitiveKind.ByteArray)
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
            PrimitiveKind.String => (string)_value,
            PrimitiveKind.Boolean => ((bool)_value).ToString(CultureInfo.InvariantCulture),
            PrimitiveKind.Integer => ((long)_value).ToString(CultureInfo.InvariantCulture),
            PrimitiveKind.Double => ((double)_value).ToString(CultureInfo.InvariantCulture),
            PrimitiveKind.ByteArray => Convert.ToBase64String((byte[])_value),
            _ => throw new InvalidOperationException($"Unknown primitive type kind: {Kind}")
        };
    }

    public override bool Equals(object? obj) => obj is Primitive other && Equals(other);

    public bool Equals(Primitive? other)
    {
        if (other is null || Kind != other.Kind)
            return false;

        return Kind switch
        {
            PrimitiveKind.String => string.Equals((string)_value, (string)other._value, StringComparison.Ordinal),
            PrimitiveKind.Boolean => (bool)_value == (bool)other._value,
            PrimitiveKind.Integer => (long)_value == (long)other._value,
            PrimitiveKind.Double => Math.Abs((double)_value - (double)other._value) < 0.0000001,
            PrimitiveKind.ByteArray => ((byte[])_value).AsSpan().SequenceEqual(((byte[])other._value).AsSpan()),
            _ => false
        };
    }

    public override int GetHashCode()
    {
        return Kind switch
        {
            PrimitiveKind.String => HashCode.Combine(Kind, _value),
            PrimitiveKind.Boolean => HashCode.Combine(Kind, _value),
            PrimitiveKind.Integer => HashCode.Combine(Kind, _value),
            PrimitiveKind.Double => HashCode.Combine(Kind, _value),
            PrimitiveKind.ByteArray => HashCode.Combine(Kind, Convert.ToBase64String((byte[])_value)),
            _ => HashCode.Combine(Kind)
        };
    }

    public static bool operator ==(Primitive? left, Primitive? right) =>
        ReferenceEquals(left, right) || (left?.Equals(right) == true);

    public static bool operator !=(Primitive? left, Primitive? right) => !(left == right);

    // Implicit conversion operators for convenience
    public static implicit operator Primitive(string value) => new(value);
    public static implicit operator Primitive(bool value) => new(value);
    public static implicit operator Primitive(long value) => new(value);
    public static implicit operator Primitive(int value) => new(value);
    public static implicit operator Primitive(double value) => new(value);
    public static implicit operator Primitive(float value) => new(value);
    public static implicit operator Primitive(byte[] value) => new(value);

    public static implicit operator string(Primitive value) => value.AsString;
    public static implicit operator bool(Primitive value) => value.AsBoolean;
    public static implicit operator long(Primitive value) => value.AsInteger;
    public static implicit operator double(Primitive value) => value.AsDouble;
    public static implicit operator byte[](Primitive value) => value.AsByteArray;

    /// <summary>
    /// Executes the appropriate action based on the Kind of the Primitive.
    /// </summary>
    /// <param name="onString">Action to execute if Kind is String</param>
    /// <param name="onBoolean">Action to execute if Kind is Boolean</param>
    /// <param name="onInteger">Action to execute if Kind is Integer</param>
    /// <param name="onDouble">Action to execute if Kind is Double</param>
    /// <param name="onByteArray">Action to execute if Kind is ByteArray</param>
    public void Match(
        Action<string>? onString = null,
        Action<bool>? onBoolean = null,
        Action<long>? onInteger = null,
        Action<double>? onDouble = null,
        Action<ReadOnlySpan<byte>>? onByteArray = null)
    {
        switch (Kind)
        {
            case PrimitiveKind.String:
                onString?.Invoke((string)_value);
                break;
            case PrimitiveKind.Boolean:
                onBoolean?.Invoke((bool)_value);
                break;
            case PrimitiveKind.Integer:
                onInteger?.Invoke((long)_value);
                break;
            case PrimitiveKind.Double:
                onDouble?.Invoke((double)_value);
                break;
            case PrimitiveKind.ByteArray:
                onByteArray?.Invoke(((byte[])_value).AsSpan());
                break;
        }
    }

    /// <summary>
    /// Returns a result by executing the appropriate function based on the Kind of the Primitive.
    /// </summary>
    /// <typeparam name="T">The return type</typeparam>
    /// <param name="onString">Function to execute if Kind is String</param>
    /// <param name="onBoolean">Function to execute if Kind is Boolean</param>
    /// <param name="onInteger">Function to execute if Kind is Integer</param>
    /// <param name="onDouble">Function to execute if Kind is Double</param>
    /// <param name="onByteArray">Function to execute if Kind is ByteArray</param>
    /// <returns>The result of the executed function</returns>
    public T Match<T>(
        Func<string, T> onString,
        Func<bool, T> onBoolean,
        Func<long, T> onInteger,
        Func<double, T> onDouble,
        Func<ReadOnlySpan<byte>, T> onByteArray)
    {
        return Kind switch
        {
            PrimitiveKind.String => onString((string)_value),
            PrimitiveKind.Boolean => onBoolean((bool)_value),
            PrimitiveKind.Integer => onInteger((long)_value),
            PrimitiveKind.Double => onDouble((double)_value),
            PrimitiveKind.ByteArray => onByteArray(((byte[])_value).AsSpan()),
            _ => throw new InvalidOperationException($"Unknown primitive type kind: {Kind}")
        };
    }
}

/// <summary>
/// Custom JSON converter for Primitive.
/// Serializes as { "kind": "String|Integer|Double|ByteArray", "value": ... } for unambiguous deserialization.
/// </summary>
public class PrimitiveJsonConverter : JsonConverter<Primitive>
{
    /// <summary>
    /// Writes the Primitive value to JSON using kind + value format.
    /// </summary>
    public override void WriteJson(JsonWriter writer, Primitive? value, JsonSerializer serializer)
    {
        if (value == null)
        {
            writer.WriteNull();
            return;
        }

        switch (value.Kind)
        {
            case PrimitiveKind.String:
                writer.WriteValue($"s-{value.AsString}");
                break;

            case PrimitiveKind.Integer:
                writer.WriteValue(value.AsInteger);
                break;

            case PrimitiveKind.Boolean:
                writer.WriteValue(value.AsBoolean);
                break;

            case PrimitiveKind.Double:
                writer.WriteValue($"d-{value.AsDouble.ToString(CultureInfo.InvariantCulture)}");
                break;

            case PrimitiveKind.ByteArray:
                writer.WriteValue($"b-{Convert.ToBase64String(value.AsByteArray)}");
                break;

            default:
                throw new JsonSerializationException($"Unsupported kind {value.Kind}");
        }
    }

    /// <summary>
    /// Reads JSON back into a Primitive.
    /// Detects the JSON token type and reconstructs the appropriate Primitive.
    /// </summary>
    public override Primitive? ReadJson(JsonReader reader, Type objectType, Primitive? existingValue, bool hasExistingValue, JsonSerializer serializer)
    {
        if (reader.TokenType == JsonToken.Null)
            return null;

        if (reader.TokenType == JsonToken.String)
        {
            var str = (string)reader.Value.NotNull();
            if (str.StartsWith("b-"))
            {
                return new Primitive(Convert.FromBase64String(str[2..]));
            }
            if (str.StartsWith("d-"))
            {
                return new Primitive(double.Parse(str[2..], CultureInfo.InvariantCulture));
            }

            return str.StartsWith("s-") ? new Primitive(str[2..]) : new Primitive(str);
        }

        if (reader.TokenType == JsonToken.Boolean) return new Primitive(Convert.ToBoolean(reader.Value));

        if (reader.TokenType == JsonToken.Integer) return new Primitive(Convert.ToInt64(reader.Value));

        if (reader.TokenType == JsonToken.Float) return new Primitive(Convert.ToDouble(reader.Value));

        throw new JsonSerializationException(
            $"Unexpected token {reader.TokenType} when parsing Primitive");
    }
}
