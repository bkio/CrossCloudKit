// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Globalization;
using Newtonsoft.Json;

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
[JsonConverter(typeof(PrimitiveTypeJsonConverter))]
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
/// Custom JSON converter for PrimitiveType.
/// Serializes as { "kind": "String|Integer|Double|ByteArray", "value": ... } for unambiguous deserialization.
/// </summary>
public class PrimitiveTypeJsonConverter : JsonConverter<PrimitiveType>
{
    /// <summary>
    /// Writes the PrimitiveType value to JSON using kind + value format.
    /// </summary>
    public override void WriteJson(JsonWriter writer, PrimitiveType? value, JsonSerializer serializer)
    {
        if (value == null)
        {
            writer.WriteNull();
            return;
        }

        writer.WriteStartObject();

        // Write the kind as a string
        writer.WritePropertyName("kind");
        writer.WriteValue(value.Kind.ToString());

        // Write the value according to the kind
        writer.WritePropertyName("value");
        switch (value.Kind)
        {
            case PrimitiveTypeKind.String:
                writer.WriteValue(value.AsString);
                break;

            case PrimitiveTypeKind.Integer:
                writer.WriteValue(value.AsInteger);
                break;

            case PrimitiveTypeKind.Double:
                writer.WriteValue(value.AsDouble);
                break;

            case PrimitiveTypeKind.ByteArray:
                writer.WriteValue(Convert.ToBase64String(value.AsByteArray));
                break;

            default:
                throw new JsonSerializationException($"Unsupported kind {value.Kind}");
        }

        writer.WriteEndObject();
    }

    /// <summary>
    /// Reads JSON back into a PrimitiveType.
    /// Expects JSON in the { "kind": ..., "value": ... } format.
    /// </summary>
    public override PrimitiveType ReadJson(JsonReader reader, Type objectType, PrimitiveType? existingValue, bool hasExistingValue, JsonSerializer serializer)
    {
        if (reader.TokenType == JsonToken.Null)
            return null!;

        if (reader.TokenType != JsonToken.StartObject)
            throw new JsonSerializationException($"Expected StartObject but got {reader.TokenType}");

        // Temporary variables to hold parsed values
        PrimitiveTypeKind? kind = null;
        object? value = null;

        // Read the properties
        while (reader.Read())
        {
            if (reader.TokenType == JsonToken.EndObject)
                break;

            if (reader.TokenType != JsonToken.PropertyName)
                continue;

            var propertyName = (string)reader.Value!;
            reader.Read(); // Move to property value

            switch (propertyName)
            {
                case "kind":
                    kind = Enum.Parse<PrimitiveTypeKind>((string)reader.Value!, ignoreCase: true);
                    break;

                case "value":
                    value = reader.Value;
                    break;
            }
        }

        if (kind == null)
            throw new JsonSerializationException("Missing 'kind' property");

        // Construct PrimitiveType based on kind
        return kind.Value switch
        {
            PrimitiveTypeKind.String => new PrimitiveType((string)value!),
            PrimitiveTypeKind.Integer => new PrimitiveType(Convert.ToInt64(value!)),
            PrimitiveTypeKind.Double => new PrimitiveType(Convert.ToDouble(value!)),
            PrimitiveTypeKind.ByteArray => new PrimitiveType(Convert.FromBase64String((string)value!)),
            _ => throw new JsonSerializationException($"Unsupported kind {kind}")
        };
    }
}
