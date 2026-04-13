// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Utilities.Common;

/*
using System.Runtime.CompilerServices;
[assembly: InternalsVisibleTo("CrossCloudKit.Database.AWS")]
[assembly: InternalsVisibleTo("CrossCloudKit.Database.Basic")]
[assembly: InternalsVisibleTo("CrossCloudKit.Database.GC")]
[assembly: InternalsVisibleTo("CrossCloudKit.Database.Mongo")]*/
namespace CrossCloudKit.Interfaces.Classes;

/// <summary>
/// Represents a primary key for database operations, consisting of a key name and a <see cref="Primitive"/> value.
/// </summary>
/// <remarks>
/// <para>
/// Every <see cref="IDatabaseService"/> CRUD method requires a <c>DbKey</c> to identify the target item.
/// The <paramref name="name"/> is the attribute name (e.g. "id", "userId") and the <paramref name="value"/>
/// is the key value wrapped in a <see cref="Primitive"/>. The key name must not be null, whitespace,
/// or have leading/trailing spaces.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// // String key
/// var key = new DbKey("id", new Primitive("user-123"));
///
/// // Integer key
/// var numKey = new DbKey("userId", new Primitive(42L));
///
/// // Use with database operations
/// var result = await dbService.GetItemAsync("Users", key);
/// </code>
/// </example>
public sealed class DbKey(string name, Primitive value)
{
    /// <summary>The attribute name of the primary key (e.g. "id", "userId").</summary>
    public string Name { get; } =
        string.IsNullOrWhiteSpace(name) || name.Trim().Length != name.Length
            ? throw new InvalidOperationException("Key name cannot be null or whitespace. Also cannot start and end with spaces.")
            : name;
    /// <summary>The primary key value, wrapped in a <see cref="Primitive"/>.</summary>
    public Primitive Value { get; } = value;
}
