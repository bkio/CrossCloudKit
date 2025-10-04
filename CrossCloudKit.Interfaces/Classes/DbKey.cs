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

public sealed class DbKey(string name, Primitive value)
{
    public string Name { get; } =
        string.IsNullOrWhiteSpace(name) || name.Trim().Length != name.Length
            ? throw new InvalidOperationException("Key name cannot be null or whitespace. Also cannot start and end with spaces.")
            : name;
    public Primitive Value { get; } = value;
}
