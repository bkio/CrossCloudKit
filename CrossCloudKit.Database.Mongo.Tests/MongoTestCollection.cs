// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Xunit;

namespace CrossCloudKit.Database.Mongo.Tests;

/// <summary>
/// Every <c>DatabaseServiceTestBase</c> test rewrites the process-wide <c>DatabaseServiceBase.SystemTableNamePostfix</c>
/// to get an isolated system table. A test that runs in parallel and reads or writes a table's key names (backup and
/// restore) then looks in a different system table than the one it registered the keys in. xUnit runs test classes in
/// parallel and the methods of one class sequentially, so every Mongo test class that touches the database service
/// shares this collection and they never overlap.
/// </summary>
[CollectionDefinition(Name)]
public sealed class MongoTestCollection
{
    public const string Name = "MongoDatabaseService";
}
