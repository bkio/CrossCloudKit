// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces;
using CrossCloudKit.Vector.Qdrant;
using CrossCloudKit.Vector.Tests.Common;

[assembly: Xunit.CollectionBehavior(DisableTestParallelization = false, MaxParallelThreads = 4)]

namespace CrossCloudKit.Vector.Qdrant.Tests;

/// <summary>
/// Integration tests for <see cref="VectorServiceQdrant"/>.
/// Set <c>QDRANT_HOST</c> and <c>QDRANT_PORT</c> (gRPC, default 6334) via
/// environment variables or <c>test.runsettings</c>.
/// Optionally set <c>QDRANT_API_KEY</c> for authenticated Qdrant Cloud clusters.
/// </summary>
public class VectorServiceQdrantIntegrationTests : VectorServiceTestBase
{
    private static string Host =>
        Environment.GetEnvironmentVariable("QDRANT_HOST") ?? "localhost";

    private static int Port =>
        int.TryParse(Environment.GetEnvironmentVariable("QDRANT_PORT"), out var p) ? p : 6334;

    private static string? ApiKey =>
        string.IsNullOrEmpty(Environment.GetEnvironmentVariable("QDRANT_API_KEY"))
            ? null
            : Environment.GetEnvironmentVariable("QDRANT_API_KEY");

    protected override IVectorService CreateVectorService()
        => new VectorServiceQdrant(Host, Port, https: false, apiKey: ApiKey);
}
