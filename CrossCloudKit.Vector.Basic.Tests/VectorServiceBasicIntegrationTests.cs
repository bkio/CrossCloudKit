// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces;
using CrossCloudKit.Vector.Basic;
using CrossCloudKit.Vector.Tests.Common;

[assembly: Xunit.CollectionBehavior(DisableTestParallelization = false, MaxParallelThreads = 8)]

namespace CrossCloudKit.Vector.Basic.Tests;

/// <summary>
/// Integration tests for <see cref="VectorServiceBasic"/> (in-memory store).
/// No external services required — all tests run without any configuration.
/// </summary>
public class VectorServiceBasicIntegrationTests : VectorServiceTestBase
{
    protected override IVectorService CreateVectorService() => new VectorServiceBasic();
}
