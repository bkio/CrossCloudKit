// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces;
using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Interfaces.Enums;
using CrossCloudKit.Interfaces.Records;
using CrossCloudKit.Utilities.Common;
using CrossCloudKit.Vector.Basic;
using CrossCloudKit.Vector.Tests.Common;
using FluentAssertions;
using Newtonsoft.Json.Linq;
using xRetry;
using Xunit;

[assembly: Xunit.CollectionBehavior(DisableTestParallelization = false, MaxParallelThreads = 8)]

namespace CrossCloudKit.Vector.Basic.Tests;

/// <summary>
/// Integration tests for <see cref="VectorServiceBasic"/> (cross-process file-based store).
/// No external services required — all tests run without any configuration.
/// </summary>
public class VectorServiceBasicIntegrationTests : VectorServiceTestBase
{
    protected override IVectorService CreateVectorService() => new VectorServiceBasic();

    private static string UniqueCollection(
        [System.Runtime.CompilerServices.CallerMemberName] string testName = "")
        => $"test-{testName.ToLowerInvariant()}-{Guid.NewGuid():N}"[..48];

    private static float[] RandomVector(int dimensions = 4)
    {
        var rng = new Random();
        var v = Enumerable.Range(0, dimensions).Select(_ => (float)rng.NextDouble()).ToArray();
        float mag = MathF.Sqrt(v.Sum(x => x * x));
        return mag == 0 ? v : v.Select(x => x / mag).ToArray();
    }

    // ══════════════════════════════════════════════════════════════════════════
    // PERSISTENCE — data survives across service instances
    // ══════════════════════════════════════════════════════════════════════════

    [RetryFact(3, 5000)]
    public async Task Persistence_DataSurvivesBetweenServiceInstances()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"vb-persist-{Guid.NewGuid():N}");
        string col = UniqueCollection();
        var vector = new float[] { 0.5f, 0.5f, 0.5f, 0.5f };
        var id = "persist-test-1";

        try
        {
            // Instance 1: write data
            {
                await using var svc = new VectorServiceBasic(basePath);
                await svc.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
                await svc.UpsertAsync(col, new VectorPoint
                {
                    Id = id,
                    Vector = vector,
                    Metadata = new JObject { ["title"] = "persistent doc" }
                });
            }

            // Instance 2: read data without writing anything
            {
                await using var svc = new VectorServiceBasic(basePath);
                var get = await svc.GetAsync(col, id);
                get.IsSuccessful.Should().BeTrue();
                get.Data.Should().NotBeNull();
                get.Data!.Id.Should().Be(id);
                get.Data.Vector.Should().BeEquivalentTo(vector, opts => opts.WithStrictOrdering());
                get.Data.Metadata?["title"]?.Value<string>().Should().Be("persistent doc");
            }
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, true);
        }
    }

    [RetryFact(3, 5000)]
    public async Task Persistence_CollectionSurvivesBetweenInstances()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"vb-persist-col-{Guid.NewGuid():N}");
        string col = UniqueCollection();

        try
        {
            // Instance 1: create collection
            {
                await using var svc = new VectorServiceBasic(basePath);
                await svc.EnsureCollectionExistsAsync(col, 8, VectorDistanceMetric.Euclidean);
            }

            // Instance 2: list collections
            {
                await using var svc = new VectorServiceBasic(basePath);
                var names = await svc.GetCollectionNamesAsync();
                names.IsSuccessful.Should().BeTrue();
                names.Data.Should().Contain(col);

                // Also verify that EnsureCollectionExistsAsync returns false (already exists)
                var second = await svc.EnsureCollectionExistsAsync(col, 8, VectorDistanceMetric.Euclidean);
                second.IsSuccessful.Should().BeTrue();
                second.Data.Should().BeFalse();
            }
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, true);
        }
    }

    [RetryFact(3, 5000)]
    public async Task Persistence_DeletedPointStaysDeletedAfterReopen()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"vb-persist-del-{Guid.NewGuid():N}");
        string col = UniqueCollection();
        var id = "del-persist-1";

        try
        {
            // Instance 1: insert then delete
            {
                await using var svc = new VectorServiceBasic(basePath);
                await svc.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
                await svc.UpsertAsync(col, new VectorPoint { Id = id, Vector = RandomVector() });
                await svc.DeleteAsync(col, id);
            }

            // Instance 2: verify it's gone
            {
                await using var svc = new VectorServiceBasic(basePath);
                var get = await svc.GetAsync(col, id);
                get.IsSuccessful.Should().BeTrue();
                get.Data.Should().BeNull();
            }
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, true);
        }
    }

    [RetryFact(3, 5000)]
    public async Task Persistence_DeletedCollectionStaysDeletedAfterReopen()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"vb-persist-delcol-{Guid.NewGuid():N}");
        string col = UniqueCollection();

        try
        {
            {
                await using var svc = new VectorServiceBasic(basePath);
                await svc.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
                await svc.UpsertAsync(col, new VectorPoint { Id = "x", Vector = RandomVector() });
                await svc.DeleteCollectionAsync(col);
            }

            {
                await using var svc = new VectorServiceBasic(basePath);
                var names = await svc.GetCollectionNamesAsync();
                names.Data.Should().NotContain(col);
            }
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, true);
        }
    }

    [RetryFact(3, 5000)]
    public async Task Persistence_MultiplePointsSurviveReopen()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"vb-persist-multi-{Guid.NewGuid():N}");
        string col = UniqueCollection();

        try
        {
            {
                await using var svc = new VectorServiceBasic(basePath);
                await svc.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
                var points = Enumerable.Range(0, 20).Select(i => new VectorPoint
                {
                    Id = $"pt-{i}",
                    Vector = RandomVector(),
                    Metadata = new JObject { ["idx"] = i }
                }).ToList();
                await svc.UpsertBatchAsync(col, points);
            }

            {
                await using var svc = new VectorServiceBasic(basePath);
                // Query should return all 20 points
                var result = await svc.QueryAsync(col, RandomVector(), topK: 100);
                result.IsSuccessful.Should().BeTrue();
                result.Data.Should().HaveCount(20);
            }
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, true);
        }
    }

    [RetryFact(3, 5000)]
    public async Task Persistence_QueryWithFilterWorksAfterReopen()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"vb-persist-qf-{Guid.NewGuid():N}");
        string col = UniqueCollection();
        var v = new float[] { 1f, 0f, 0f, 0f };

        try
        {
            {
                await using var svc = new VectorServiceBasic(basePath);
                await svc.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
                await svc.UpsertBatchAsync(col, [
                    new VectorPoint { Id = "a", Vector = v, Metadata = new JObject { ["type"] = "cat" } },
                    new VectorPoint { Id = "b", Vector = v, Metadata = new JObject { ["type"] = "dog" } },
                    new VectorPoint { Id = "c", Vector = v, Metadata = new JObject { ["type"] = "cat" } }
                ]);
            }

            {
                await using var svc = new VectorServiceBasic(basePath);
                var filter = svc.FieldEquals("type", new Primitive("cat"));
                var result = await svc.QueryAsync(col, v, topK: 10, filter: filter);
                result.IsSuccessful.Should().BeTrue();
                result.Data.Should().HaveCount(2);
                result.Data.Select(r => r.Id).Should().BeEquivalentTo(["a", "c"]);
            }
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, true);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // CONCURRENCY — multi-instance tests (file-based specific)
    // ══════════════════════════════════════════════════════════════════════════

    [RetryFact(3, 10000)]
    public async Task Concurrency_TwoServiceInstances_SameBasePath_ShouldSeeEachOthersData()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"vb-two-inst-{Guid.NewGuid():N}");
        string col = UniqueCollection();
        var v = new float[] { 1f, 0f, 0f, 0f };

        try
        {
            await using var svc1 = new VectorServiceBasic(basePath);
            await using var svc2 = new VectorServiceBasic(basePath);

            await svc1.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            // svc1 writes, svc2 reads
            await svc1.UpsertAsync(col, new VectorPoint
            {
                Id = "from-svc1",
                Vector = v,
                Metadata = new JObject { ["source"] = "svc1" }
            });

            var get = await svc2.GetAsync(col, "from-svc1");
            get.IsSuccessful.Should().BeTrue();
            get.Data.Should().NotBeNull();
            get.Data!.Metadata?["source"]?.Value<string>().Should().Be("svc1");

            // svc2 writes, svc1 reads
            await svc2.UpsertAsync(col, new VectorPoint
            {
                Id = "from-svc2",
                Vector = v,
                Metadata = new JObject { ["source"] = "svc2" }
            });

            var get2 = await svc1.GetAsync(col, "from-svc2");
            get2.IsSuccessful.Should().BeTrue();
            get2.Data.Should().NotBeNull();
            get2.Data!.Metadata?["source"]?.Value<string>().Should().Be("svc2");
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, true);
        }
    }

    [RetryFact(3, 15000)]
    public async Task Concurrency_ParallelWritesFromTwoInstances_ShouldNotLoseData()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"vb-par-inst-{Guid.NewGuid():N}");
        string col = UniqueCollection();

        try
        {
            await using var svc1 = new VectorServiceBasic(basePath);
            await using var svc2 = new VectorServiceBasic(basePath);

            await svc1.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            // Both instances write 25 points each with unique IDs
            var t1 = Enumerable.Range(0, 25).Select(i => Task.Run(async () =>
            {
                await svc1.UpsertAsync(col, new VectorPoint
                {
                    Id = $"svc1-{i}",
                    Vector = RandomVector(),
                    Metadata = new JObject { ["inst"] = 1 }
                });
            }));

            var t2 = Enumerable.Range(0, 25).Select(i => Task.Run(async () =>
            {
                await svc2.UpsertAsync(col, new VectorPoint
                {
                    Id = $"svc2-{i}",
                    Vector = RandomVector(),
                    Metadata = new JObject { ["inst"] = 2 }
                });
            }));

            await Task.WhenAll(t1.Concat(t2));

            // All 50 points should exist
            await using var verifier = new VectorServiceBasic(basePath);
            var result = await verifier.QueryAsync(col, RandomVector(), topK: 100);
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(50);
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, true);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // SPECIAL CHARACTERS — IDs and collection names with unusual characters
    // ══════════════════════════════════════════════════════════════════════════

    [RetryFact(3, 5000)]
    public async Task SpecialChars_PointIdWithSlashes_ShouldRoundTrip()
    {
        await using var service = new VectorServiceBasic();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var id = "folder/subfolder/doc.pdf";
            var vec = RandomVector();

            await service.UpsertAsync(col, new VectorPoint
            {
                Id = id, Vector = vec, Metadata = new JObject { ["path"] = id }
            });

            var get = await service.GetAsync(col, id);
            get.IsSuccessful.Should().BeTrue();
            get.Data.Should().NotBeNull();
            get.Data!.Id.Should().Be(id);
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    [RetryFact(3, 5000)]
    public async Task SpecialChars_PointIdWithSpacesAndUnicode_ShouldRoundTrip()
    {
        await using var service = new VectorServiceBasic();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var id = "my document 日本語 über résumé";
            var vec = RandomVector();

            await service.UpsertAsync(col, new VectorPoint
            {
                Id = id, Vector = vec, Metadata = new JObject { ["title"] = "unicode test" }
            });

            var get = await service.GetAsync(col, id);
            get.IsSuccessful.Should().BeTrue();
            get.Data.Should().NotBeNull();
            get.Data!.Id.Should().Be(id);

            // Also test query returns correct IDs
            var query = await service.QueryAsync(col, vec, topK: 1);
            query.Data[0].Id.Should().Be(id);
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    [RetryFact(3, 5000)]
    public async Task SpecialChars_PointIdWithDots_ShouldRoundTrip()
    {
        await using var service = new VectorServiceBasic();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var id = "com.example.app.v2.3.1";

            await service.UpsertAsync(col, new VectorPoint { Id = id, Vector = RandomVector() });
            var get = await service.GetAsync(col, id);
            get.Data.Should().NotBeNull();
            get.Data!.Id.Should().Be(id);
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    [RetryFact(3, 5000)]
    public async Task SpecialChars_PointIdWithColonsAndEquals_ShouldRoundTrip()
    {
        await using var service = new VectorServiceBasic();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var id = "key=value:scope/ns+base64==";

            await service.UpsertAsync(col, new VectorPoint { Id = id, Vector = RandomVector() });
            var get = await service.GetAsync(col, id);
            get.Data.Should().NotBeNull();
            get.Data!.Id.Should().Be(id);
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    [RetryFact(3, 5000)]
    public async Task SpecialChars_EmptyStringId_ShouldRoundTrip()
    {
        await using var service = new VectorServiceBasic();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            await service.UpsertAsync(col, new VectorPoint { Id = "", Vector = RandomVector() });
            var get = await service.GetAsync(col, "");
            get.Data.Should().NotBeNull();
            get.Data!.Id.Should().Be("");
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    [RetryFact(3, 5000)]
    public async Task SpecialChars_VeryLongId_ShouldRoundTrip()
    {
        await using var service = new VectorServiceBasic();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var id = new string('x', 150);

            await service.UpsertAsync(col, new VectorPoint { Id = id, Vector = RandomVector() });
            var get = await service.GetAsync(col, id);
            get.Data.Should().NotBeNull();
            get.Data!.Id.Should().Be(id);
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // QUERY — persistence-specific complex filter test
    // ══════════════════════════════════════════════════════════════════════════

    [RetryFact(3, 5000)]
    public async Task Query_ComplexAndOrFilter_WithPersistence()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"vb-complex-{Guid.NewGuid():N}");
        string col = UniqueCollection();
        var v = new float[] { 1f, 0f, 0f, 0f };

        try
        {
            // Write data
            {
                await using var svc = new VectorServiceBasic(basePath);
                await svc.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
                await svc.UpsertBatchAsync(col, [
                    new VectorPoint { Id = "admin-active", Vector = v, Metadata = new JObject { ["role"] = "admin", ["active"] = true, ["score"] = 90 } },
                    new VectorPoint { Id = "admin-inactive", Vector = v, Metadata = new JObject { ["role"] = "admin", ["active"] = false, ["score"] = 80 } },
                    new VectorPoint { Id = "user-active-high", Vector = v, Metadata = new JObject { ["role"] = "user", ["active"] = true, ["score"] = 85 } },
                    new VectorPoint { Id = "user-active-low", Vector = v, Metadata = new JObject { ["role"] = "user", ["active"] = true, ["score"] = 30 } },
                    new VectorPoint { Id = "mod-active", Vector = v, Metadata = new JObject { ["role"] = "moderator", ["active"] = true, ["score"] = 70 } }
                ]);
            }

            // Query from new instance: (role=admin OR (active=true AND score>50))
            {
                await using var svc = new VectorServiceBasic(basePath);
                var activeHighScore = svc.FieldEquals("active", new Primitive(true))
                    .And(svc.FieldGreaterThan("score", new Primitive(50L)));
                var filter = svc.FieldEquals("role", new Primitive("admin"))
                    .Or(activeHighScore);

                var result = await svc.QueryAsync(col, v, topK: 10, filter: filter);
                result.IsSuccessful.Should().BeTrue();
                result.Data.Select(r => r.Id).Should().BeEquivalentTo(
                    ["admin-active", "admin-inactive", "user-active-high", "mod-active"]);
            }
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, true);
        }
    }
}
