// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces;
using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Interfaces.Enums;
using CrossCloudKit.Interfaces.Records;
using CrossCloudKit.Utilities.Common;
using FluentAssertions;
using Newtonsoft.Json.Linq;
using xRetry;
using Xunit;

namespace CrossCloudKit.Vector.Tests.Common;

/// <summary>
/// Abstract base class for <see cref="IVectorService"/> integration tests.
/// </summary>
public abstract class VectorServiceTestBase
{
    /// <summary>Creates the service under test.</summary>
    protected abstract IVectorService CreateVectorService();

    private static string UniqueCollection(
        [System.Runtime.CompilerServices.CallerMemberName] string testName = "")
        => $"test-{testName.ToLowerInvariant()}-{Guid.NewGuid():N}"[..48];

    private static float[] RandomVector(int dimensions = 4)
    {
        var rng = new Random();
        var v = Enumerable.Range(0, dimensions).Select(_ => (float)rng.NextDouble()).ToArray();
        // Normalise
        float mag = MathF.Sqrt(v.Sum(x => x * x));
        return mag == 0 ? v : v.Select(x => x / mag).ToArray();
    }

    // ── Collection management ─────────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task EnsureCollectionExistsAsync_WhenAbsent_ShouldCreateCollection()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            var result = await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            result.IsSuccessful.Should().BeTrue(because: result.IsSuccessful ? "" : result.ErrorMessage);
            result.Data.Should().BeTrue(because: "collection should be newly created");
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    [RetryFact(3, 5000)]
    public async Task EnsureCollectionExistsAsync_WhenAlreadyExists_ShouldReturnFalse()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var second = await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            second.IsSuccessful.Should().BeTrue();
            second.Data.Should().BeFalse(because: "collection already existed");
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    [RetryFact(3, 5000)]
    public async Task GetCollectionNamesAsync_ShouldIncludeCreatedCollection()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var names = await service.GetCollectionNamesAsync();
            names.IsSuccessful.Should().BeTrue();
            names.Data.Should().Contain(col);
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    [RetryFact(3, 5000)]
    public async Task DeleteCollectionAsync_ShouldRemoveCollection()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
        var del = await service.DeleteCollectionAsync(col);
        del.IsSuccessful.Should().BeTrue();

        var names = await service.GetCollectionNamesAsync();
        names.Data.Should().NotContain(col);
    }

    // ── Upsert / Get ──────────────────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task UpsertAsync_AndGetAsync_ShouldRoundTripPoint()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            var vector = RandomVector(4);
            var point = new VectorPoint
            {
                Id       = Guid.NewGuid().ToString(),
                Vector   = vector,
                Metadata = new JObject { ["name"] = "test", ["score"] = 42 }
            };

            var upsert = await service.UpsertAsync(col, point);
            upsert.IsSuccessful.Should().BeTrue();

            var get = await service.GetAsync(col, point.Id);
            get.IsSuccessful.Should().BeTrue();
            get.Data.Should().NotBeNull();
            get.Data!.Id.Should().Be(point.Id);
            get.Data.Vector.Should().BeEquivalentTo(vector, opts => opts.WithStrictOrdering());
            get.Data.Metadata?["name"]?.Value<string>().Should().Be("test");
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    [RetryFact(3, 5000)]
    public async Task UpsertBatchAsync_ShouldStoreAllPoints()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            var points = Enumerable.Range(0, 5).Select(i => new VectorPoint
            {
                Id       = $"batch-{i}",
                Vector   = RandomVector(4),
                Metadata = new JObject { ["idx"] = i }
            }).ToList();

            var result = await service.UpsertBatchAsync(col, points);
            result.IsSuccessful.Should().BeTrue();

            foreach (var p in points)
            {
                var get = await service.GetAsync(col, p.Id, includeVector: false, includeMetadata: true);
                get.IsSuccessful.Should().BeTrue();
                get.Data.Should().NotBeNull();
            }
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    [RetryFact(3, 5000)]
    public async Task GetAsync_WithNonExistentId_ShouldReturnNull()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var result = await service.GetAsync(col, Guid.NewGuid().ToString());
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeNull();
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    // ── Delete point ──────────────────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task DeleteAsync_ShouldRemovePoint()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            var id = Guid.NewGuid().ToString();
            await service.UpsertAsync(col, new VectorPoint { Id = id, Vector = RandomVector(4) });

            var del = await service.DeleteAsync(col, id);
            del.IsSuccessful.Should().BeTrue();

            var get = await service.GetAsync(col, id);
            get.IsSuccessful.Should().BeTrue();
            get.Data.Should().BeNull();
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    // ── Query / Search ────────────────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task QueryAsync_ShouldReturnNearestNeighbours()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            // Insert two clusters
            var closeVector = new float[] { 0.9f, 0.1f, 0.0f, 0.0f };
            var farVector   = new float[] { 0.0f, 0.0f, 0.9f, 0.1f };

            var close = new VectorPoint { Id = "close", Vector = closeVector, Metadata = new JObject { ["cluster"] = "A" } };
            var far   = new VectorPoint { Id = "far",   Vector = farVector,   Metadata = new JObject { ["cluster"] = "B" } };

            await service.UpsertBatchAsync(col, [close, far]);

            var query  = new float[] { 0.9f, 0.1f, 0.0f, 0.0f }; // aligned with "close"
            var result = await service.QueryAsync(col, query, topK: 2);

            result.IsSuccessful.Should().BeTrue(because: result.IsSuccessful ? "" : result.ErrorMessage);
            result.Data.Should().HaveCount(2);
            result.Data[0].Id.Should().Be("close",
                because: "the nearest neighbour should be the point with the most similar vector");
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    [RetryFact(3, 5000)]
    public async Task QueryAsync_WithFilter_ShouldReturnOnlyMatchingPoints()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            var baseVec = new float[] { 1f, 0f, 0f, 0f };
            var points = new[]
            {
                new VectorPoint { Id = "cat1", Vector = baseVec, Metadata = new JObject { ["type"] = "cat" } },
                new VectorPoint { Id = "dog1", Vector = baseVec, Metadata = new JObject { ["type"] = "dog" } },
                new VectorPoint { Id = "cat2", Vector = baseVec, Metadata = new JObject { ["type"] = "cat" } }
            };
            await service.UpsertBatchAsync(col, points);

            var filter = service.FieldEquals("type", new Primitive("cat"));
            var result = await service.QueryAsync(col, baseVec, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(2);
            result.Data.Should().OnlyContain(r => r.Id.StartsWith("cat"),
                because: "filter should exclude non-cat points");
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    [RetryFact(3, 5000)]
    public async Task QueryAsync_TopK_ShouldLimitResults()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            var points = Enumerable.Range(0, 10).Select(i => new VectorPoint
            {
                Id     = $"p{i}",
                Vector = RandomVector(4)
            }).ToList();
            await service.UpsertBatchAsync(col, points);

            var result = await service.QueryAsync(col, RandomVector(4), topK: 3);
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(3, because: "topK = 3 should limit results to 3");
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    // ── Upsert update ─────────────────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task UpsertAsync_UpdateExistingPoint_ShouldOverwriteMetadata()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            var id  = Guid.NewGuid().ToString();
            var vec = RandomVector(4);

            await service.UpsertAsync(col, new VectorPoint
            {
                Id       = id,
                Vector   = vec,
                Metadata = new JObject { ["status"] = "draft" }
            });

            // Overwrite with new metadata
            await service.UpsertAsync(col, new VectorPoint
            {
                Id       = id,
                Vector   = vec,
                Metadata = new JObject { ["status"] = "published" }
            });

            var get = await service.GetAsync(col, id, includeVector: false, includeMetadata: true);
            get.IsSuccessful.Should().BeTrue();
            get.Data.Should().NotBeNull();
            get.Data!.Metadata?["status"]?.Value<string>().Should().Be("published",
                because: "upsert on an existing ID should overwrite the point");
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    // ── Scalar filter conditions ───────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task Filter_FieldEquals_Integer_ShouldMatchExactValue()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "a", Vector = v, Metadata = new JObject { ["level"] = 1 } },
                new VectorPoint { Id = "b", Vector = v, Metadata = new JObject { ["level"] = 2 } },
                new VectorPoint { Id = "c", Vector = v, Metadata = new JObject { ["level"] = 1 } }
            ]);

            var filter = service.FieldEquals("level", new Primitive(1));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(2);
            result.Data.Select(r => r.Id).Should().BeEquivalentTo(["a", "c"]);
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    [RetryFact(3, 5000)]
    public async Task Filter_FieldNotEquals_ShouldExcludeMatchingPoints()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "x", Vector = v, Metadata = new JObject { ["role"] = "admin" } },
                new VectorPoint { Id = "y", Vector = v, Metadata = new JObject { ["role"] = "user"  } },
                new VectorPoint { Id = "z", Vector = v, Metadata = new JObject { ["role"] = "user"  } }
            ]);

            var filter = service.FieldNotEquals("role", new Primitive("admin"));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(2);
            result.Data.Should().OnlyContain(r => r.Id != "x");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    [RetryFact(3, 5000)]
    public async Task Filter_FieldGreaterThan_ShouldReturnOnlyPointsAboveThreshold()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "low",  Vector = v, Metadata = new JObject { ["score"] = 10 } },
                new VectorPoint { Id = "mid",  Vector = v, Metadata = new JObject { ["score"] = 50 } },
                new VectorPoint { Id = "high", Vector = v, Metadata = new JObject { ["score"] = 90 } }
            ]);

            var filter = service.FieldGreaterThan("score", new Primitive(49));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(2);
            result.Data.Select(r => r.Id).Should().BeEquivalentTo(["mid", "high"]);
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    [RetryFact(3, 5000)]
    public async Task Filter_FieldLessThan_ShouldReturnOnlyPointsBelowThreshold()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "low",  Vector = v, Metadata = new JObject { ["score"] = 10 } },
                new VectorPoint { Id = "mid",  Vector = v, Metadata = new JObject { ["score"] = 50 } },
                new VectorPoint { Id = "high", Vector = v, Metadata = new JObject { ["score"] = 90 } }
            ]);

            var filter = service.FieldLessThan("score", new Primitive(51));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(2);
            result.Data.Select(r => r.Id).Should().BeEquivalentTo(["low", "mid"]);
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    [RetryFact(3, 5000)]
    public async Task Filter_FieldGreaterThanOrEqual_ShouldIncludeBoundaryValue()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "a", Vector = v, Metadata = new JObject { ["score"] = 10 } },
                new VectorPoint { Id = "b", Vector = v, Metadata = new JObject { ["score"] = 50 } },
                new VectorPoint { Id = "c", Vector = v, Metadata = new JObject { ["score"] = 90 } }
            ]);

            var filter = service.FieldGreaterThanOrEqual("score", new Primitive(50));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Select(r => r.Id).Should().BeEquivalentTo(["b", "c"],
                because: "boundary value 50 must be included with >=");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    [RetryFact(3, 5000)]
    public async Task Filter_FieldLessThanOrEqual_ShouldIncludeBoundaryValue()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "a", Vector = v, Metadata = new JObject { ["score"] = 10 } },
                new VectorPoint { Id = "b", Vector = v, Metadata = new JObject { ["score"] = 50 } },
                new VectorPoint { Id = "c", Vector = v, Metadata = new JObject { ["score"] = 90 } }
            ]);

            var filter = service.FieldLessThanOrEqual("score", new Primitive(50));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Select(r => r.Id).Should().BeEquivalentTo(["a", "b"],
                because: "boundary value 50 must be included with <=");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    [RetryFact(3, 5000)]
    public async Task Filter_FieldExists_ShouldReturnOnlyPointsWithField()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "has-tag",  Vector = v, Metadata = new JObject { ["tag"] = "x" } },
                new VectorPoint { Id = "no-tag",   Vector = v, Metadata = new JObject { ["other"] = 1 } },
                new VectorPoint { Id = "has-tag2", Vector = v, Metadata = new JObject { ["tag"] = "y" } }
            ]);

            var filter = service.FieldExists("tag");
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(2);
            result.Data.Select(r => r.Id).Should().Contain("has-tag").And.Contain("has-tag2");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    [RetryFact(3, 5000)]
    public async Task Filter_FieldNotExists_ShouldReturnOnlyPointsWithoutField()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "with",    Vector = v, Metadata = new JObject { ["secret"] = "val" } },
                new VectorPoint { Id = "without1", Vector = v, Metadata = new JObject { ["other"] = 1 } },
                new VectorPoint { Id = "without2", Vector = v, Metadata = new JObject { ["x"] = "y" } }
            ]);

            var filter = service.FieldNotExists("secret");
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(2);
            result.Data.Select(r => r.Id).Should().NotContain("with");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── AND coupling ──────────────────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task Coupling_And_BothConditionsMustMatch()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "match",   Vector = v, Metadata = new JObject { ["kind"] = "cat", ["color"] = "black" } },
                new VectorPoint { Id = "knd-only", Vector = v, Metadata = new JObject { ["kind"] = "cat", ["color"] = "white" } },
                new VectorPoint { Id = "col-only", Vector = v, Metadata = new JObject { ["kind"] = "dog", ["color"] = "black" } },
                new VectorPoint { Id = "neither",  Vector = v, Metadata = new JObject { ["kind"] = "dog", ["color"] = "white" } }
            ]);

            var filter = service.FieldEquals("kind",  new Primitive("cat"))
                            .And(service.FieldEquals("color", new Primitive("black")));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(1, because: "AND requires BOTH conditions");
            result.Data[0].Id.Should().Be("match");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    [RetryFact(3, 5000)]
    public async Task Coupling_And_ThreeConditions_AllMustMatch()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "all3",  Vector = v, Metadata = new JObject { ["a"] = "x", ["b"] = "y", ["c"] = "z" } },
                new VectorPoint { Id = "miss-c", Vector = v, Metadata = new JObject { ["a"] = "x", ["b"] = "y", ["c"] = "w" } },
                new VectorPoint { Id = "miss-b", Vector = v, Metadata = new JObject { ["a"] = "x", ["b"] = "n", ["c"] = "z" } },
                new VectorPoint { Id = "miss-a", Vector = v, Metadata = new JObject { ["a"] = "q", ["b"] = "y", ["c"] = "z" } }
            ]);

            var filter = service.FieldEquals("a", new Primitive("x"))
                            .And(service.FieldEquals("b", new Primitive("y")))
                            .And(service.FieldEquals("c", new Primitive("z")));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(1);
            result.Data[0].Id.Should().Be("all3");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── OR coupling ───────────────────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task Coupling_Or_EitherConditionMayMatch()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "cat",   Vector = v, Metadata = new JObject { ["type"] = "cat" } },
                new VectorPoint { Id = "dog",   Vector = v, Metadata = new JObject { ["type"] = "dog" } },
                new VectorPoint { Id = "bird",  Vector = v, Metadata = new JObject { ["type"] = "bird" } }
            ]);

            var filter = service.FieldEquals("type", new Primitive("cat"))
                            .Or(service.FieldEquals("type", new Primitive("dog")));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(2);
            result.Data.Select(r => r.Id).Should().BeEquivalentTo(["cat", "dog"]);
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    [RetryFact(3, 5000)]
    public async Task Coupling_Or_ThreeAlternatives_AnyMayMatch()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "r", Vector = v, Metadata = new JObject { ["col"] = "red" } },
                new VectorPoint { Id = "g", Vector = v, Metadata = new JObject { ["col"] = "green" } },
                new VectorPoint { Id = "b", Vector = v, Metadata = new JObject { ["col"] = "blue" } },
                new VectorPoint { Id = "y", Vector = v, Metadata = new JObject { ["col"] = "yellow" } }
            ]);

            var filter = service.FieldEquals("col", new Primitive("red"))
                            .Or(service.FieldEquals("col", new Primitive("green")))
                            .Or(service.FieldEquals("col", new Primitive("blue")));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(3);
            result.Data.Select(r => r.Id).Should().NotContain("y");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Nested / complex coupling ─────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task Coupling_AndInOr__AandB_Or_C__ShouldEvaluateCorrectly()
    {
        // Filter: (kind=cat AND color=black) OR kind=dog
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "cat-black", Vector = v, Metadata = new JObject { ["kind"] = "cat", ["color"] = "black" } },
                new VectorPoint { Id = "cat-white", Vector = v, Metadata = new JObject { ["kind"] = "cat", ["color"] = "white" } },
                new VectorPoint { Id = "dog-any",   Vector = v, Metadata = new JObject { ["kind"] = "dog", ["color"] = "brown" } },
                new VectorPoint { Id = "bird",      Vector = v, Metadata = new JObject { ["kind"] = "bird", ["color"] = "red" } }
            ]);

            // (kind=cat AND color=black) OR kind=dog
            var andPart = service.FieldEquals("kind",  new Primitive("cat"))
                             .And(service.FieldEquals("color", new Primitive("black")));
            var filter = andPart.Or(service.FieldEquals("kind", new Primitive("dog")));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(2, because: "only cat-black and dog-any should match");
            result.Data.Select(r => r.Id).Should().BeEquivalentTo(["cat-black", "dog-any"]);
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    [RetryFact(3, 5000)]
    public async Task Coupling_OrInAnd__A_And_BorC__ShouldEvaluateCorrectly()
    {
        // Filter: status=active AND (role=admin OR role=moderator)
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "active-admin", Vector = v, Metadata = new JObject { ["status"] = "active", ["role"] = "admin" } },
                new VectorPoint { Id = "active-mod",   Vector = v, Metadata = new JObject { ["status"] = "active", ["role"] = "moderator" } },
                new VectorPoint { Id = "active-user",  Vector = v, Metadata = new JObject { ["status"] = "active", ["role"] = "user" } },
                new VectorPoint { Id = "banned-admin", Vector = v, Metadata = new JObject { ["status"] = "banned", ["role"] = "admin" } }
            ]);

            // status=active AND (role=admin OR role=moderator)
            var orPart = service.FieldEquals("role", new Primitive("admin"))
                            .Or(service.FieldEquals("role", new Primitive("moderator")));
            var filter = service.FieldEquals("status", new Primitive("active")).And(orPart);
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(2);
            result.Data.Select(r => r.Id).Should().BeEquivalentTo(["active-admin", "active-mod"]);
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    [RetryFact(3, 5000)]
    public async Task Coupling_ThreeLevelNested___AandB_Or_C__And_D()
    {
        // ((kind=cat AND color=black) OR kind=dog) AND score>5
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "cb-hi",  Vector = v, Metadata = new JObject { ["kind"] = "cat", ["color"] = "black", ["score"] = 10 } },
                new VectorPoint { Id = "cb-lo",  Vector = v, Metadata = new JObject { ["kind"] = "cat", ["color"] = "black", ["score"] = 2  } },
                new VectorPoint { Id = "cw-hi",  Vector = v, Metadata = new JObject { ["kind"] = "cat", ["color"] = "white", ["score"] = 10 } },
                new VectorPoint { Id = "dog-hi", Vector = v, Metadata = new JObject { ["kind"] = "dog", ["color"] = "tan",   ["score"] = 8  } },
                new VectorPoint { Id = "dog-lo", Vector = v, Metadata = new JObject { ["kind"] = "dog", ["color"] = "tan",   ["score"] = 1  } }
            ]);

            // ((cat AND black) OR dog) AND score>5
            var andPart    = service.FieldEquals("kind",  new Primitive("cat"))
                                .And(service.FieldEquals("color", new Primitive("black")));
            var orPart     = andPart.Or(service.FieldEquals("kind", new Primitive("dog")));
            var filter     = orPart.And(service.FieldGreaterThan("score", new Primitive(5)));

            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(2, because: "cb-hi and dog-hi are the only matches for all three levels");
            result.Data.Select(r => r.Id).Should().BeEquivalentTo(["cb-hi", "dog-hi"]);
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    [RetryFact(3, 5000)]
    public async Task Coupling_And_ExistenceWithEquality()
    {
        // FieldExists("tag") AND FieldEquals("status", "ok")
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "tag-ok",     Vector = v, Metadata = new JObject { ["tag"] = "x", ["status"] = "ok"  } },
                new VectorPoint { Id = "notag-ok",   Vector = v, Metadata = new JObject { ["status"] = "ok"  } },
                new VectorPoint { Id = "tag-bad",    Vector = v, Metadata = new JObject { ["tag"] = "x", ["status"] = "bad" } },
                new VectorPoint { Id = "notag-bad",  Vector = v, Metadata = new JObject { ["status"] = "bad" } }
            ]);

            var filter = service.FieldExists("tag")
                            .And(service.FieldEquals("status", new Primitive("ok")));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(1);
            result.Data[0].Id.Should().Be("tag-ok");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    [RetryFact(3, 5000)]
    public async Task Coupling_Or_NumericRangeAlternatives()
    {
        // score < 10 OR score > 90  (i.e. "outliers")
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "very-low",  Vector = v, Metadata = new JObject { ["score"] = 5  } },
                new VectorPoint { Id = "mid-low",   Vector = v, Metadata = new JObject { ["score"] = 30 } },
                new VectorPoint { Id = "mid",       Vector = v, Metadata = new JObject { ["score"] = 50 } },
                new VectorPoint { Id = "mid-high",  Vector = v, Metadata = new JObject { ["score"] = 70 } },
                new VectorPoint { Id = "very-high", Vector = v, Metadata = new JObject { ["score"] = 95 } }
            ]);

            var filter = service.FieldLessThan("score",    new Primitive(10))
                            .Or(service.FieldGreaterThan("score", new Primitive(90)));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(2, because: "only scores <10 or >90 are outliers");
            result.Data.Select(r => r.Id).Should().BeEquivalentTo(["very-low", "very-high"]);
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    [RetryFact(3, 5000)]
    public async Task Coupling_And_NotExistsWithFieldEquals()
    {
        // FieldNotExists("banned") AND FieldEquals("active", "true")
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "clean-active",  Vector = v, Metadata = new JObject { ["active"] = "true" } },
                new VectorPoint { Id = "banned-active", Vector = v, Metadata = new JObject { ["active"] = "true",  ["banned"] = "yes" } },
                new VectorPoint { Id = "clean-inactive",Vector = v, Metadata = new JObject { ["active"] = "false" } }
            ]);

            var filter = service.FieldNotExists("banned")
                            .And(service.FieldEquals("active", new Primitive("true")));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(1);
            result.Data[0].Id.Should().Be("clean-active");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    [RetryFact(3, 5000)]
    public async Task Coupling_AggregateAnd_MultipleSingleConditions()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "match",  Vector = v, Metadata = new JObject { ["p"] = "1", ["q"] = "2", ["r"] = "3" } },
                new VectorPoint { Id = "miss-r", Vector = v, Metadata = new JObject { ["p"] = "1", ["q"] = "2", ["r"] = "X" } },
                new VectorPoint { Id = "miss-pq",Vector = v, Metadata = new JObject { ["p"] = "X", ["q"] = "X", ["r"] = "3" } }
            ]);

            // AggregateAnd combines conditions from an IEnumerable<Condition>
            var conditions = new[]
            {
                service.FieldEquals("p", new Primitive("1")),
                service.FieldEquals("q", new Primitive("2")),
                service.FieldEquals("r", new Primitive("3"))
            };
            var filter = conditions.AggregateAnd();
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(1);
            result.Data[0].Id.Should().Be("match");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    [RetryFact(3, 5000)]
    public async Task Coupling_Or_ManualChain_ThreeAlternatives_AllMatch()
    {
        // Equivalent to AggregateOr — but built by explicit chaining, avoiding
        // the Empty seed issue that AggregateOr has in OR contexts.
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "red",    Vector = v, Metadata = new JObject { ["color"] = "red" } },
                new VectorPoint { Id = "green",  Vector = v, Metadata = new JObject { ["color"] = "green" } },
                new VectorPoint { Id = "blue",   Vector = v, Metadata = new JObject { ["color"] = "blue" } },
                new VectorPoint { Id = "purple", Vector = v, Metadata = new JObject { ["color"] = "purple" } }
            ]);

            // Direct chain: red OR green OR blue  (no Empty seed)
            var filter = service.FieldEquals("color", new Primitive("red"))
                            .Or(service.FieldEquals("color", new Primitive("green")))
                            .Or(service.FieldEquals("color", new Primitive("blue")));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(3);
            result.Data.Select(r => r.Id).Should().NotContain("purple");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    [RetryFact(3, 5000)]
    public async Task Coupling_FourLevel_Complex_FullTree()
    {
        // Filter: ((type=cat AND score>5) OR (type=dog AND score>8)) AND active=true
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                // (cat, 7, active) → matches cat>5 AND active → YES
                new VectorPoint { Id = "cat-7-act",  Vector = v, Metadata = new JObject { ["type"] = "cat", ["score"] = 7, ["active"] = "true"  } },
                // (cat, 3, active) → fails cat>5 → NO
                new VectorPoint { Id = "cat-3-act",  Vector = v, Metadata = new JObject { ["type"] = "cat", ["score"] = 3, ["active"] = "true"  } },
                // (dog, 9, active) → matches dog>8 AND active → YES
                new VectorPoint { Id = "dog-9-act",  Vector = v, Metadata = new JObject { ["type"] = "dog", ["score"] = 9, ["active"] = "true"  } },
                // (dog, 9, inactive) → fails active → NO
                new VectorPoint { Id = "dog-9-inact",Vector = v, Metadata = new JObject { ["type"] = "dog", ["score"] = 9, ["active"] = "false" } },
                // (cat, 6, inactive) → fails active → NO
                new VectorPoint { Id = "cat-6-inact",Vector = v, Metadata = new JObject { ["type"] = "cat", ["score"] = 6, ["active"] = "false" } }
            ]);

            var catBranch = service.FieldEquals("type",  new Primitive("cat"))
                               .And(service.FieldGreaterThan("score", new Primitive(5)));
            var dogBranch = service.FieldEquals("type",  new Primitive("dog"))
                               .And(service.FieldGreaterThan("score", new Primitive(8)));
            var filter    = catBranch.Or(dogBranch)
                               .And(service.FieldEquals("active", new Primitive("true")));

            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(2);
            result.Data.Select(r => r.Id).Should().BeEquivalentTo(["cat-7-act", "dog-9-act"]);
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Boolean metadata ──────────────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task Filter_FieldEquals_Boolean_ShouldFilterCorrectly()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "t1", Vector = v, Metadata = new JObject { ["active"] = true  } },
                new VectorPoint { Id = "f1", Vector = v, Metadata = new JObject { ["active"] = false } },
                new VectorPoint { Id = "t2", Vector = v, Metadata = new JObject { ["active"] = true  } }
            ]);

            var filter = service.FieldEquals("active", new Primitive(true));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(2);
            result.Data.Select(r => r.Id).Should().BeEquivalentTo(["t1", "t2"]);
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Distance metric correctness ───────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task QueryAsync_EuclideanMetric_ShouldRankByL2Distance()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Euclidean);
            var close = new float[] { 0.5f, 0.5f, 0.5f, 0.5f };
            var far   = new float[] { -1f,  -1f,  -1f,  -1f  };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "close", Vector = close },
                new VectorPoint { Id = "far",   Vector = far   }
            ]);

            var query  = new float[] { 0.5f, 0.5f, 0.5f, 0.5f };
            var result = await service.QueryAsync(col, query, topK: 2);

            result.IsSuccessful.Should().BeTrue();
            result.Data[0].Id.Should().Be("close",
                because: "the Euclidean nearest neighbour should be the identical vector");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    [RetryFact(3, 5000)]
    public async Task QueryAsync_DotProductMetric_ShouldRankByDotProduct()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.DotProduct);

            var aligned = new float[] { 0.9f, 0.9f, 0.9f, 0.9f };
            var opposed = new float[] { -0.5f, -0.5f, -0.5f, -0.5f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "aligned", Vector = aligned },
                new VectorPoint { Id = "opposed", Vector = opposed }
            ]);

            var query  = new float[] { 1f, 1f, 1f, 1f };
            var result = await service.QueryAsync(col, query, topK: 2);

            result.IsSuccessful.Should().BeTrue();
            result.Data[0].Id.Should().Be("aligned",
                because: "the most aligned vector should have the highest dot product");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── includeMetadata / includeVector flags ─────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task GetAsync_WithIncludeVectorFalse_ShouldReturnEmptyVector()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var point = new VectorPoint
            {
                Id       = Guid.NewGuid().ToString(),
                Vector   = new float[] { 1f, 0f, 0f, 0f },
                Metadata = new JObject { ["tag"] = "hello" }
            };
            await service.UpsertAsync(col, point);

            var get = await service.GetAsync(col, point.Id, includeVector: false, includeMetadata: true);
            get.IsSuccessful.Should().BeTrue();
            get.Data.Should().NotBeNull();
            get.Data!.Vector.Should().BeEmpty(because: "includeVector was false");
            get.Data.Metadata?["tag"]?.Value<string>().Should().Be("hello");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    [RetryFact(3, 5000)]
    public async Task GetAsync_WithIncludeMetadataFalse_ShouldReturnNullMetadata()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var point = new VectorPoint
            {
                Id       = Guid.NewGuid().ToString(),
                Vector   = new float[] { 1f, 0f, 0f, 0f },
                Metadata = new JObject { ["tag"] = "hello" }
            };
            await service.UpsertAsync(col, point);

            var get = await service.GetAsync(col, point.Id, includeVector: true, includeMetadata: false);
            get.IsSuccessful.Should().BeTrue();
            get.Data.Should().NotBeNull();
            get.Data!.Metadata.Should().BeNull(because: "includeMetadata was false");
            get.Data.Vector.Should().NotBeEmpty();
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Null / missing metadata + filter ──────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task QueryAsync_NullMetadataPoints_ShouldNotMatchFieldEqualsFilter()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "with-meta", Vector = v, Metadata = new JObject { ["tag"] = "yes" } },
                new VectorPoint { Id = "no-meta",   Vector = v, Metadata = null }
            ]);

            var filter = service.FieldEquals("tag", new Primitive("yes"));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Select(r => r.Id).Should().Contain("with-meta");
            result.Data.Select(r => r.Id).Should().NotContain("no-meta",
                because: "a point with null metadata should not match a field equality filter");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── FieldNotEquals through OR coupling ────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task Filter_NotEquals_Or_NotEquals_ShouldMatchPointsMissingEitherValue()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "a", Vector = v, Metadata = new JObject { ["x"] = "a", ["y"] = "b" } },
                new VectorPoint { Id = "b", Vector = v, Metadata = new JObject { ["x"] = "c", ["y"] = "b" } },
                new VectorPoint { Id = "c", Vector = v, Metadata = new JObject { ["x"] = "a", ["y"] = "d" } }
            ]);

            // (x != "a") OR (y != "b")
            // "a": x=a → false, y=b → false → false ∨ false = false
            // "b": x=c → true,  y=b → false → true  ∨ false = true
            // "c": x=a → false, y=d → true  → false ∨ true  = true
            var filter = service.FieldNotEquals("x", new Primitive("a"))
                                .Or(service.FieldNotEquals("y", new Primitive("b")));

            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(2);
            result.Data.Select(r => r.Id).Should().BeEquivalentTo(["b", "c"]);
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Empty filter (no conditions) ──────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task QueryAsync_WithNullFilter_ShouldReturnAllPoints()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "p1", Vector = v },
                new VectorPoint { Id = "p2", Vector = v },
                new VectorPoint { Id = "p3", Vector = v }
            ]);

            var result = await service.QueryAsync(col, v, topK: 10, filter: null);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(3);
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── QueryAsync includeMetadata flag ───────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task QueryAsync_WithIncludeMetadataFalse_ShouldReturnNullMetadata()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertAsync(col, new VectorPoint
            {
                Id = "m1", Vector = v, Metadata = new JObject { ["key"] = "value" }
            });

            var result = await service.QueryAsync(col, v, topK: 1, includeMetadata: false);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(1);
            result.Data[0].Metadata.Should().BeNull(
                because: "includeMetadata was set to false");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Non-existent collection operations ────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task UpsertAsync_ToNonExistentCollection_ShouldReturnFailure()
    {
        await using var service = CreateVectorService();

        var result = await service.UpsertAsync("does-not-exist", new VectorPoint
        {
            Id = "x", Vector = new float[] { 1f, 0f, 0f, 0f }
        });

        result.IsSuccessful.Should().BeFalse(
            because: "inserting into a non-existent collection must fail");
    }

    [RetryFact(3, 5000)]
    public async Task QueryAsync_OnNonExistentCollection_ShouldReturnFailure()
    {
        await using var service = CreateVectorService();

        var result = await service.QueryAsync(
            "does-not-exist", new float[] { 1f, 0f, 0f, 0f }, topK: 1);

        result.IsSuccessful.Should().BeFalse(
            because: "querying a non-existent collection must fail");
    }

    [RetryFact(3, 5000)]
    public async Task GetAsync_OnNonExistentCollection_ShouldReturnFailure()
    {
        await using var service = CreateVectorService();

        var result = await service.GetAsync("does-not-exist", "some-id");

        result.IsSuccessful.Should().BeFalse(
            because: "getting from a non-existent collection must fail");
    }

    // ── Delete non-existent point ─────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task DeleteAsync_NonExistentPoint_ShouldNotThrow()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            // Should not throw — implementations may return success or failure
            var result = await service.DeleteAsync(col, Guid.NewGuid().ToString());
            // We only assert it doesn't throw; specific error behavior is implementation-defined
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Non-UUID string ID round-trip ─────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task UpsertAsync_WithNonUuidStringId_ShouldRoundTripCorrectly()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            var point = new VectorPoint
            {
                Id       = "my-custom-id-123",
                Vector   = new float[] { 1f, 0f, 0f, 0f },
                Metadata = new JObject { ["label"] = "custom" }
            };

            var upsert = await service.UpsertAsync(col, point);
            upsert.IsSuccessful.Should().BeTrue();

            var get = await service.GetAsync(col, "my-custom-id-123");
            get.IsSuccessful.Should().BeTrue();
            get.Data.Should().NotBeNull();
            get.Data!.Id.Should().Be("my-custom-id-123",
                because: "non-UUID string IDs must survive the round-trip");
            get.Data.Metadata?["label"]?.Value<string>().Should().Be("custom");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    [RetryFact(3, 5000)]
    public async Task QueryAsync_WithNonUuidStringIds_ShouldReturnOriginalIds()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "alpha",   Vector = v, Metadata = new JObject { ["x"] = 1 } },
                new VectorPoint { Id = "beta-2",  Vector = v, Metadata = new JObject { ["x"] = 2 } },
                new VectorPoint { Id = "gamma_3", Vector = v, Metadata = new JObject { ["x"] = 3 } }
            ]);

            var result = await service.QueryAsync(col, v, topK: 10);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Select(r => r.Id).Should().BeEquivalentTo(
                ["alpha", "beta-2", "gamma_3"],
                because: "query results must return the original non-UUID string IDs");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Empty batch upsert ────────────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task UpsertBatchAsync_EmptyList_ShouldSucceed()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            var result = await service.UpsertBatchAsync(col, []);
            result.IsSuccessful.Should().BeTrue(
                because: "upserting an empty batch should be a no-op success");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── TopK larger than point count ──────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task QueryAsync_TopKLargerThanPointCount_ShouldReturnAllPoints()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "p1", Vector = v },
                new VectorPoint { Id = "p2", Vector = v }
            ]);

            var result = await service.QueryAsync(col, v, topK: 100);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(2,
                because: "topK > total points should return all available points");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Score ordering invariant ──────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task QueryAsync_ResultsShouldBeOrderedByScoreDescending()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            // Create diverse vectors so scores differ
            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "exact",   Vector = new float[] { 1f, 0f, 0f, 0f } },
                new VectorPoint { Id = "close",   Vector = new float[] { 0.9f, 0.1f, 0f, 0f } },
                new VectorPoint { Id = "medium",  Vector = new float[] { 0.5f, 0.5f, 0.5f, 0f } },
                new VectorPoint { Id = "distant", Vector = new float[] { 0f, 0f, 0f, 1f } }
            ]);

            var query  = new float[] { 1f, 0f, 0f, 0f };
            var result = await service.QueryAsync(col, query, topK: 4);

            result.IsSuccessful.Should().BeTrue();
            var scores = result.Data.Select(r => r.Score).ToList();
            scores.Should().BeInDescendingOrder(
                because: "query results must be sorted by similarity score descending");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Metadata with multiple value types ────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task Metadata_MultipleValueTypes_ShouldRoundTrip()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            var id = Guid.NewGuid().ToString();
            var metadata = new JObject
            {
                ["name"]   = "widget",
                ["count"]  = 42,
                ["price"]  = 9.99,
                ["active"] = true
            };

            await service.UpsertAsync(col, new VectorPoint
            {
                Id = id, Vector = new float[] { 1f, 0f, 0f, 0f }, Metadata = metadata
            });

            var get = await service.GetAsync(col, id);
            get.IsSuccessful.Should().BeTrue();
            get.Data.Should().NotBeNull();
            get.Data!.Metadata?["name"]?.Value<string>().Should().Be("widget");
            get.Data.Metadata?["count"]?.Value<int>().Should().Be(42);
            get.Data.Metadata?["price"]?.Value<double>().Should().BeApproximately(9.99, 0.001);
            get.Data.Metadata?["active"]?.Value<bool>().Should().BeTrue();
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Duplicate IDs in single batch ─────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task UpsertBatchAsync_DuplicateIds_ShouldUseLastValue()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            var id = Guid.NewGuid().ToString();
            var result = await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = id, Vector = new float[] { 1f, 0f, 0f, 0f }, Metadata = new JObject { ["v"] = 1 } },
                new VectorPoint { Id = id, Vector = new float[] { 0f, 1f, 0f, 0f }, Metadata = new JObject { ["v"] = 2 } }
            ]);

            result.IsSuccessful.Should().BeTrue();

            var get = await service.GetAsync(col, id);
            get.IsSuccessful.Should().BeTrue();
            get.Data.Should().NotBeNull();
            get.Data!.Metadata?["v"]?.Value<int>().Should().Be(2,
                because: "when batch contains duplicate IDs, the last value should win");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Vector dimension mismatch ─────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task UpsertAsync_WrongDimensions_ShouldReturnFailure()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            // Insert a 6-dimensional vector into a 4-dim collection
            var result = await service.UpsertAsync(col, new VectorPoint
            {
                Id     = "wrong-dims",
                Vector = new float[] { 1f, 0f, 0f, 0f, 0.5f, 0.5f }
            });

            result.IsSuccessful.Should().BeFalse(
                because: "upserting a vector with wrong dimensions should fail");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Filter on field that no point has ─────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task Filter_OnFieldNoneHave_ShouldReturnEmpty()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "a", Vector = v, Metadata = new JObject { ["x"] = 1 } },
                new VectorPoint { Id = "b", Vector = v, Metadata = new JObject { ["x"] = 2 } }
            ]);

            var filter = service.FieldEquals("nonexistent-field", new Primitive("anything"));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeEmpty(
                because: "no point has the filtered field");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Filter: FieldEquals with double value ─────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task Filter_FieldEquals_Double_ShouldFilterCorrectly()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "a", Vector = v, Metadata = new JObject { ["price"] = 9.99  } },
                new VectorPoint { Id = "b", Vector = v, Metadata = new JObject { ["price"] = 19.99 } },
                new VectorPoint { Id = "c", Vector = v, Metadata = new JObject { ["price"] = 9.99  } }
            ]);

            var filter = service.FieldEquals("price", new Primitive(9.99));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(2);
            result.Data.Select(r => r.Id).Should().BeEquivalentTo(["a", "c"]);
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Empty string metadata value ───────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task Metadata_EmptyStringValue_ShouldNotBeConfusedWithMissing()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "empty-str", Vector = v, Metadata = new JObject { ["tag"] = "" } },
                new VectorPoint { Id = "no-tag",    Vector = v, Metadata = new JObject { ["other"] = 1 } }
            ]);

            // FieldExists should match "empty-str" because the field exists (even if empty)
            var existsFilter = service.FieldExists("tag");
            var result = await service.QueryAsync(col, v, topK: 10, filter: existsFilter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Select(r => r.Id).Should().Contain("empty-str");
            result.Data.Select(r => r.Id).Should().NotContain("no-tag");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── AND with NotEquals: exclude specific combination ──────────────────────

    [RetryFact(3, 5000)]
    public async Task Filter_NotEquals_And_Equals_ShouldCombineCorrectly()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "a", Vector = v, Metadata = new JObject { ["status"] = "active", ["role"] = "admin" } },
                new VectorPoint { Id = "b", Vector = v, Metadata = new JObject { ["status"] = "active", ["role"] = "user"  } },
                new VectorPoint { Id = "c", Vector = v, Metadata = new JObject { ["status"] = "banned", ["role"] = "admin" } }
            ]);

            // status=active AND role!=admin → should only match "b"
            var filter = service.FieldEquals("status", new Primitive("active"))
                                .And(service.FieldNotEquals("role", new Primitive("admin")));

            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(1);
            result.Data[0].Id.Should().Be("b");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Single point query ────────────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task QueryAsync_SinglePointInCollection_ShouldReturnThatPoint()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            await service.UpsertAsync(col, new VectorPoint
            {
                Id = "only", Vector = new float[] { 1f, 0f, 0f, 0f }
            });

            var result = await service.QueryAsync(col, new float[] { 0f, 1f, 0f, 0f }, topK: 5);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(1);
            result.Data[0].Id.Should().Be("only");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── (A OR B) AND (C OR D) coupling ────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task Coupling_Or_And_Or_BothBranchesWithOr_ShouldEvaluateCorrectly()
    {
        // Filter: (color=red OR color=blue) AND (size=small OR size=medium)
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "rs", Vector = v, Metadata = new JObject { ["color"] = "red",   ["size"] = "small"  } },
                new VectorPoint { Id = "rm", Vector = v, Metadata = new JObject { ["color"] = "red",   ["size"] = "medium" } },
                new VectorPoint { Id = "rl", Vector = v, Metadata = new JObject { ["color"] = "red",   ["size"] = "large"  } },
                new VectorPoint { Id = "bs", Vector = v, Metadata = new JObject { ["color"] = "blue",  ["size"] = "small"  } },
                new VectorPoint { Id = "gl", Vector = v, Metadata = new JObject { ["color"] = "green", ["size"] = "large"  } },
                new VectorPoint { Id = "gm", Vector = v, Metadata = new JObject { ["color"] = "green", ["size"] = "medium" } }
            ]);

            var colorFilter = service.FieldEquals("color", new Primitive("red"))
                                 .Or(service.FieldEquals("color", new Primitive("blue")));
            var sizeFilter  = service.FieldEquals("size", new Primitive("small"))
                                 .Or(service.FieldEquals("size", new Primitive("medium")));
            var filter = colorFilter.And(sizeFilter);

            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            // rs (red,small) ✓  rm (red,medium) ✓  rl (red,large) ✗  bs (blue,small) ✓
            // gl (green,large) ✗  gm (green,medium) ✗
            result.Data.Should().HaveCount(3,
                because: "(red OR blue) AND (small OR medium) must require BOTH conditions");
            result.Data.Select(r => r.Id).Should().BeEquivalentTo(["rs", "rm", "bs"]);
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Query empty collection ────────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task QueryAsync_EmptyCollection_ShouldReturnEmptyResults()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            var result = await service.QueryAsync(col, new float[] { 1f, 0f, 0f, 0f }, topK: 5);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeEmpty(
                because: "an empty collection has no points to return");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── FieldNotEquals when field is absent ────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task Filter_FieldNotEquals_WhenFieldMissing_ShouldMatch()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "has-admin", Vector = v, Metadata = new JObject { ["role"] = "admin" } },
                new VectorPoint { Id = "has-user",  Vector = v, Metadata = new JObject { ["role"] = "user"  } },
                new VectorPoint { Id = "no-role",   Vector = v, Metadata = new JObject { ["other"] = 1 } }
            ]);

            var filter = service.FieldNotEquals("role", new Primitive("admin"));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            // has-user: role=user ≠ admin → match
            // no-role: role absent → definitionally not "admin" → match
            result.Data.Should().HaveCount(2);
            result.Data.Select(r => r.Id).Should().BeEquivalentTo(["has-user", "no-role"],
                because: "a missing field is definitionally not equal to any value");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── UpsertBatch dimension mismatch ────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task UpsertBatchAsync_WrongDimensions_ShouldReturnFailure()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            var result = await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "ok",    Vector = new float[] { 1f, 0f, 0f, 0f } },
                new VectorPoint { Id = "wrong", Vector = new float[] { 1f, 0f } }
            ]);

            result.IsSuccessful.Should().BeFalse(
                because: "a batch containing a vector with wrong dimensions should fail");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── FieldNotEquals on null-metadata point ─────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task Filter_FieldNotEquals_NullMetadata_ShouldMatch()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "with-tag", Vector = v, Metadata = new JObject { ["tag"] = "x"  } },
                new VectorPoint { Id = "no-meta",  Vector = v, Metadata = null }
            ]);

            var filter = service.FieldNotEquals("tag", new Primitive("x"));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            // with-tag: tag=x → excluded (equals x)
            // no-meta: no metadata at all → not equal to x → included
            result.Data.Should().HaveCount(1);
            result.Data[0].Id.Should().Be("no-meta",
                because: "a point with null metadata has no fields, so NotEquals should match");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Integer/Double cross-type equality ────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task Filter_FieldEquals_IntegerPrimitiveOnDoubleMetadata_ShouldMatch()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            // Store 42.0 (double/float in JSON), query with integer 42
            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "a", Vector = v, Metadata = new JObject { ["val"] = 42.0 } },
                new VectorPoint { Id = "b", Vector = v, Metadata = new JObject { ["val"] = 99.0 } }
            ]);

            var filter = service.FieldEquals("val", new Primitive(42));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(1);
            result.Data[0].Id.Should().Be("a",
                because: "integer primitive 42 should match double metadata 42.0");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    [RetryFact(3, 5000)]
    public async Task Filter_FieldEquals_DoublePrimitiveOnIntegerMetadata_ShouldMatch()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            // Store 42 (integer in JSON), query with double 42.0
            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "a", Vector = v, Metadata = new JObject { ["val"] = 42 } },
                new VectorPoint { Id = "b", Vector = v, Metadata = new JObject { ["val"] = 99 } }
            ]);

            var filter = service.FieldEquals("val", new Primitive(42.0));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(1);
            result.Data[0].Id.Should().Be("a",
                because: "double primitive 42.0 should match integer metadata 42");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Delete then re-insert same ID ─────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task DeleteAsync_ThenReinsertSameId_ShouldUseNewData()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            var id = "reuse-me";
            await service.UpsertAsync(col, new VectorPoint
            {
                Id = id, Vector = new float[] { 1f, 0f, 0f, 0f }, Metadata = new JObject { ["v"] = 1 }
            });

            var del = await service.DeleteAsync(col, id);
            del.IsSuccessful.Should().BeTrue();

            await service.UpsertAsync(col, new VectorPoint
            {
                Id = id, Vector = new float[] { 0f, 1f, 0f, 0f }, Metadata = new JObject { ["v"] = 2 }
            });

            var get = await service.GetAsync(col, id);
            get.IsSuccessful.Should().BeTrue();
            get.Data.Should().NotBeNull();
            get.Data!.Metadata?["v"]?.Value<int>().Should().Be(2,
                because: "re-inserted point should have new metadata");
            get.Data.Vector[1].Should().BeApproximately(1f, 0.01f,
                because: "re-inserted point should have new vector");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Filter matches ALL points ─────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task Filter_MatchesAllPoints_ShouldReturnAll()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "a", Vector = v, Metadata = new JObject { ["active"] = true } },
                new VectorPoint { Id = "b", Vector = v, Metadata = new JObject { ["active"] = true } },
                new VectorPoint { Id = "c", Vector = v, Metadata = new JObject { ["active"] = true } }
            ]);

            var filter = service.FieldEquals("active", new Primitive(true));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(3,
                because: "all points match the filter");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Double dispose should not throw ───────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task DisposeAsync_CalledTwice_ShouldNotThrow()
    {
        var service = CreateVectorService();
        string col = UniqueCollection();

        await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
        await service.DeleteCollectionAsync(col);

        await service.DisposeAsync();

        Func<Task> act = async () => await service.DisposeAsync();
        await act.Should().NotThrowAsync(
            because: "disposing an already-disposed service should be safe (idempotent)");
    }

    // ── NotEquals with Integer across types ───────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task Filter_FieldNotEquals_IntegerPrimitive_OnDoubleMetadata_ShouldExcludeMatch()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "a", Vector = v, Metadata = new JObject { ["val"] = 42.0 } },
                new VectorPoint { Id = "b", Vector = v, Metadata = new JObject { ["val"] = 99.0 } }
            ]);

            var filter = service.FieldNotEquals("val", new Primitive(42));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(1);
            result.Data[0].Id.Should().Be("b",
                because: "integer NotEquals 42 should exclude double metadata 42.0");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── FieldGreaterThan with double primitive on integer metadata ─────────────

    [RetryFact(3, 5000)]
    public async Task Filter_FieldGreaterThan_DoublePrimitive_OnIntegerMetadata()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "low",  Vector = v, Metadata = new JObject { ["score"] = 10 } },
                new VectorPoint { Id = "high", Vector = v, Metadata = new JObject { ["score"] = 50 } }
            ]);

            var filter = service.FieldGreaterThan("score", new Primitive(10.5));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(1);
            result.Data[0].Id.Should().Be("high",
                because: "integer 50 > double 10.5");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Query vector dimension mismatch ───────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task QueryAsync_WrongDimensionQueryVector_ShouldReturnFailure()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            await service.UpsertAsync(col, new VectorPoint
            {
                Id = "p1", Vector = new float[] { 1f, 0f, 0f, 0f }
            });

            // Query with 3-dimensional vector into a 4-dimensional collection
            var result = await service.QueryAsync(col, new float[] { 1f, 0f, 0f }, topK: 5);

            result.IsSuccessful.Should().BeFalse(
                because: "query vector dimensions must match the collection's configured dimensions");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Boolean false filter ──────────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task Filter_FieldEquals_BooleanFalse_ShouldFilterCorrectly()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "t1", Vector = v, Metadata = new JObject { ["active"] = true  } },
                new VectorPoint { Id = "f1", Vector = v, Metadata = new JObject { ["active"] = false } },
                new VectorPoint { Id = "f2", Vector = v, Metadata = new JObject { ["active"] = false } }
            ]);

            var filter = service.FieldEquals("active", new Primitive(false));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(2);
            result.Data.Select(r => r.Id).Should().BeEquivalentTo(["f1", "f2"]);
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── FieldNotEquals boolean ────────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task Filter_FieldNotEquals_Boolean_ShouldExcludeMatching()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "t1", Vector = v, Metadata = new JObject { ["active"] = true  } },
                new VectorPoint { Id = "f1", Vector = v, Metadata = new JObject { ["active"] = false } },
                new VectorPoint { Id = "t2", Vector = v, Metadata = new JObject { ["active"] = true  } }
            ]);

            var filter = service.FieldNotEquals("active", new Primitive(true));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(1);
            result.Data[0].Id.Should().Be("f1");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── AggregateOr with conditions ───────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task Coupling_AggregateOr_MultipleConditions_ShouldMatchAny()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "r", Vector = v, Metadata = new JObject { ["color"] = "red"    } },
                new VectorPoint { Id = "g", Vector = v, Metadata = new JObject { ["color"] = "green"  } },
                new VectorPoint { Id = "b", Vector = v, Metadata = new JObject { ["color"] = "blue"   } },
                new VectorPoint { Id = "y", Vector = v, Metadata = new JObject { ["color"] = "yellow" } }
            ]);

            var conditions = new[]
            {
                service.FieldEquals("color", new Primitive("red")),
                service.FieldEquals("color", new Primitive("blue"))
            };
            var filter = conditions.AggregateOr();
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(2);
            result.Data.Select(r => r.Id).Should().BeEquivalentTo(["r", "b"]);
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Get with both includeVector=false and includeMetadata=false ───────────

    [RetryFact(3, 5000)]
    public async Task GetAsync_WithBothIncludesFalse_ShouldReturnIdOnly()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var id = Guid.NewGuid().ToString();
            await service.UpsertAsync(col, new VectorPoint
            {
                Id       = id,
                Vector   = new float[] { 1f, 0f, 0f, 0f },
                Metadata = new JObject { ["key"] = "value" }
            });

            var get = await service.GetAsync(col, id, includeVector: false, includeMetadata: false);
            get.IsSuccessful.Should().BeTrue();
            get.Data.Should().NotBeNull();
            get.Data!.Id.Should().Be(id);
            get.Data.Vector.Should().BeEmpty(because: "includeVector was false");
            get.Data.Metadata.Should().BeNull(because: "includeMetadata was false");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Upsert with empty JObject metadata (not null) ─────────────────────────

    [RetryFact(3, 5000)]
    public async Task UpsertAsync_EmptyMetadataJObject_ShouldRoundTrip()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var id = Guid.NewGuid().ToString();
            await service.UpsertAsync(col, new VectorPoint
            {
                Id       = id,
                Vector   = new float[] { 1f, 0f, 0f, 0f },
                Metadata = new JObject()
            });

            var get = await service.GetAsync(col, id);
            get.IsSuccessful.Should().BeTrue();
            get.Data.Should().NotBeNull();
            get.Data!.Id.Should().Be(id);
            get.Data.Vector.Should().NotBeEmpty();
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Delete from non-existent collection ───────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task DeleteAsync_FromNonExistentCollection_ShouldReturnFailure()
    {
        await using var service = CreateVectorService();

        var result = await service.DeleteAsync("does-not-exist", "some-id");

        result.IsSuccessful.Should().BeFalse(
            because: "deleting from a non-existent collection must fail");
    }

    // ── Non-UUID ID delete round-trip ─────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task DeleteAsync_WithNonUuidStringId_ShouldRemovePoint()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "custom-id-A",  Vector = v },
                new VectorPoint { Id = "custom-id-B",  Vector = v }
            ]);

            var del = await service.DeleteAsync(col, "custom-id-A");
            del.IsSuccessful.Should().BeTrue();

            var get = await service.GetAsync(col, "custom-id-A");
            get.IsSuccessful.Should().BeTrue();
            get.Data.Should().BeNull(because: "the point was deleted");

            var getB = await service.GetAsync(col, "custom-id-B");
            getB.IsSuccessful.Should().BeTrue();
            getB.Data.Should().NotBeNull(because: "only custom-id-A was deleted");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Filter with FieldGreaterThan on exact boundary ────────────────────────

    [RetryFact(3, 5000)]
    public async Task Filter_FieldGreaterThan_ExactBoundary_ShouldExcludeBoundary()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "at",    Vector = v, Metadata = new JObject { ["score"] = 50 } },
                new VectorPoint { Id = "above", Vector = v, Metadata = new JObject { ["score"] = 51 } },
                new VectorPoint { Id = "below", Vector = v, Metadata = new JObject { ["score"] = 49 } }
            ]);

            var filter = service.FieldGreaterThan("score", new Primitive(50));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(1);
            result.Data[0].Id.Should().Be("above",
                because: "GreaterThan is strict — the boundary value 50 must be excluded");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Filter with FieldLessThan on exact boundary ───────────────────────────

    [RetryFact(3, 5000)]
    public async Task Filter_FieldLessThan_ExactBoundary_ShouldExcludeBoundary()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "at",    Vector = v, Metadata = new JObject { ["score"] = 50 } },
                new VectorPoint { Id = "above", Vector = v, Metadata = new JObject { ["score"] = 51 } },
                new VectorPoint { Id = "below", Vector = v, Metadata = new JObject { ["score"] = 49 } }
            ]);

            var filter = service.FieldLessThan("score", new Primitive(50));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(1);
            result.Data[0].Id.Should().Be("below",
                because: "LessThan is strict — the boundary value 50 must be excluded");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Edge case: topK = 0 ───────────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task QueryAsync_TopKZero_ShouldReturnEmpty()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertAsync(col, new VectorPoint { Id = "a", Vector = v });

            var result = await service.QueryAsync(col, v, topK: 0);

            // topK=0 should either succeed with empty results or fail gracefully
            if (result.IsSuccessful)
                result.Data.Should().BeEmpty(
                    because: "requesting zero results should return an empty list");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Edge case: EnsureCollectionExists is idempotent ───────────────────────

    [RetryFact(3, 5000)]
    public async Task EnsureCollectionExistsAsync_CalledTwice_ShouldBeIdempotent()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            var first  = await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var second = await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            first.IsSuccessful.Should().BeTrue();
            second.IsSuccessful.Should().BeTrue(
                because: "EnsureCollectionExists should be idempotent");

            // Verify the collection is functional
            var v = new float[] { 1f, 0f, 0f, 0f };
            var up = await service.UpsertAsync(col, new VectorPoint { Id = "t", Vector = v });
            up.IsSuccessful.Should().BeTrue();
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Edge case: query with includeMetadata true returns metadata ──────────

    [RetryFact(3, 5000)]
    public async Task QueryAsync_IncludeMetadataTrue_ShouldReturnMetadata()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertAsync(col, new VectorPoint
            {
                Id = "p1", Vector = v, Metadata = new JObject { ["x"] = 1 }
            });

            var result = await service.QueryAsync(col, v, topK: 1, includeMetadata: true);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(1);
            result.Data[0].Metadata.Should().NotBeNull(
                because: "includeMetadata=true should return the stored metadata");
            result.Data[0].Metadata?["x"]?.Value<int>().Should().Be(1);
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Edge case: delete collection then query should fail ───────────────────

    [RetryFact(3, 5000)]
    public async Task DeleteCollectionAsync_ThenQuery_ShouldReturnFailure()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
        var v = new float[] { 1f, 0f, 0f, 0f };

        await service.UpsertAsync(col, new VectorPoint { Id = "a", Vector = v });
        await service.DeleteCollectionAsync(col);

        var result = await service.QueryAsync(col, v, topK: 5);
        result.IsSuccessful.Should().BeFalse(
            because: "querying a deleted collection should fail");
    }

    // ── Edge case: AggregateOr with single condition ──────────────────────────

    [RetryFact(3, 5000)]
    public async Task Coupling_AggregateOr_SingleCondition_ShouldBehaveAsDirectCondition()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "a", Vector = v, Metadata = new JObject { ["color"] = "red"   } },
                new VectorPoint { Id = "b", Vector = v, Metadata = new JObject { ["color"] = "green" } },
                new VectorPoint { Id = "c", Vector = v, Metadata = new JObject { ["color"] = "blue"  } }
            ]);

            // AggregateOr with a single condition should behave the same as that condition alone
            var conditions = new[] { service.FieldEquals("color", new Primitive("red")) };
            var filter = conditions.AggregateOr();
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(1);
            result.Data[0].Id.Should().Be("a");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Edge case: FieldGreaterThanOrEqual on exact boundary ──────────────────

    [RetryFact(3, 5000)]
    public async Task Filter_FieldGreaterThanOrEqual_ExactBoundary_ShouldIncludeBoundary()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "at",    Vector = v, Metadata = new JObject { ["score"] = 50 } },
                new VectorPoint { Id = "above", Vector = v, Metadata = new JObject { ["score"] = 51 } },
                new VectorPoint { Id = "below", Vector = v, Metadata = new JObject { ["score"] = 49 } }
            ]);

            var filter = service.FieldGreaterThanOrEqual("score", new Primitive(50));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(2);
            result.Data.Select(r => r.Id).Should().BeEquivalentTo(["at", "above"],
                because: "GreaterThanOrEqual must include the boundary value");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Edge case: FieldLessThanOrEqual on exact boundary ─────────────────────

    [RetryFact(3, 5000)]
    public async Task Filter_FieldLessThanOrEqual_ExactBoundary_ShouldIncludeBoundary()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "at",    Vector = v, Metadata = new JObject { ["score"] = 50 } },
                new VectorPoint { Id = "above", Vector = v, Metadata = new JObject { ["score"] = 51 } },
                new VectorPoint { Id = "below", Vector = v, Metadata = new JObject { ["score"] = 49 } }
            ]);

            var filter = service.FieldLessThanOrEqual("score", new Primitive(50));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(2);
            result.Data.Select(r => r.Id).Should().BeEquivalentTo(["at", "below"],
                because: "LessThanOrEqual must include the boundary value");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Edge case: upsert update replaces metadata (not merge) ────────────────

    [RetryFact(3, 5000)]
    public async Task UpsertAsync_UpdateReplaceMetadata_ShouldNotMerge()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };
            var id = Guid.NewGuid().ToString();

            // First insert with two fields
            await service.UpsertAsync(col, new VectorPoint
            {
                Id = id, Vector = v, Metadata = new JObject { ["a"] = 1, ["b"] = 2 }
            });

            // Update with only one field — should REPLACE, not merge
            await service.UpsertAsync(col, new VectorPoint
            {
                Id = id, Vector = v, Metadata = new JObject { ["c"] = 3 }
            });

            var get = await service.GetAsync(col, id);
            get.IsSuccessful.Should().BeTrue();
            get.Data.Should().NotBeNull();
            get.Data!.Metadata?["c"]?.Value<int>().Should().Be(3,
                because: "the new metadata should be present");
            get.Data.Metadata?["a"].Should().BeNull(
                because: "upsert should replace, not merge metadata");
            get.Data.Metadata?["b"].Should().BeNull(
                because: "upsert should replace, not merge metadata");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Edge case: numeric comparison on non-numeric field ────────────────────

    [RetryFact(3, 5000)]
    public async Task Filter_FieldGreaterThan_OnStringField_ShouldNotMatch()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "a", Vector = v, Metadata = new JObject { ["name"] = "alice" } },
                new VectorPoint { Id = "b", Vector = v, Metadata = new JObject { ["name"] = "bob"   } }
            ]);

            // Numeric filter on a string field — should not match any points
            var filter = service.FieldGreaterThan("name", new Primitive(0));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeEmpty(
                because: "a numeric comparison on a string field should not match");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Edge case: query with all identical vectors (tie-breaking) ─────────────

    [RetryFact(3, 5000)]
    public async Task QueryAsync_AllIdenticalVectors_ShouldReturnAll()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 0.5f, 0.5f, 0.5f, 0.5f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "a", Vector = v },
                new VectorPoint { Id = "b", Vector = v },
                new VectorPoint { Id = "c", Vector = v }
            ]);

            var result = await service.QueryAsync(col, v, topK: 10);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(3,
                because: "all points are equidistant and topK exceeds count");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Edge case: numeric filter on string field with negative primitive ──────

    [RetryFact(3, 5000)]
    public async Task Filter_FieldGreaterThan_OnStringField_WithNegativePrimitive_ShouldNotMatch()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "a", Vector = v, Metadata = new JObject { ["name"] = "alice" } },
                new VectorPoint { Id = "b", Vector = v, Metadata = new JObject { ["name"] = "bob"   } }
            ]);

            // Even with a negative primitive, a string field should never match a numeric comparison
            var filter = service.FieldGreaterThan("name", new Primitive(-1));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeEmpty(
                because: "a numeric comparison on a string field must never match, regardless of the primitive value");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Edge case: LessThan on string field should not match ──────────────────

    [RetryFact(3, 5000)]
    public async Task Filter_FieldLessThan_OnStringField_ShouldNotMatch()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "a", Vector = v, Metadata = new JObject { ["tag"] = "hello" } }
            ]);

            var filter = service.FieldLessThan("tag", new Primitive(999));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeEmpty(
                because: "LessThan comparison on a string field must not match");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Edge case: numeric comparison on boolean field should not match ────────

    [RetryFact(3, 5000)]
    public async Task Filter_FieldGreaterThan_OnBooleanField_ShouldNotMatch()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "a", Vector = v, Metadata = new JObject { ["active"] = true  } },
                new VectorPoint { Id = "b", Vector = v, Metadata = new JObject { ["active"] = false } }
            ]);

            var filter = service.FieldGreaterThan("active", new Primitive(-1));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeEmpty(
                because: "a numeric comparison on a boolean field must not match");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Edge case: mixed type fields — only numeric ones match range filters ──

    [RetryFact(3, 5000)]
    public async Task Filter_FieldGreaterThan_MixedFieldTypes_ShouldMatchOnlyNumeric()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "num",  Vector = v, Metadata = new JObject { ["val"] = 100   } },
                new VectorPoint { Id = "str",  Vector = v, Metadata = new JObject { ["val"] = "abc" } },
                new VectorPoint { Id = "bool", Vector = v, Metadata = new JObject { ["val"] = true  } }
            ]);

            var filter = service.FieldGreaterThan("val", new Primitive(50));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(1);
            result.Data[0].Id.Should().Be("num",
                because: "only the numeric field should match the range filter");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Edge case: large batch upsert ─────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task UpsertBatchAsync_LargeBatch_ShouldSucceed()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            var points = Enumerable.Range(0, 50).Select(i => new VectorPoint
            {
                Id       = $"item-{i}",
                Vector   = new float[] { 1f, 0f, 0f, 0f },
                Metadata = new JObject { ["index"] = i }
            }).ToList();

            var result = await service.UpsertBatchAsync(col, points);
            result.IsSuccessful.Should().BeTrue();

            var query = await service.QueryAsync(col, new float[] { 1f, 0f, 0f, 0f }, topK: 100);
            query.IsSuccessful.Should().BeTrue();
            query.Data.Should().HaveCount(50,
                because: "all 50 points should be retrievable after batch upsert");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Edge case: GetCollectionNames after delete ────────────────────────────

    [RetryFact(3, 5000)]
    public async Task GetCollectionNamesAsync_AfterDelete_ShouldNotContainCollection()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
        await service.DeleteCollectionAsync(col);

        var names = await service.GetCollectionNamesAsync();
        names.IsSuccessful.Should().BeTrue();
        names.Data.Should().NotContain(col,
            because: "a deleted collection should not appear in the collection list");
    }

    // ── Edge case: Get returns null for missing point (not failure) ────────────

    [RetryFact(3, 5000)]
    public async Task GetAsync_MissingPoint_ShouldReturnSuccessWithNull()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            var result = await service.GetAsync(col, "nonexistent-id");
            result.IsSuccessful.Should().BeTrue(
                because: "a missing point should return success with null data, not a failure");
            result.Data.Should().BeNull();
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Edge case: filter excludes all points ─────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task Filter_ExcludesAllPoints_ShouldReturnEmpty()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "a", Vector = v, Metadata = new JObject { ["x"] = 1 } },
                new VectorPoint { Id = "b", Vector = v, Metadata = new JObject { ["x"] = 2 } }
            ]);

            // x > 100 excludes everything
            var filter = service.FieldGreaterThan("x", new Primitive(100));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().BeEmpty(
                because: "when the filter excludes all points, the result should be empty");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Edge case: AggregateAnd with multiple conditions ──────────────────────

    [RetryFact(3, 5000)]
    public async Task Coupling_AggregateAnd_MultipleConditions_ShouldRequireAll()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "all",  Vector = v, Metadata = new JObject { ["a"] = 1, ["b"] = 2, ["c"] = 3 } },
                new VectorPoint { Id = "ab",   Vector = v, Metadata = new JObject { ["a"] = 1, ["b"] = 2, ["c"] = 99 } },
                new VectorPoint { Id = "none", Vector = v, Metadata = new JObject { ["a"] = 99, ["b"] = 99, ["c"] = 99 } }
            ]);

            var conditions = new[]
            {
                service.FieldEquals("a", new Primitive(1)),
                service.FieldEquals("b", new Primitive(2)),
                service.FieldEquals("c", new Primitive(3))
            };
            var filter = conditions.AggregateAnd();
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(1);
            result.Data[0].Id.Should().Be("all",
                because: "AggregateAnd requires all conditions to be satisfied");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Edge case: FieldNotExists on points where field is genuinely absent ───

    [RetryFact(3, 5000)]
    public async Task FieldNotExists_AbsentField_ShouldMatchPointsMissingField()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            // "has-role" has the field; "no-role" does NOT have the field at all.
            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "has-role", Vector = v, Metadata = new JObject { ["role"] = "admin" } },
                new VectorPoint { Id = "no-role",  Vector = v, Metadata = new JObject { ["other"] = 1 } }
            ]);

            var filter = service.FieldNotExists("role");
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(1);
            result.Data[0].Id.Should().Be("no-role",
                because: "FieldNotExists should match points where the field is genuinely absent from the payload");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Edge case: FieldExists + FieldNotExists combined in AND filter ────────

    [RetryFact(3, 5000)]
    public async Task Filter_FieldExists_And_FieldNotExists_Combined()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "both",   Vector = v, Metadata = new JObject { ["a"] = 1, ["b"] = 2 } },
                new VectorPoint { Id = "a-only", Vector = v, Metadata = new JObject { ["a"] = 1 } },
                new VectorPoint { Id = "b-only", Vector = v, Metadata = new JObject { ["b"] = 2 } },
                new VectorPoint { Id = "none",   Vector = v, Metadata = new JObject { ["c"] = 3 } }
            ]);

            // "a" must exist AND "b" must NOT exist
            var filter = service.FieldExists("a").And(service.FieldNotExists("b"));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(1);
            result.Data[0].Id.Should().Be("a-only",
                because: "only a-only has field 'a' present and field 'b' absent");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Edge case: FieldNotEquals on a point where the field is missing ───────

    [RetryFact(3, 5000)]
    public async Task FieldNotEquals_MissingField_ShouldMatchBecauseMissingIsNotEqual()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "has",     Vector = v, Metadata = new JObject { ["status"] = "active" } },
                new VectorPoint { Id = "diff",    Vector = v, Metadata = new JObject { ["status"] = "inactive" } },
                new VectorPoint { Id = "missing", Vector = v, Metadata = new JObject { ["other"] = 1 } }
            ]);

            var filter = service.FieldNotEquals("status", new Primitive("active"));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            var ids = result.Data.Select(r => r.Id).ToList();
            ids.Should().Contain("diff", because: "'inactive' != 'active'");
            ids.Should().Contain("missing", because: "missing field is definitionally not equal to any value");
            ids.Should().NotContain("has", because: "'active' == 'active'");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Edge case: AggregateOr with multiple conditions ───────────────────────

    [RetryFact(3, 5000)]
    public async Task Coupling_AggregateOr_MultipleConditions_EdgeCase_ShouldMatchAny()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "a",    Vector = v, Metadata = new JObject { ["tag"] = "alpha" } },
                new VectorPoint { Id = "b",    Vector = v, Metadata = new JObject { ["tag"] = "beta" } },
                new VectorPoint { Id = "c",    Vector = v, Metadata = new JObject { ["tag"] = "gamma" } },
                new VectorPoint { Id = "none", Vector = v, Metadata = new JObject { ["tag"] = "delta" } }
            ]);

            var conditions = new[]
            {
                service.FieldEquals("tag", new Primitive("alpha")),
                service.FieldEquals("tag", new Primitive("beta")),
                service.FieldEquals("tag", new Primitive("gamma"))
            };
            var filter = conditions.AggregateOr();
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            var ids = result.Data.Select(r => r.Id).ToList();
            ids.Should().HaveCount(3);
            ids.Should().Contain("a");
            ids.Should().Contain("b");
            ids.Should().Contain("c");
            ids.Should().NotContain("none",
                because: "delta does not match any of the three OR conditions");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Edge case: query with zero vector (cosine similarity) ─────────────────

    [RetryFact(3, 5000)]
    public async Task QueryAsync_ZeroVector_Cosine_ShouldReturnResultsWithZeroScore()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };
            await service.UpsertAsync(col, new VectorPoint { Id = "p1", Vector = v });

            var zeroVector = new float[] { 0f, 0f, 0f, 0f };
            var result = await service.QueryAsync(col, zeroVector, topK: 5);

            result.IsSuccessful.Should().BeTrue(
                because: "querying with a zero vector should not throw; " +
                         (result.IsSuccessful ? "" : result.ErrorMessage));
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Edge case: greater-than with non-numeric field value ──────────────────

    [RetryFact(3, 5000)]
    public async Task FieldGreaterThan_NonNumericField_ShouldNotMatch()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "str",  Vector = v, Metadata = new JObject { ["val"] = "hello" } },
                new VectorPoint { Id = "num5", Vector = v, Metadata = new JObject { ["val"] = 5 } }
            ]);

            // "val" > 3: should match num5 (5 > 3) but NOT str (string isn't comparable numerically)
            var filter = service.FieldGreaterThan("val", new Primitive(3));
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().HaveCount(1);
            result.Data[0].Id.Should().Be("num5",
                because: "a string value should never satisfy a numeric comparison");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Edge case: non-UUID string ID survives GetAsync with includeMetadata=false ──

    /// <summary>
    /// Regression test: implementations that hash non-UUID IDs to an internal UUID
    /// must still return the original string ID even when <c>includeMetadata</c> is
    /// <c>false</c> (i.e. the payload is not fetched by default).
    /// </summary>
    [RetryFact(3, 5000)]
    public async Task GetAsync_IncludeMetadataFalse_NonUuidId_ShouldReturnOriginalId()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();
        const string id = "non-uuid-id-regression-test";

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            await service.UpsertAsync(col, new VectorPoint
            {
                Id       = id,
                Vector   = new float[] { 1f, 0f, 0f, 0f },
                Metadata = new JObject { ["key"] = "value" }
            });

            var get = await service.GetAsync(col, id, includeVector: false, includeMetadata: false);

            get.IsSuccessful.Should().BeTrue();
            get.Data.Should().NotBeNull();
            get.Data!.Id.Should().Be(id,
                because: "the original non-UUID ID must be returned even when includeMetadata=false");
            get.Data.Vector.Should().BeEmpty(because: "includeVector was false");
            get.Data.Metadata.Should().BeNull(because: "includeMetadata was false");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Edge case: non-UUID string IDs survive QueryAsync with includeMetadata=false ──

    /// <summary>
    /// Regression test: implementations that hash non-UUID IDs to an internal UUID
    /// must still return the original string IDs in query results even when
    /// <c>includeMetadata</c> is <c>false</c>.
    /// </summary>
    [RetryFact(3, 5000)]
    public async Task QueryAsync_IncludeMetadataFalse_NonUuidIds_ShouldReturnOriginalIds()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            var points = new[]
            {
                new VectorPoint { Id = "batch-0", Vector = new float[] { 1f, 0f, 0f, 0f }, Metadata = new JObject { ["i"] = 0 } },
                new VectorPoint { Id = "batch-1", Vector = new float[] { 0f, 1f, 0f, 0f }, Metadata = new JObject { ["i"] = 1 } },
                new VectorPoint { Id = "batch-2", Vector = new float[] { 0f, 0f, 1f, 0f }, Metadata = new JObject { ["i"] = 2 } }
            };
            await service.UpsertBatchAsync(col, points);

            var query      = new float[] { 1f, 0f, 0f, 0f };
            var result     = await service.QueryAsync(col, query, topK: 3, includeMetadata: false);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().NotBeEmpty();

            var returnedIds = result.Data.Select(r => r.Id).ToList();
            returnedIds.Should().Contain("batch-0",
                because: "non-UUID IDs must survive a QueryAsync call even when includeMetadata=false");

            foreach (var item in result.Data)
            {
                item.Id.Should().NotMatchRegex(
                    @"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
                    because: $"'{item.Id}' should be the original string ID, not an internal UUID hash");
                item.Metadata.Should().BeNull(because: "includeMetadata was false");
            }
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Edge case: GetAsync after DeleteAsync returns null ────────────────────

    [RetryFact(3, 5000)]
    public async Task GetAsync_AfterDeleteAsync_ShouldReturnNull()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            const string id = "ephemeral-point";
            await service.UpsertAsync(col, new VectorPoint
            {
                Id     = id,
                Vector = new float[] { 1f, 0f, 0f, 0f },
                Metadata = new JObject { ["x"] = 1 }
            });

            // Confirm it exists first
            var before = await service.GetAsync(col, id);
            before.IsSuccessful.Should().BeTrue();
            before.Data.Should().NotBeNull(because: "point should exist before deletion");

            // Delete it
            var del = await service.DeleteAsync(col, id);
            del.IsSuccessful.Should().BeTrue();

            // Now it must be gone
            var after = await service.GetAsync(col, id);
            after.IsSuccessful.Should().BeTrue();
            after.Data.Should().BeNull(
                because: "GetAsync must return null for a point that has been deleted");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Edge case: deleted point must not appear in QueryAsync results ─────────

    [RetryFact(3, 5000)]
    public async Task QueryAsync_DeletedPoint_ShouldNotAppearInResults()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var vec = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "keep",   Vector = vec, Metadata = new JObject { ["tag"] = "yes" } },
                new VectorPoint { Id = "delete", Vector = vec, Metadata = new JObject { ["tag"] = "no" } }
            ]);

            await service.DeleteAsync(col, "delete");

            var result = await service.QueryAsync(col, vec, topK: 10);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Select(r => r.Id).Should().NotContain("delete",
                because: "a deleted point must not appear in similarity search results");
            result.Data.Select(r => r.Id).Should().Contain("keep",
                because: "non-deleted points must still be returned");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Edge case: FieldExists / FieldNotExists on a point with null Metadata ──

    /// <summary>
    /// A point stored with <c>Metadata = null</c> has no payload at all.
    /// <c>FieldExists</c> must NOT match such points because no field can exist in an absent payload.
    /// </summary>
    [RetryFact(3, 5000)]
    public async Task Filter_FieldExists_OnPointWithNullMetadata_ShouldNotMatch()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "has-field",  Vector = v, Metadata = new JObject { ["tag"] = "present" } },
                new VectorPoint { Id = "null-meta",  Vector = v, Metadata = null }
            ]);

            var filter = service.FieldExists("tag");
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Select(r => r.Id).Should().Contain("has-field");
            result.Data.Select(r => r.Id).Should().NotContain("null-meta",
                because: "a point with no metadata cannot have any field present");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    /// <summary>
    /// A point stored with <c>Metadata = null</c> has no payload.
    /// <c>FieldNotExists</c> must match such points because every field is absent.
    /// </summary>
    [RetryFact(3, 5000)]
    public async Task Filter_FieldNotExists_OnPointWithNullMetadata_ShouldMatch()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col,
            [
                new VectorPoint { Id = "has-field", Vector = v, Metadata = new JObject { ["tag"] = "present" } },
                new VectorPoint { Id = "null-meta", Vector = v, Metadata = null }
            ]);

            var filter = service.FieldNotExists("tag");
            var result = await service.QueryAsync(col, v, topK: 10, filter: filter);

            result.IsSuccessful.Should().BeTrue();
            result.Data.Select(r => r.Id).Should().Contain("null-meta",
                because: "a point with no metadata is treated as if ALL fields are absent");
            result.Data.Select(r => r.Id).Should().NotContain("has-field",
                because: "a point with a 'tag' field present must not match FieldNotExists('tag')");
        }
        finally { await service.DeleteCollectionAsync(col); }
    }

    // ── Concurrency ───────────────────────────────────────────────────────────

    [RetryFact(3, 10000)]
    public async Task Concurrency_ParallelUpserts_SameCollection_ShouldAllSucceed()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            var tasks = Enumerable.Range(0, 50).Select(i => Task.Run(async () =>
            {
                var result = await service.UpsertAsync(col, new VectorPoint
                {
                    Id = $"concurrent-{i}",
                    Vector = RandomVector(),
                    Metadata = new JObject { ["idx"] = i }
                });
                result.IsSuccessful.Should().BeTrue($"upsert {i} should succeed");
            }));

            await Task.WhenAll(tasks);

            var query = await service.QueryAsync(col, RandomVector(), topK: 100);
            query.IsSuccessful.Should().BeTrue();
            query.Data.Should().HaveCount(50);
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    [RetryFact(3, 10000)]
    public async Task Concurrency_ParallelReads_SameCollection_ShouldAllSucceed()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var vec = new float[] { 1f, 0f, 0f, 0f };
            await service.UpsertAsync(col, new VectorPoint
            {
                Id = "shared-point",
                Vector = vec,
                Metadata = new JObject { ["key"] = "value" }
            });

            var tasks = Enumerable.Range(0, 50).Select(_ => Task.Run(async () =>
            {
                var result = await service.GetAsync(col, "shared-point");
                result.IsSuccessful.Should().BeTrue();
                result.Data.Should().NotBeNull();
                result.Data!.Id.Should().Be("shared-point");
                result.Data.Metadata?["key"]?.Value<string>().Should().Be("value");
            }));

            await Task.WhenAll(tasks);
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    [RetryFact(3, 10000)]
    public async Task Concurrency_ParallelQueries_SameCollection_ShouldAllSucceed()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var points = Enumerable.Range(0, 10).Select(i => new VectorPoint
            {
                Id = $"q-{i}",
                Vector = RandomVector(),
                Metadata = new JObject { ["val"] = i }
            }).ToList();
            await service.UpsertBatchAsync(col, points);

            var tasks = Enumerable.Range(0, 30).Select(_ => Task.Run(async () =>
            {
                var result = await service.QueryAsync(col, RandomVector(), topK: 5);
                result.IsSuccessful.Should().BeTrue();
                result.Data.Should().HaveCount(5);
            }));

            await Task.WhenAll(tasks);
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    [RetryFact(3, 15000)]
    public async Task Concurrency_MixedReadWriteDelete_ShouldNotCorruptData()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            for (int i = 0; i < 20; i++)
            {
                await service.UpsertAsync(col, new VectorPoint
                {
                    Id = $"seed-{i}",
                    Vector = RandomVector(),
                    Metadata = new JObject { ["seeded"] = true }
                });
            }

            var tasks = new List<Task>();

            for (int i = 0; i < 20; i++)
            {
                int idx = i;
                tasks.Add(Task.Run(async () =>
                {
                    await service.UpsertAsync(col, new VectorPoint
                    {
                        Id = $"writer-{idx}",
                        Vector = RandomVector(),
                        Metadata = new JObject { ["written"] = true }
                    });
                }));
            }

            for (int i = 0; i < 20; i++)
            {
                int idx = i;
                tasks.Add(Task.Run(async () =>
                {
                    var result = await service.GetAsync(col, $"seed-{idx}");
                    result.IsSuccessful.Should().BeTrue();
                }));
            }

            for (int i = 0; i < 10; i++)
            {
                tasks.Add(Task.Run(async () =>
                {
                    var result = await service.QueryAsync(col, RandomVector(), topK: 10);
                    result.IsSuccessful.Should().BeTrue();
                }));
            }

            for (int i = 10; i < 15; i++)
            {
                int idx = i;
                tasks.Add(Task.Run(async () =>
                {
                    await service.DeleteAsync(col, $"seed-{idx}");
                }));
            }

            await Task.WhenAll(tasks);

            for (int i = 0; i < 10; i++)
            {
                var get = await service.GetAsync(col, $"seed-{i}");
                get.IsSuccessful.Should().BeTrue();
                get.Data.Should().NotBeNull($"seed-{i} should still exist");
            }

            for (int i = 10; i < 15; i++)
            {
                var get = await service.GetAsync(col, $"seed-{i}");
                get.IsSuccessful.Should().BeTrue();
                get.Data.Should().BeNull($"seed-{i} should have been deleted");
            }

            for (int i = 0; i < 20; i++)
            {
                var get = await service.GetAsync(col, $"writer-{i}");
                get.IsSuccessful.Should().BeTrue();
                get.Data.Should().NotBeNull($"writer-{i} should exist");
            }
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    [RetryFact(3, 10000)]
    public async Task Concurrency_ParallelUpsertsToSamePoint_LastWriteWins()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            var tasks = Enumerable.Range(0, 20).Select(i => Task.Run(async () =>
            {
                await service.UpsertAsync(col, new VectorPoint
                {
                    Id = "contested-point",
                    Vector = new float[] { i, 0f, 0f, 0f },
                    Metadata = new JObject { ["version"] = i }
                });
            }));

            await Task.WhenAll(tasks);

            var result = await service.GetAsync(col, "contested-point");
            result.IsSuccessful.Should().BeTrue();
            result.Data.Should().NotBeNull();
            result.Data!.Metadata?["version"]?.Type.Should().Be(JTokenType.Integer);
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    [RetryFact(3, 10000)]
    public async Task Concurrency_ParallelCollectionCreation_ShouldNotConflict()
    {
        await using var service = CreateVectorService();
        var collections = Enumerable.Range(0, 10)
            .Select(i => $"par-col-{i}-{Guid.NewGuid():N}")
            .ToList();

        try
        {
            var tasks = collections.Select(c => Task.Run(async () =>
            {
                var result = await service.EnsureCollectionExistsAsync(c, 4, VectorDistanceMetric.Cosine);
                result.IsSuccessful.Should().BeTrue();
            }));

            await Task.WhenAll(tasks);

            var names = await service.GetCollectionNamesAsync();
            names.IsSuccessful.Should().BeTrue();
            foreach (var c in collections)
                names.Data.Should().Contain(c);
        }
        finally
        {
            foreach (var c in collections)
                await service.DeleteCollectionAsync(c);
        }
    }

    // ── Collection isolation ──────────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task Isolation_DeleteFromOneCollectionDoesNotAffectAnother()
    {
        await using var service = CreateVectorService();
        string col1 = UniqueCollection() + "a";
        string col2 = UniqueCollection() + "b";

        try
        {
            await service.EnsureCollectionExistsAsync(col1, 4, VectorDistanceMetric.Cosine);
            await service.EnsureCollectionExistsAsync(col2, 4, VectorDistanceMetric.Cosine);

            var vec = RandomVector();
            await service.UpsertAsync(col1, new VectorPoint { Id = "shared-id", Vector = vec });
            await service.UpsertAsync(col2, new VectorPoint { Id = "shared-id", Vector = vec });

            await service.DeleteAsync(col1, "shared-id");

            var get = await service.GetAsync(col2, "shared-id");
            get.Data.Should().NotBeNull();
        }
        finally
        {
            await service.DeleteCollectionAsync(col1);
            await service.DeleteCollectionAsync(col2);
        }
    }

    [RetryFact(3, 5000)]
    public async Task Isolation_DeleteCollectionDoesNotAffectOther()
    {
        await using var service = CreateVectorService();
        string col1 = UniqueCollection() + "a";
        string col2 = UniqueCollection() + "b";

        try
        {
            await service.EnsureCollectionExistsAsync(col1, 4, VectorDistanceMetric.Cosine);
            await service.EnsureCollectionExistsAsync(col2, 4, VectorDistanceMetric.Cosine);
            await service.UpsertAsync(col2, new VectorPoint { Id = "survivor", Vector = RandomVector() });

            await service.DeleteCollectionAsync(col1);

            var get = await service.GetAsync(col2, "survivor");
            get.Data.Should().NotBeNull();
        }
        finally
        {
            await service.DeleteCollectionAsync(col2);
        }
    }

    [RetryFact(3, 5000)]
    public async Task Isolation_DifferentDimensionCollections_ShouldCoexist()
    {
        await using var service = CreateVectorService();
        string col4 = UniqueCollection() + "a";
        string col8 = UniqueCollection() + "b";

        try
        {
            await service.EnsureCollectionExistsAsync(col4, 4, VectorDistanceMetric.Cosine);
            await service.EnsureCollectionExistsAsync(col8, 8, VectorDistanceMetric.Euclidean);

            await service.UpsertAsync(col4, new VectorPoint { Id = "p4", Vector = RandomVector(4) });
            await service.UpsertAsync(col8, new VectorPoint { Id = "p8", Vector = RandomVector(8) });

            var get4 = await service.GetAsync(col4, "p4");
            get4.Data!.Vector.Should().HaveCount(4);

            var get8 = await service.GetAsync(col8, "p8");
            get8.Data!.Vector.Should().HaveCount(8);

            var bad = await service.UpsertAsync(col4, new VectorPoint { Id = "wrong", Vector = RandomVector(8) });
            bad.IsSuccessful.Should().BeFalse();
        }
        finally
        {
            await service.DeleteCollectionAsync(col4);
            await service.DeleteCollectionAsync(col8);
        }
    }

    // ── Recreate after delete ─────────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task RecreateCollection_AfterDelete_ShouldBeEmpty()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            await service.UpsertAsync(col, new VectorPoint
            {
                Id = "old-data", Vector = RandomVector(), Metadata = new JObject { ["old"] = true }
            });

            await service.DeleteCollectionAsync(col);
            var created = await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            created.Data.Should().BeTrue("should be freshly created");

            var get = await service.GetAsync(col, "old-data");
            get.IsSuccessful.Should().BeTrue();
            get.Data.Should().BeNull("old data should not survive collection delete+recreate");

            var query = await service.QueryAsync(col, RandomVector(), topK: 10);
            query.Data.Should().BeEmpty();
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    [RetryFact(3, 5000)]
    public async Task RecreateCollection_WithDifferentDimensions_ShouldWork()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            await service.UpsertAsync(col, new VectorPoint { Id = "4d", Vector = RandomVector(4) });

            await service.DeleteCollectionAsync(col);
            await service.EnsureCollectionExistsAsync(col, 8, VectorDistanceMetric.Euclidean);

            var bad = await service.UpsertAsync(col, new VectorPoint { Id = "4d", Vector = RandomVector(4) });
            bad.IsSuccessful.Should().BeFalse();

            var good = await service.UpsertAsync(col, new VectorPoint { Id = "8d", Vector = RandomVector(8) });
            good.IsSuccessful.Should().BeTrue();
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    // ── Metadata edge cases ───────────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task Metadata_DeeplyNestedJObject_ShouldRoundTrip()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            var metadata = new JObject
            {
                ["level1"] = new JObject
                {
                    ["level2"] = new JObject
                    {
                        ["level3"] = new JObject
                        {
                            ["value"] = "deep"
                        }
                    }
                },
                ["tags"] = new JArray("a", "b", "c"),
                ["count"] = 42
            };

            await service.UpsertAsync(col, new VectorPoint
            {
                Id = "nested", Vector = RandomVector(), Metadata = metadata
            });

            var get = await service.GetAsync(col, "nested");
            get.Data.Should().NotBeNull();
            get.Data!.Metadata?["level1"]?["level2"]?["level3"]?["value"]?.Value<string>()
                .Should().Be("deep");
            get.Data.Metadata?["tags"]?.ToObject<string[]>().Should().BeEquivalentTo(["a", "b", "c"]);
            get.Data.Metadata?["count"]?.Value<int>().Should().Be(42);
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    [RetryFact(3, 5000)]
    public async Task Metadata_LargeMetadata_ShouldRoundTrip()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            var metadata = new JObject();
            for (int i = 0; i < 100; i++)
                metadata[$"field_{i}"] = $"value_{i}_{new string('x', 100)}";

            await service.UpsertAsync(col, new VectorPoint
            {
                Id = "large-meta", Vector = RandomVector(), Metadata = metadata
            });

            var get = await service.GetAsync(col, "large-meta");
            get.Data.Should().NotBeNull();
            get.Data!.Metadata!.Count.Should().Be(100);
            get.Data.Metadata["field_50"]?.Value<string>().Should().StartWith("value_50_");
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    [RetryFact(3, 5000)]
    public async Task Metadata_NullMetadata_ShouldPersistCorrectly()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            await service.UpsertAsync(col, new VectorPoint
            {
                Id = "no-meta", Vector = RandomVector(), Metadata = null
            });

            var get = await service.GetAsync(col, "no-meta");
            get.Data.Should().NotBeNull();
            get.Data!.Metadata.Should().BeNull();
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    // ── Distance metrics ──────────────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task DistanceMetric_Cosine_IdenticalVectors_ShouldScoreOne()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 3, VectorDistanceMetric.Cosine);
            var vec = new float[] { 1f, 0f, 0f };
            await service.UpsertAsync(col, new VectorPoint { Id = "same", Vector = vec });

            var result = await service.QueryAsync(col, vec, topK: 1);
            result.Data[0].Score.Should().BeApproximately(1.0f, 0.001f,
                because: "cosine similarity of identical vectors is 1.0");
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    [RetryFact(3, 5000)]
    public async Task DistanceMetric_Cosine_OrthogonalVectors_ShouldScoreZero()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 3, VectorDistanceMetric.Cosine);
            await service.UpsertAsync(col, new VectorPoint
            {
                Id = "orthogonal", Vector = new float[] { 0f, 1f, 0f }
            });

            var query = new float[] { 1f, 0f, 0f };
            var result = await service.QueryAsync(col, query, topK: 1);
            result.Data[0].Score.Should().BeApproximately(0.0f, 0.001f,
                because: "cosine similarity of orthogonal vectors is 0.0");
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    [RetryFact(3, 5000)]
    public async Task DistanceMetric_DotProduct_KnownValues_ShouldBeCorrect()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 3, VectorDistanceMetric.DotProduct);
            await service.UpsertAsync(col, new VectorPoint
            {
                Id = "dp", Vector = new float[] { 2f, 3f, 0f }
            });

            var query = new float[] { 1f, 1f, 0f };
            var result = await service.QueryAsync(col, query, topK: 1);
            result.Data[0].Score.Should().BeApproximately(5.0f, 0.001f,
                because: "dot product of [1,1,0]·[2,3,0] = 5");
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    [RetryFact(3, 5000)]
    public async Task DistanceMetric_Euclidean_IdenticalVectors_ShouldScoreZero()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 3, VectorDistanceMetric.Euclidean);
            var vec = new float[] { 1f, 2f, 3f };
            await service.UpsertAsync(col, new VectorPoint { Id = "same", Vector = vec });

            var result = await service.QueryAsync(col, vec, topK: 1);
            result.Data[0].Score.Should().BeApproximately(0.0f, 0.001f,
                because: "euclidean distance of identical vectors is 0 (negated = -0 = 0)");
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    [RetryFact(3, 5000)]
    public async Task DistanceMetric_Euclidean_KnownDistance_ShouldBeCorrect()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 3, VectorDistanceMetric.Euclidean);
            await service.UpsertAsync(col, new VectorPoint
            {
                Id = "far", Vector = new float[] { 3f, 4f, 0f }
            });

            var query = new float[] { 0f, 0f, 0f };
            var result = await service.QueryAsync(col, query, topK: 1);
            MathF.Abs(result.Data[0].Score).Should().BeApproximately(5.0f, 0.001f,
                because: "euclidean distance between [0,0,0] and [3,4,0] is 5 (sign convention varies by provider)");
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    // ── Batch edge cases ──────────────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task UpsertBatch_ThenDeleteSome_ThenQueryShouldReflectChanges()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            var points = Enumerable.Range(0, 10).Select(i => new VectorPoint
            {
                Id = $"batch-{i}",
                Vector = RandomVector(),
                Metadata = new JObject { ["val"] = i }
            }).ToList();
            await service.UpsertBatchAsync(col, points);

            for (int i = 0; i < 5; i++)
                await service.DeleteAsync(col, $"batch-{i}");

            var result = await service.QueryAsync(col, RandomVector(), topK: 20);
            result.Data.Should().HaveCount(5);
            foreach (var r in result.Data)
            {
                var idx = int.Parse(r.Id.Split('-')[1]);
                idx.Should().BeGreaterOrEqualTo(5);
            }
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    [RetryFact(3, 5000)]
    public async Task UpsertBatch_WithMixedDimensions_ShouldFailAtomically()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            var points = new List<VectorPoint>
            {
                new() { Id = "good-1", Vector = RandomVector(4) },
                new() { Id = "bad", Vector = RandomVector(8) },
                new() { Id = "good-2", Vector = RandomVector(4) }
            };

            var result = await service.UpsertBatchAsync(col, points);
            result.IsSuccessful.Should().BeFalse("batch should fail when any point has wrong dimensions");
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    // ── Dispose ───────────────────────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task IsInitialized_AfterDispose_ShouldReturnFalse()
    {
        var service = CreateVectorService();
        service.IsInitialized.Should().BeTrue();

        await service.DisposeAsync();
        service.IsInitialized.Should().BeFalse();
    }

    // ── Query edge cases ──────────────────────────────────────────────────────

    [RetryFact(3, 5000)]
    public async Task Query_WithFilterExcludingAll_ThenWithoutFilter_ShouldReturnResults()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col, [
                new VectorPoint { Id = "a", Vector = v, Metadata = new JObject { ["status"] = "active" } },
                new VectorPoint { Id = "b", Vector = v, Metadata = new JObject { ["status"] = "active" } }
            ]);

            var noMatch = service.FieldEquals("status", new Primitive("deleted"));
            var empty = await service.QueryAsync(col, v, topK: 10, filter: noMatch);
            empty.Data.Should().BeEmpty();

            var all = await service.QueryAsync(col, v, topK: 10);
            all.Data.Should().HaveCount(2);
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    [RetryFact(3, 5000)]
    public async Task Query_FilterOnMixedMetadataTypes_ShouldHandleCorrectly()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);
            var v = new float[] { 1f, 0f, 0f, 0f };

            await service.UpsertBatchAsync(col, [
                new VectorPoint { Id = "str", Vector = v, Metadata = new JObject { ["field"] = "text" } },
                new VectorPoint { Id = "int", Vector = v, Metadata = new JObject { ["field"] = 42 } },
                new VectorPoint { Id = "bool", Vector = v, Metadata = new JObject { ["field"] = true } },
                new VectorPoint { Id = "dbl", Vector = v, Metadata = new JObject { ["field"] = 3.14 } }
            ]);

            var strFilter = service.FieldEquals("field", new Primitive("text"));
            var strResult = await service.QueryAsync(col, v, topK: 10, filter: strFilter);
            strResult.Data.Should().HaveCount(1);
            strResult.Data[0].Id.Should().Be("str");

            var boolFilter = service.FieldEquals("field", new Primitive(true));
            var boolResult = await service.QueryAsync(col, v, topK: 10, filter: boolFilter);
            boolResult.Data.Should().HaveCount(1);
            boolResult.Data[0].Id.Should().Be("bool");
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }

    [RetryFact(3, 5000)]
    public async Task UpsertAsync_UpdateVector_QueryShouldReflectNewVector()
    {
        await using var service = CreateVectorService();
        string col = UniqueCollection();

        try
        {
            await service.EnsureCollectionExistsAsync(col, 4, VectorDistanceMetric.Cosine);

            await service.UpsertAsync(col, new VectorPoint
            {
                Id = "movable",
                Vector = new float[] { 1f, 0f, 0f, 0f },
                Metadata = new JObject { ["label"] = "original" }
            });

            await service.UpsertAsync(col, new VectorPoint
            {
                Id = "movable",
                Vector = new float[] { 0f, 0f, 0f, 1f },
                Metadata = new JObject { ["label"] = "updated" }
            });

            var oldDir = await service.QueryAsync(col, new float[] { 1f, 0f, 0f, 0f }, topK: 1);
            oldDir.Data[0].Metadata?["label"]?.Value<string>().Should().Be("updated");

            var newDirScore = (await service.QueryAsync(col, new float[] { 0f, 0f, 0f, 1f }, topK: 1)).Data[0].Score;
            var oldDirScore = (await service.QueryAsync(col, new float[] { 1f, 0f, 0f, 0f }, topK: 1)).Data[0].Score;
            newDirScore.Should().BeGreaterThan(oldDirScore,
                because: "updated vector should be closer to the new direction");
        }
        finally
        {
            await service.DeleteCollectionAsync(col);
        }
    }
}
