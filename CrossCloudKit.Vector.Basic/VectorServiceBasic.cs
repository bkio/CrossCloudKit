// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Net;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Interfaces.Enums;
using CrossCloudKit.Interfaces.Records;
using CrossCloudKit.Utilities.Common;
using Newtonsoft.Json.Linq;

namespace CrossCloudKit.Vector.Basic;

/// <summary>
/// In-memory <see cref="IVectorService"/> implementation.
/// All data lives in RAM and is lost when the process exits.
/// Intended for unit tests, offline development, and prototyping.
/// </summary>
/// <example>
/// <code>
/// IVectorService vs = new VectorServiceBasic();
/// await vs.EnsureCollectionExistsAsync("test", 384, VectorDistanceMetric.Cosine);
/// </code>
/// </example>
public sealed class VectorServiceBasic : IVectorService
{
    // ── Internal state ────────────────────────────────────────────────────────

    private sealed record CollectionEntry(
        int VectorDimensions,
        VectorDistanceMetric DistanceMetric,
        ConcurrentDictionary<string, VectorPoint> Points);

    private readonly ConcurrentDictionary<string, CollectionEntry> _collections = new();

    // ── IsInitialized ─────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public bool IsInitialized => true;

    // ── Condition builders ────────────────────────────────────────────────────

    /// <inheritdoc/>
    public Condition FieldExists(string fieldName)
        => new ExistenceCondition(ConditionType.AttributeExists, fieldName);

    /// <inheritdoc/>
    public Condition FieldNotExists(string fieldName)
        => new ExistenceCondition(ConditionType.AttributeNotExists, fieldName);

    /// <inheritdoc/>
    public Condition FieldEquals(string fieldName, Primitive value)
        => new ValueCondition(ConditionType.AttributeEquals, fieldName, value);

    /// <inheritdoc/>
    public Condition FieldNotEquals(string fieldName, Primitive value)
        => new ValueCondition(ConditionType.AttributeNotEquals, fieldName, value);

    /// <inheritdoc/>
    public Condition FieldGreaterThan(string fieldName, Primitive value)
        => new ValueCondition(ConditionType.AttributeGreater, fieldName, value);

    /// <inheritdoc/>
    public Condition FieldGreaterThanOrEqual(string fieldName, Primitive value)
        => new ValueCondition(ConditionType.AttributeGreaterOrEqual, fieldName, value);

    /// <inheritdoc/>
    public Condition FieldLessThan(string fieldName, Primitive value)
        => new ValueCondition(ConditionType.AttributeLess, fieldName, value);

    /// <inheritdoc/>
    public Condition FieldLessThanOrEqual(string fieldName, Primitive value)
        => new ValueCondition(ConditionType.AttributeLessOrEqual, fieldName, value);

    // ── Collection management ─────────────────────────────────────────────────

    /// <inheritdoc/>
    public Task<OperationResult<bool>> EnsureCollectionExistsAsync(
        string collectionName,
        int vectorDimensions,
        VectorDistanceMetric distanceMetric,
        CancellationToken cancellationToken = default)
    {
        var created = false;
        _collections.GetOrAdd(collectionName, _ =>
        {
            created = true;
            return new CollectionEntry(vectorDimensions, distanceMetric, new ConcurrentDictionary<string, VectorPoint>());
        });
        return Task.FromResult(OperationResult<bool>.Success(created));
    }

    /// <inheritdoc/>
    public Task<OperationResult<bool>> DeleteCollectionAsync(
        string collectionName,
        CancellationToken cancellationToken = default)
    {
        if (!_collections.TryRemove(collectionName, out _))
            return Task.FromResult(OperationResult<bool>.Failure(
                $"Collection '{collectionName}' not found.", HttpStatusCode.NotFound));

        return Task.FromResult(OperationResult<bool>.Success(true));
    }

    /// <inheritdoc/>
    public Task<OperationResult<IReadOnlyList<string>>> GetCollectionNamesAsync(
        CancellationToken cancellationToken = default)
    {
        IReadOnlyList<string> names = _collections.Keys.ToList();
        return Task.FromResult(OperationResult<IReadOnlyList<string>>.Success(names));
    }

    // ── Point operations ──────────────────────────────────────────────────────

    /// <inheritdoc/>
    public Task<OperationResult<bool>> UpsertAsync(
        string collectionName,
        VectorPoint point,
        CancellationToken cancellationToken = default)
    {
        if (!_collections.TryGetValue(collectionName, out var collection))
            return Task.FromResult(OperationResult<bool>.Failure(
                $"Collection '{collectionName}' not found.", HttpStatusCode.NotFound));

        if (point.Vector.Length != collection.VectorDimensions)
            return Task.FromResult(OperationResult<bool>.Failure(
                $"Vector dimension mismatch: expected {collection.VectorDimensions}, got {point.Vector.Length}.",
                HttpStatusCode.BadRequest));

        collection.Points[point.Id] = point;
        return Task.FromResult(OperationResult<bool>.Success(true));
    }

    /// <inheritdoc/>
    public Task<OperationResult<bool>> UpsertBatchAsync(
        string collectionName,
        IReadOnlyList<VectorPoint> points,
        CancellationToken cancellationToken = default)
    {
        if (!_collections.TryGetValue(collectionName, out var collection))
            return Task.FromResult(OperationResult<bool>.Failure(
                $"Collection '{collectionName}' not found.", HttpStatusCode.NotFound));

        foreach (var point in points)
        {
            if (point.Vector.Length != collection.VectorDimensions)
                return Task.FromResult(OperationResult<bool>.Failure(
                    $"Vector dimension mismatch for point '{point.Id}': expected {collection.VectorDimensions}, got {point.Vector.Length}.",
                    HttpStatusCode.BadRequest));
        }

        foreach (var point in points)
            collection.Points[point.Id] = point;

        return Task.FromResult(OperationResult<bool>.Success(true));
    }

    /// <inheritdoc/>
    public Task<OperationResult<bool>> DeleteAsync(
        string collectionName,
        string id,
        CancellationToken cancellationToken = default)
    {
        if (!_collections.TryGetValue(collectionName, out var collection))
            return Task.FromResult(OperationResult<bool>.Failure(
                $"Collection '{collectionName}' not found.", HttpStatusCode.NotFound));

        if (!collection.Points.TryRemove(id, out _))
            return Task.FromResult(OperationResult<bool>.Failure(
                $"Point '{id}' not found.", HttpStatusCode.NotFound));

        return Task.FromResult(OperationResult<bool>.Success(true));
    }

    /// <inheritdoc/>
    public Task<OperationResult<IReadOnlyList<VectorSearchResult>>> QueryAsync(
        string collectionName,
        float[] queryVector,
        int topK,
        ConditionCoupling? filter = null,
        bool includeMetadata = true,
        CancellationToken cancellationToken = default)
    {
        if (!_collections.TryGetValue(collectionName, out var collection))
            return Task.FromResult(OperationResult<IReadOnlyList<VectorSearchResult>>.Failure(
                $"Collection '{collectionName}' not found.", HttpStatusCode.NotFound));

        if (queryVector.Length != collection.VectorDimensions)
            return Task.FromResult(OperationResult<IReadOnlyList<VectorSearchResult>>.Failure(
                $"Query vector dimension mismatch: expected {collection.VectorDimensions}, got {queryVector.Length}.",
                HttpStatusCode.BadRequest));

        var results = collection.Points.Values
            .Where(p => filter is null || MatchesCoupling(p.Metadata, filter))
            .Select(p => new VectorSearchResult
            {
                Id       = p.Id,
                Score    = ComputeScore(queryVector, p.Vector, collection.DistanceMetric),
                Metadata = includeMetadata ? p.Metadata : null
            })
            .OrderByDescending(r => r.Score)
            .Take(topK)
            .ToList();

        return Task.FromResult(
            OperationResult<IReadOnlyList<VectorSearchResult>>.Success(results));
    }

    /// <inheritdoc/>
    public Task<OperationResult<VectorPoint?>> GetAsync(
        string collectionName,
        string id,
        bool includeVector = true,
        bool includeMetadata = true,
        CancellationToken cancellationToken = default)
    {
        if (!_collections.TryGetValue(collectionName, out var collection))
            return Task.FromResult(OperationResult<VectorPoint?>.Failure(
                $"Collection '{collectionName}' not found.", HttpStatusCode.NotFound));

        if (!collection.Points.TryGetValue(id, out var point))
            return Task.FromResult(OperationResult<VectorPoint?>.Success(null));

        var result = point with
        {
            Vector   = includeVector   ? point.Vector   : [],
            Metadata = includeMetadata ? point.Metadata : null
        };

        return Task.FromResult(OperationResult<VectorPoint?>.Success(result));
    }

    // ── IAsyncDisposable ──────────────────────────────────────────────────────

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        _collections.Clear();
        return ValueTask.CompletedTask;
    }

    // ── Similarity computation ────────────────────────────────────────────────

    private static float ComputeScore(float[] query, float[] stored, VectorDistanceMetric metric)
    {
        return metric switch
        {
            VectorDistanceMetric.Cosine     => CosineSimilarity(query, stored),
            VectorDistanceMetric.DotProduct => DotProduct(query, stored),
            VectorDistanceMetric.Euclidean  => -EuclideanDistance(query, stored), // negate so higher = better
            _                               => CosineSimilarity(query, stored)
        };
    }

    private static float CosineSimilarity(float[] a, float[] b)
    {
        var len = Math.Min(a.Length, b.Length);
        float dot = 0, magA = 0, magB = 0;
        for (int i = 0; i < len; i++)
        {
            dot  += a[i] * b[i];
            magA += a[i] * a[i];
            magB += b[i] * b[i];
        }
        var denom = MathF.Sqrt(magA) * MathF.Sqrt(magB);
        return denom == 0 ? 0 : dot / denom;
    }

    private static float DotProduct(float[] a, float[] b)
    {
        var len = Math.Min(a.Length, b.Length);
        float dot = 0;
        for (int i = 0; i < len; i++)
            dot += a[i] * b[i];
        return dot;
    }

    private static float EuclideanDistance(float[] a, float[] b)
    {
        var len = Math.Min(a.Length, b.Length);
        float sum = 0;
        for (int i = 0; i < len; i++)
        {
            var diff = a[i] - b[i];
            sum += diff * diff;
        }
        return MathF.Sqrt(sum);
    }

    // ── Condition evaluation ──────────────────────────────────────────────────

    private static bool MatchesCoupling(JObject? metadata, ConditionCoupling coupling)
    {
        return coupling.CouplingType switch
        {
            ConditionCouplingType.Empty  => true,
            ConditionCouplingType.Single => coupling.SingleCondition is not null &&
                                           MatchesCondition(metadata, coupling.SingleCondition),
            ConditionCouplingType.And    => coupling.First is not null &&
                                           coupling.Second is not null &&
                                           MatchesCoupling(metadata, coupling.First) &&
                                           MatchesCoupling(metadata, coupling.Second),
            ConditionCouplingType.Or     => coupling.First is not null &&
                                           coupling.Second is not null &&
                                           (MatchesCoupling(metadata, coupling.First) ||
                                            MatchesCoupling(metadata, coupling.Second)),
            _                            => true
        };
    }

    private static bool MatchesCondition(JObject? metadata, Condition condition)
    {
        var field = condition.AttributeName;

        switch (condition.ConditionType)
        {
            case ConditionType.AttributeExists:
                return metadata?[field] is not null;

            case ConditionType.AttributeNotExists:
                return metadata?[field] is null;

            default:
            {
                if (condition is not ValueCondition vc)
                    return false;

                var token = metadata?[field];
                if (token is null)
                {
                    // Field is missing. A missing field is definitionally not equal to any value,
                    // but it is not greater/less/etc. than anything either.
                    return condition.ConditionType == ConditionType.AttributeNotEquals;
                }

                // Numeric comparisons require numeric types on both sides.
                bool isNumericToken = token.Type is JTokenType.Integer or JTokenType.Float;
                bool isNumericPrimitive = vc.Value.Kind is PrimitiveKind.Integer or PrimitiveKind.Double;

                return condition.ConditionType switch
                {
                    ConditionType.AttributeEquals           => PrimitiveTokenEquals(vc.Value, token),
                    ConditionType.AttributeNotEquals        => !PrimitiveTokenEquals(vc.Value, token),
                    ConditionType.AttributeGreater          => isNumericToken && isNumericPrimitive && CompareNumeric(vc.Value, token) > 0,
                    ConditionType.AttributeGreaterOrEqual   => isNumericToken && isNumericPrimitive && CompareNumeric(vc.Value, token) >= 0,
                    ConditionType.AttributeLess             => isNumericToken && isNumericPrimitive && CompareNumeric(vc.Value, token) < 0,
                    ConditionType.AttributeLessOrEqual      => isNumericToken && isNumericPrimitive && CompareNumeric(vc.Value, token) <= 0,
                    _                                       => false
                };
            }
        }
    }

    private static bool PrimitiveTokenEquals(Primitive p, JToken token)
    {
        return p.Kind switch
        {
            PrimitiveKind.String  => token.Type == JTokenType.String && token.Value<string>() == p.AsString,
            PrimitiveKind.Integer => token.Type switch
            {
                JTokenType.Integer => token.Value<long>() == p.AsInteger,
                JTokenType.Float   => Math.Abs(token.Value<double>() - (double)p.AsInteger) < 1e-9,
                _                  => false
            },
            PrimitiveKind.Double  => token.Type switch
            {
                JTokenType.Float   => Math.Abs(token.Value<double>() - p.AsDouble) < 1e-9,
                JTokenType.Integer => Math.Abs((double)token.Value<long>() - p.AsDouble) < 1e-9,
                _                  => false
            },
            PrimitiveKind.Boolean => token.Type == JTokenType.Boolean && token.Value<bool>() == p.AsBoolean,
            _                     => false
        };
    }

    private static int CompareNumeric(Primitive p, JToken token)
    {
        double tokenVal = token.Type is JTokenType.Integer or JTokenType.Float
            ? token.Value<double>()
            : 0;

        double primVal = p.Kind switch
        {
            PrimitiveKind.Integer => (double)p.AsInteger,
            PrimitiveKind.Double  => p.AsDouble,
            _                     => 0
        };

        // Compares token against primitive: positive = token > primitive
        return tokenVal.CompareTo(primVal);
    }
}
