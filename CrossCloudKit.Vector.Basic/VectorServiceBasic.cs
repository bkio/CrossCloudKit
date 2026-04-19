// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;
using System.Text;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Interfaces.Enums;
using CrossCloudKit.Interfaces.Records;
using CrossCloudKit.Utilities.Common;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CrossCloudKit.Vector.Basic;

/// <summary>
/// Cross-process file-based <see cref="IVectorService"/> implementation.
/// Data is persisted to local JSON files and protected by OS-level named mutexes,
/// enabling safe concurrent access from multiple threads and processes on the same machine.
/// Intended for development, testing, and lightweight workloads.
/// </summary>
/// <example>
/// <code>
/// await using IVectorService vs = new VectorServiceBasic();
/// await vs.EnsureCollectionExistsAsync("test", 384, VectorDistanceMetric.Cosine);
/// </code>
/// </example>
public sealed class VectorServiceBasic : IVectorService
{
    // ── Internal state ────────────────────────────────────────────────────────

    private readonly string _storageDirectory;
    private bool _disposed;

    private const string RootFolderName = "CrossCloudKit.Vector.Basic";
    private const string MetaFileName = "_meta.json";

    private sealed record CollectionMeta(int VectorDimensions, VectorDistanceMetric DistanceMetric);

    /// <summary>
    /// Creates a new file-based vector service.
    /// </summary>
    /// <param name="basePath">Root directory for storage. Defaults to <see cref="Path.GetTempPath"/>.</param>
    public VectorServiceBasic(string? basePath = null)
    {
        basePath ??= Path.GetTempPath();
        _storageDirectory = Path.Combine(basePath, RootFolderName);
        if (!Directory.Exists(_storageDirectory))
            Directory.CreateDirectory(_storageDirectory);
    }

    // ── IsInitialized ─────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public bool IsInitialized => !_disposed;

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
        try
        {
            using var mutex = AcquireCollectionMutex(collectionName);

            var collectionDir = GetCollectionPath(collectionName);
            var metaPath = Path.Combine(collectionDir, MetaFileName);

            if (Directory.Exists(collectionDir) && File.Exists(metaPath))
                return Task.FromResult(OperationResult<bool>.Success(false));

            Directory.CreateDirectory(collectionDir);
            var meta = new CollectionMeta(vectorDimensions, distanceMetric);
            var json = JsonConvert.SerializeObject(meta, Formatting.None);
            FileSystemUtilities.WriteToFileEnsureWrittenToDisk(json, metaPath);

            return Task.FromResult(OperationResult<bool>.Success(true));
        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<bool>.Failure(
                $"Failed to create collection: {ex.Message}", HttpStatusCode.InternalServerError));
        }
    }

    /// <inheritdoc/>
    public Task<OperationResult<bool>> DeleteCollectionAsync(
        string collectionName,
        CancellationToken cancellationToken = default)
    {
        try
        {
            using var mutex = AcquireCollectionMutex(collectionName);

            var collectionDir = GetCollectionPath(collectionName);
            if (!Directory.Exists(collectionDir))
                return Task.FromResult(OperationResult<bool>.Failure(
                    $"Collection '{collectionName}' not found.", HttpStatusCode.NotFound));

            Directory.Delete(collectionDir, true);

            return Task.FromResult(OperationResult<bool>.Success(true));
        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<bool>.Failure(
                $"Failed to delete collection: {ex.Message}", HttpStatusCode.InternalServerError));
        }
    }

    /// <inheritdoc/>
    public Task<OperationResult<IReadOnlyList<string>>> GetCollectionNamesAsync(
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (!Directory.Exists(_storageDirectory))
                return Task.FromResult(
                    OperationResult<IReadOnlyList<string>>.Success(Array.Empty<string>() as IReadOnlyList<string>));

            var names = Directory.GetDirectories(_storageDirectory)
                .Where(d => File.Exists(Path.Combine(d, MetaFileName)))
                .Select(Path.GetFileName)
                .Where(n => n is not null)
                .Cast<string>()
                .ToList();

            return Task.FromResult(
                OperationResult<IReadOnlyList<string>>.Success(names as IReadOnlyList<string>));
        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<IReadOnlyList<string>>.Failure(
                $"Failed to list collections: {ex.Message}", HttpStatusCode.InternalServerError));
        }
    }

    // ── Point operations ──────────────────────────────────────────────────────

    /// <inheritdoc/>
    public Task<OperationResult<bool>> UpsertAsync(
        string collectionName,
        VectorPoint point,
        CancellationToken cancellationToken = default)
    {
        try
        {
            using var mutex = AcquireCollectionMutex(collectionName);

            var meta = ReadCollectionMeta(collectionName);
            if (meta is null)
                return Task.FromResult(OperationResult<bool>.Failure(
                    $"Collection '{collectionName}' not found.", HttpStatusCode.NotFound));

            if (point.Vector.Length != meta.VectorDimensions)
                return Task.FromResult(OperationResult<bool>.Failure(
                    $"Vector dimension mismatch: expected {meta.VectorDimensions}, got {point.Vector.Length}.",
                    HttpStatusCode.BadRequest));

            SavePoint(collectionName, point);
            return Task.FromResult(OperationResult<bool>.Success(true));
        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<bool>.Failure(
                $"Failed to upsert point: {ex.Message}", HttpStatusCode.InternalServerError));
        }
    }

    /// <inheritdoc/>
    public Task<OperationResult<bool>> UpsertBatchAsync(
        string collectionName,
        IReadOnlyList<VectorPoint> points,
        CancellationToken cancellationToken = default)
    {
        try
        {
            using var mutex = AcquireCollectionMutex(collectionName);

            var meta = ReadCollectionMeta(collectionName);
            if (meta is null)
                return Task.FromResult(OperationResult<bool>.Failure(
                    $"Collection '{collectionName}' not found.", HttpStatusCode.NotFound));

            foreach (var point in points)
            {
                if (point.Vector.Length != meta.VectorDimensions)
                    return Task.FromResult(OperationResult<bool>.Failure(
                        $"Vector dimension mismatch for point '{point.Id}': expected {meta.VectorDimensions}, got {point.Vector.Length}.",
                        HttpStatusCode.BadRequest));
            }

            foreach (var point in points)
                SavePoint(collectionName, point);

            return Task.FromResult(OperationResult<bool>.Success(true));
        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<bool>.Failure(
                $"Failed to upsert batch: {ex.Message}", HttpStatusCode.InternalServerError));
        }
    }

    /// <inheritdoc/>
    public Task<OperationResult<bool>> DeleteAsync(
        string collectionName,
        string id,
        CancellationToken cancellationToken = default)
    {
        try
        {
            using var mutex = AcquireCollectionMutex(collectionName);

            var meta = ReadCollectionMeta(collectionName);
            if (meta is null)
                return Task.FromResult(OperationResult<bool>.Failure(
                    $"Collection '{collectionName}' not found.", HttpStatusCode.NotFound));

            var filePath = GetPointFilePath(collectionName, id);
            if (!File.Exists(filePath))
                return Task.FromResult(OperationResult<bool>.Failure(
                    $"Point '{id}' not found.", HttpStatusCode.NotFound));

            File.Delete(filePath);

            return Task.FromResult(OperationResult<bool>.Success(true));
        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<bool>.Failure(
                $"Failed to delete point: {ex.Message}", HttpStatusCode.InternalServerError));
        }
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
        try
        {
            using var mutex = AcquireCollectionMutex(collectionName);

            var meta = ReadCollectionMeta(collectionName);
            if (meta is null)
                return Task.FromResult(OperationResult<IReadOnlyList<VectorSearchResult>>.Failure(
                    $"Collection '{collectionName}' not found.", HttpStatusCode.NotFound));

            if (queryVector.Length != meta.VectorDimensions)
                return Task.FromResult(OperationResult<IReadOnlyList<VectorSearchResult>>.Failure(
                    $"Query vector dimension mismatch: expected {meta.VectorDimensions}, got {queryVector.Length}.",
                    HttpStatusCode.BadRequest));

            var allPoints = ReadAllPoints(collectionName);

            var results = allPoints
                .Where(p => filter is null || MatchesCoupling(p.Metadata, filter))
                .Select(p => new VectorSearchResult
                {
                    Id       = p.Id,
                    Score    = ComputeScore(queryVector, p.Vector, meta.DistanceMetric),
                    Metadata = includeMetadata ? p.Metadata : null
                })
                .OrderByDescending(r => r.Score)
                .Take(topK)
                .ToList();

            return Task.FromResult(
                OperationResult<IReadOnlyList<VectorSearchResult>>.Success(results as IReadOnlyList<VectorSearchResult>));
        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<IReadOnlyList<VectorSearchResult>>.Failure(
                $"Failed to query: {ex.Message}", HttpStatusCode.InternalServerError));
        }
    }

    /// <inheritdoc/>
    public Task<OperationResult<VectorPoint?>> GetAsync(
        string collectionName,
        string id,
        bool includeVector = true,
        bool includeMetadata = true,
        CancellationToken cancellationToken = default)
    {
        try
        {
            using var mutex = AcquireCollectionMutex(collectionName);

            var meta = ReadCollectionMeta(collectionName);
            if (meta is null)
                return Task.FromResult(OperationResult<VectorPoint?>.Failure(
                    $"Collection '{collectionName}' not found.", HttpStatusCode.NotFound));

            var point = ReadPoint(collectionName, id);
            if (point is null)
                return Task.FromResult(OperationResult<VectorPoint?>.Success(null));

            var result = point with
            {
                Vector   = includeVector   ? point.Vector   : [],
                Metadata = includeMetadata ? point.Metadata : null
            };

            return Task.FromResult(OperationResult<VectorPoint?>.Success(result));
        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<VectorPoint?>.Failure(
                $"Failed to get point: {ex.Message}", HttpStatusCode.InternalServerError));
        }
    }

    // ── IAsyncDisposable ──────────────────────────────────────────────────────

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        _disposed = true;
        return ValueTask.CompletedTask;
    }

    // ── File I/O helpers ──────────────────────────────────────────────────────

    private string GetCollectionPath(string collectionName)
        => Path.Combine(_storageDirectory, collectionName.MakeValidFileName());

    private string GetPointFilePath(string collectionName, string pointId)
    {
        var encoded = EncodingUtilities.Base64EncodeNoPadding(pointId);
        return Path.Combine(GetCollectionPath(collectionName), $"{encoded}.json");
    }

    private CollectionMeta? ReadCollectionMeta(string collectionName)
    {
        var metaPath = Path.Combine(GetCollectionPath(collectionName), MetaFileName);
        if (!File.Exists(metaPath))
            return null;

        try
        {
            var json = File.ReadAllText(metaPath, Encoding.UTF8);
            return JsonConvert.DeserializeObject<CollectionMeta>(json);
        }
        catch
        {
            return null;
        }
    }

    private void SavePoint(string collectionName, VectorPoint point)
    {
        var filePath = GetPointFilePath(collectionName, point.Id);
        var json = JsonConvert.SerializeObject(point, Formatting.None);
        FileSystemUtilities.WriteToFileEnsureWrittenToDisk(json, filePath);
    }

    private VectorPoint? ReadPoint(string collectionName, string pointId)
    {
        var filePath = GetPointFilePath(collectionName, pointId);
        if (!File.Exists(filePath))
            return null;

        try
        {
            var json = File.ReadAllText(filePath, Encoding.UTF8);
            return JsonConvert.DeserializeObject<VectorPoint>(json);
        }
        catch
        {
            return null;
        }
    }

    private List<VectorPoint> ReadAllPoints(string collectionName)
    {
        var collectionDir = GetCollectionPath(collectionName);
        if (!Directory.Exists(collectionDir))
            return [];

        var points = new List<VectorPoint>();
        foreach (var file in Directory.GetFiles(collectionDir, "*.json"))
        {
            if (Path.GetFileName(file) == MetaFileName)
                continue;

            try
            {
                var json = File.ReadAllText(file, Encoding.UTF8);
                var point = JsonConvert.DeserializeObject<VectorPoint>(json);
                if (point is not null)
                    points.Add(point);
            }
            catch
            {
                // Skip corrupted files
            }
        }
        return points;
    }

    // ── Cross-process mutex ───────────────────────────────────────────────────

    private static AutoMutex AcquireCollectionMutex(string collectionName)
    {
        var mutexName = "CrossCloudKit.Vector.Basic." + Convert.ToBase64String(
                Encoding.UTF8.GetBytes(collectionName))
            .Replace('/', '_').Replace('+', '-').Replace("=", "");

        return new AutoMutex(mutexName);
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
