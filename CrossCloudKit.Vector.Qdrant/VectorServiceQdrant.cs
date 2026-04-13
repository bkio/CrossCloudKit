// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Net;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Interfaces.Enums;
using CrossCloudKit.Interfaces.Records;
using CrossCloudKit.Utilities.Common;
using Newtonsoft.Json.Linq;
using Qdrant.Client;
using Qdrant.Client.Grpc;
// Alias to resolve 'Condition' clash between Qdrant.Client.Grpc.Condition
// and CrossCloudKit.Interfaces.Classes.Condition.
using SvcCondition = CrossCloudKit.Interfaces.Classes.Condition;
using QdrantCondition = Qdrant.Client.Grpc.Condition;
// Alias to resolve 'Range' clash between Qdrant.Client.Grpc.Range and System.Range.
using QdrantRange = Qdrant.Client.Grpc.Range;

namespace CrossCloudKit.Vector.Qdrant;

/// <summary>
/// <see cref="IVectorService"/> implementation backed by a Qdrant cluster.
/// Uses the official Qdrant .NET gRPC client (<c>Qdrant.Client</c>).
/// </summary>
/// <example>
/// <code>
/// IVectorService vs = new VectorServiceQdrant(host: "localhost", grpcPort: 6334);
/// await vs.EnsureCollectionExistsAsync("docs", 1536, VectorDistanceMetric.Cosine);
/// </code>
/// </example>
public sealed class VectorServiceQdrant : IVectorService
{
    // Reserved payload key for storing the original string ID when it is not a UUID.
    // This enables round-tripping non-UUID IDs like "batch-0" through Qdrant.
    private const string OriginalIdPayloadKey = "__cck_original_id";

    // ── Fields ────────────────────────────────────────────────────────────────

    private readonly QdrantClient _client;
    private bool _disposed;

    // ── IsInitialized ─────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public bool IsInitialized { get; }

    // ── Constructor ───────────────────────────────────────────────────────────

    /// <summary>
    /// Initialises a new <see cref="VectorServiceQdrant"/> connected to a Qdrant node via gRPC.
    /// </summary>
    /// <param name="host">Hostname or IP of the Qdrant node.</param>
    /// <param name="grpcPort">gRPC port (default: 6334).</param>
    /// <param name="https">Whether to use HTTPS/TLS for the gRPC channel (default: <c>false</c>).</param>
    /// <param name="apiKey">Optional API key for authenticated Qdrant Cloud clusters.</param>
    public VectorServiceQdrant(
        string host,
        int grpcPort = 6334,
        bool https = false,
        string? apiKey = null)
    {
        try
        {
            _client = new QdrantClient(host, grpcPort, https, apiKey);
            IsInitialized = true;
        }
        catch
        {
            IsInitialized = false;
            throw;
        }
    }

    // ── Condition builders ────────────────────────────────────────────────────

    /// <inheritdoc/>
    public SvcCondition FieldExists(string fieldName)
        => new ExistenceCondition(ConditionType.AttributeExists, fieldName);

    /// <inheritdoc/>
    public SvcCondition FieldNotExists(string fieldName)
        => new ExistenceCondition(ConditionType.AttributeNotExists, fieldName);

    /// <inheritdoc/>
    public SvcCondition FieldEquals(string fieldName, Primitive value)
        => new ValueCondition(ConditionType.AttributeEquals, fieldName, value);

    /// <inheritdoc/>
    public SvcCondition FieldNotEquals(string fieldName, Primitive value)
        => new ValueCondition(ConditionType.AttributeNotEquals, fieldName, value);

    /// <inheritdoc/>
    public SvcCondition FieldGreaterThan(string fieldName, Primitive value)
        => new ValueCondition(ConditionType.AttributeGreater, fieldName, value);

    /// <inheritdoc/>
    public SvcCondition FieldGreaterThanOrEqual(string fieldName, Primitive value)
        => new ValueCondition(ConditionType.AttributeGreaterOrEqual, fieldName, value);

    /// <inheritdoc/>
    public SvcCondition FieldLessThan(string fieldName, Primitive value)
        => new ValueCondition(ConditionType.AttributeLess, fieldName, value);

    /// <inheritdoc/>
    public SvcCondition FieldLessThanOrEqual(string fieldName, Primitive value)
        => new ValueCondition(ConditionType.AttributeLessOrEqual, fieldName, value);

    // ── Collection management ─────────────────────────────────────────────────

    /// <inheritdoc/>
    public async Task<OperationResult<bool>> EnsureCollectionExistsAsync(
        string collectionName,
        int vectorDimensions,
        VectorDistanceMetric distanceMetric,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var exists = await _client.CollectionExistsAsync(collectionName, cancellationToken);
            if (exists)
                return OperationResult<bool>.Success(false);

            await _client.CreateCollectionAsync(
                collectionName,
                new VectorParams
                {
                    Size     = (ulong)vectorDimensions,
                    Distance = MapDistance(distanceMetric)
                },
                cancellationToken: cancellationToken);

            return OperationResult<bool>.Success(true);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure(ex.Message, HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc/>
    public async Task<OperationResult<bool>> DeleteCollectionAsync(
        string collectionName,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await _client.DeleteCollectionAsync(collectionName, cancellationToken: cancellationToken);
            return OperationResult<bool>.Success(true);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure(ex.Message, HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc/>
    public async Task<OperationResult<IReadOnlyList<string>>> GetCollectionNamesAsync(
        CancellationToken cancellationToken = default)
    {
        try
        {
            var collections = await _client.ListCollectionsAsync(cancellationToken);
            IReadOnlyList<string> names = collections.ToList();
            return OperationResult<IReadOnlyList<string>>.Success(names);
        }
        catch (Exception ex)
        {
            return OperationResult<IReadOnlyList<string>>.Failure(ex.Message, HttpStatusCode.InternalServerError);
        }
    }

    // ── Point operations ──────────────────────────────────────────────────────

    /// <inheritdoc/>
    public async Task<OperationResult<bool>> UpsertAsync(
        string collectionName,
        VectorPoint point,
        CancellationToken cancellationToken = default)
    {
        return await UpsertBatchAsync(collectionName, [point], cancellationToken);
    }

    /// <inheritdoc/>
    public async Task<OperationResult<bool>> UpsertBatchAsync(
        string collectionName,
        IReadOnlyList<VectorPoint> points,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var structs = points.Select(p =>
            {
                var pid        = ToPointId(p.Id);
                var isNativeUuid = Guid.TryParse(p.Id, out _);

                var s = new PointStruct
                {
                    Id      = pid,
                    Vectors = p.Vector   // implicit float[] → Vectors conversion
                };

                if (p.Metadata is not null)
                {
                    foreach (var kv in p.Metadata)
                    {
                        if (kv.Value is not null)
                            s.Payload[kv.Key] = ToValue(kv.Value);
                    }
                }

                // Persist the original string ID when it is not already a UUID,
                // so we can reconstruct it on read.
                // This MUST be set after user metadata to prevent a colliding key
                // from overwriting the internal ID tracker.
                if (!isNativeUuid)
                    s.Payload[OriginalIdPayloadKey] = new Value { StringValue = p.Id };

                return s;
            }).ToList();

            await _client.UpsertAsync(collectionName, structs, cancellationToken: cancellationToken);
            return OperationResult<bool>.Success(true);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure(ex.Message, HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc/>
    public async Task<OperationResult<bool>> DeleteAsync(
        string collectionName,
        string id,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var pid = ToPointId(id);
            if (pid.HasUuid)
            {
                await _client.DeleteAsync(
                    collectionName,
                    Guid.Parse(pid.Uuid),
                    cancellationToken: cancellationToken);
            }
            else
            {
                await _client.DeleteAsync(
                    collectionName,
                    pid.Num,
                    cancellationToken: cancellationToken);
            }
            return OperationResult<bool>.Success(true);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure(ex.Message, HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc/>
    public async Task<OperationResult<IReadOnlyList<VectorSearchResult>>> QueryAsync(
        string collectionName,
        float[] queryVector,
        int topK,
        ConditionCoupling? filter = null,
        bool includeMetadata = true,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var qdrantFilter = filter is not null ? BuildFilter(filter) : null;

            var searchResult = await _client.SearchAsync(
                collectionName,
                queryVector,
                filter: qdrantFilter,
                limit: (ulong)topK,
                // Always fetch payload so we can recover non-UUID original IDs from __cck_original_id.
                // The metadata is stripped from results below when includeMetadata=false.
                payloadSelector: true,
                cancellationToken: cancellationToken);

            var results = searchResult.Select(r => new VectorSearchResult
            {
                Id       = ExtractOriginalId(r.Id, r.Payload),
                Score    = r.Score,
                Metadata = includeMetadata ? PayloadToJObject(r.Payload) : null
            }).ToList();

            return OperationResult<IReadOnlyList<VectorSearchResult>>.Success(results);
        }
        catch (Exception ex)
        {
            return OperationResult<IReadOnlyList<VectorSearchResult>>.Failure(
                ex.Message, HttpStatusCode.InternalServerError);
        }
    }

    /// <inheritdoc/>
    public async Task<OperationResult<VectorPoint?>> GetAsync(
        string collectionName,
        string id,
        bool includeVector = true,
        bool includeMetadata = true,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var pid = ToPointId(id);
            IReadOnlyList<RetrievedPoint> retrieved;

            if (pid.HasUuid)
            {
                retrieved = await _client.RetrieveAsync(
                    collectionName,
                    Guid.Parse(pid.Uuid),
                    // Always fetch payload so we can recover non-UUID original IDs from __cck_original_id.
                    // The metadata is stripped from results below when includeMetadata=false.
                    withPayload: true,
                    withVectors: includeVector,
                    cancellationToken: cancellationToken);
            }
            else
            {
                retrieved = await _client.RetrieveAsync(
                    collectionName,
                    pid.Num,
                    withPayload: true,
                    withVectors: includeVector,
                    cancellationToken: cancellationToken);
            }

            var point = retrieved.FirstOrDefault();
            if (point is null)
                return OperationResult<VectorPoint?>.Success(null);

            float[] vector = includeVector
                ? point.Vectors?.Vector?.Data?.ToArray() ?? []
                : [];

            return OperationResult<VectorPoint?>.Success(new VectorPoint
            {
                Id       = ExtractOriginalId(point.Id, point.Payload),
                Vector   = vector,
                Metadata = includeMetadata ? PayloadToJObject(point.Payload) : null
            });
        }
        catch (Exception ex)
        {
            return OperationResult<VectorPoint?>.Failure(ex.Message, HttpStatusCode.InternalServerError);
        }
    }

    // ── IAsyncDisposable ──────────────────────────────────────────────────────

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            _client.Dispose();
        }
        return ValueTask.CompletedTask;
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private static Distance MapDistance(VectorDistanceMetric metric) => metric switch
    {
        VectorDistanceMetric.Cosine     => Distance.Cosine,
        VectorDistanceMetric.Euclidean  => Distance.Euclid,
        VectorDistanceMetric.DotProduct => Distance.Dot,
        _                               => Distance.Cosine
    };

    /// <summary>
    /// Converts a string ID to a Qdrant <see cref="PointId"/>.
    /// UUID-formatted strings become UUID point IDs; others are converted to a
    /// deterministic UUID-v5 (SHA-1 name-based) so that the mapping is stable
    /// across process restarts — <c>string.GetHashCode()</c> is randomised in .NET Core.
    /// </summary>
    private static PointId ToPointId(string id)
    {
        if (Guid.TryParse(id, out var guid))
            return new PointId { Uuid = guid.ToString("D") };

        // Deterministic UUID-v5 derived from the string ID.
        // Uses a fixed namespace GUID so the same string always produces the same UUID.
        var ns = new Guid("6ba7b810-9dad-11d1-80b4-00c04fd430c8"); // RFC 4122 DNS namespace
        var nameBytes = System.Text.Encoding.UTF8.GetBytes(id);
        var nsBytes   = ns.ToByteArray();
        SwapGuidByteOrder(nsBytes);

        byte[] hash;
        using (var sha1 = System.Security.Cryptography.SHA1.Create())
        {
            sha1.TransformBlock(nsBytes, 0, nsBytes.Length, null, 0);
            sha1.TransformFinalBlock(nameBytes, 0, nameBytes.Length);
            hash = sha1.Hash!;
        }

        var result = new byte[16];
        Array.Copy(hash, 0, result, 0, 16);
        result[6] = (byte)((result[6] & 0x0F) | 0x50); // version 5
        result[8] = (byte)((result[8] & 0x3F) | 0x80); // variant RFC 4122
        SwapGuidByteOrder(result);

        return new PointId { Uuid = new Guid(result).ToString("D") };
    }

    /// <summary>
    /// .NET Guid uses mixed-endian layout (first 3 groups little-endian, last 2 big-endian).
    /// This swaps the first 3 groups to/from network byte order for UUID-v5 computation.
    /// </summary>
    private static void SwapGuidByteOrder(byte[] b)
    {
        (b[0], b[3]) = (b[3], b[0]);
        (b[1], b[2]) = (b[2], b[1]);
        (b[4], b[5]) = (b[5], b[4]);
        (b[6], b[7]) = (b[7], b[6]);
    }

    private static string PointIdToString(PointId id) =>
        id.HasUuid ? id.Uuid : id.Num.ToString();

    /// <summary>
    /// Returns the original string ID if one was stored in payload, otherwise falls back
    /// to the Qdrant point ID string representation.
    /// </summary>
    private static string ExtractOriginalId(
        PointId id,
        Google.Protobuf.Collections.MapField<string, Value> payload)
    {
        if (payload.TryGetValue(OriginalIdPayloadKey, out var val)
            && val.KindCase == Value.KindOneofCase.StringValue)
            return val.StringValue;

        return PointIdToString(id);
    }

    private static Value ToValue(JToken token) => token.Type switch
    {
        JTokenType.String  => new Value { StringValue  = token.Value<string>() ?? string.Empty },
        JTokenType.Integer => new Value { IntegerValue = token.Value<long>() },
        JTokenType.Float   => new Value { DoubleValue  = token.Value<double>() },
        JTokenType.Boolean => new Value { BoolValue    = token.Value<bool>() },
        JTokenType.Null    => new Value { NullValue    = NullValue.NullValue },
        _                  => new Value { StringValue  = token.ToString() }
    };

    private static JObject? PayloadToJObject(
        Google.Protobuf.Collections.MapField<string, Value> payload)
    {
        if (payload.Count == 0) return null;

        var obj = new JObject();
        foreach (var kv in payload)
        {
            // Skip the internal round-trip key — it is not user metadata.
            if (kv.Key == OriginalIdPayloadKey) continue;

            obj[kv.Key] = kv.Value.KindCase switch
            {
                Value.KindOneofCase.StringValue  => new JValue(kv.Value.StringValue),
                Value.KindOneofCase.IntegerValue => new JValue(kv.Value.IntegerValue),
                Value.KindOneofCase.DoubleValue  => new JValue(kv.Value.DoubleValue),
                Value.KindOneofCase.BoolValue    => new JValue(kv.Value.BoolValue),
                _                                => JValue.CreateNull()
            };
        }
        return obj.Count > 0 ? obj : null;
    }

    // ── Filter translation ────────────────────────────────────────────────────

    private static Filter? BuildFilter(ConditionCoupling coupling)
    {
        if (coupling.CouplingType == ConditionCouplingType.Empty)
            return null;

        var filter = new Filter();
        ApplyCoupling(filter, coupling);
        return filter;
    }

    private static void ApplyCoupling(Filter target, ConditionCoupling coupling)
    {
        switch (coupling.CouplingType)
        {
            case ConditionCouplingType.Single:
            {
                if (coupling.SingleCondition is not null)
                    TranslateAndAdd(target, coupling.SingleCondition);
                break;
            }

            case ConditionCouplingType.And:
            {
                if (coupling.First is not null && coupling.Second is not null)
                {
                    var left  = new Filter();
                    var right = new Filter();
                    ApplyCoupling(left,  coupling.First);
                    ApplyCoupling(right, coupling.Second);

                    // When both branches produce Should conditions (OR sub-expressions),
                    // flattening them into a single Should list would merge (A OR B) AND (C OR D)
                    // into (A OR B OR C OR D). Instead, wrap each branch as a sub-filter in Must.
                    if (left.Should.Count > 0 && right.Should.Count > 0)
                    {
                        target.Must.Add(new QdrantCondition { Filter = left });
                        target.Must.Add(new QdrantCondition { Filter = right });
                    }
                    else
                    {
                        foreach (var c in left.Must)    target.Must.Add(c);
                        foreach (var c in left.Should)  target.Should.Add(c);
                        foreach (var c in left.MustNot) target.MustNot.Add(c);
                        foreach (var c in right.Must)    target.Must.Add(c);
                        foreach (var c in right.Should)  target.Should.Add(c);
                        foreach (var c in right.MustNot) target.MustNot.Add(c);
                    }
                }
                break;
            }

            case ConditionCouplingType.Or:
            {
                if (coupling.First is not null && coupling.Second is not null)
                {
                    var left  = new Filter();
                    var right = new Filter();
                    ApplyCoupling(left,  coupling.First);
                    ApplyCoupling(right, coupling.Second);

                    if (left.Must.Count > 0 || left.Should.Count > 0 || left.MustNot.Count > 0)
                        target.Should.Add(new QdrantCondition { Filter = left });
                    if (right.Must.Count > 0 || right.Should.Count > 0 || right.MustNot.Count > 0)
                        target.Should.Add(new QdrantCondition { Filter = right });
                }
                break;
            }
        }
    }

    private static void TranslateAndAdd(Filter target, SvcCondition condition)
    {
        var key = condition.AttributeName;

        switch (condition.ConditionType)
        {
            case ConditionType.AttributeExists:
                // IsEmpty in MustNot → "must NOT be empty" == field exists
                target.MustNot.Add(new QdrantCondition
                    { IsEmpty = new IsEmptyCondition { Key = key } });
                break;

            case ConditionType.AttributeNotExists:
                // IsEmpty matches when the field does not exist, is null, or is an empty array.
                // IsNull only matches when the field exists with an explicit null value.
                // We need IsEmpty because FieldNotExists means "field is absent from payload".
                target.Must.Add(new QdrantCondition
                    { IsEmpty = new IsEmptyCondition { Key = key } });
                break;

            case ConditionType.AttributeEquals when condition is ValueCondition vc:
                if (vc.Value.Kind == PrimitiveKind.Double)
                {
                    // Qdrant Match has no double field; approximate equality via Range.
                    var d = vc.Value.AsDouble;
                    target.Must.Add(new QdrantCondition
                        { Field = new FieldCondition { Key = key, Range = new QdrantRange { Gte = d, Lte = d } } });
                }
                else
                {
                    target.Must.Add(new QdrantCondition
                        { Field = new FieldCondition { Key = key, Match = PrimitiveToMatch(vc.Value) } });
                }
                break;

            case ConditionType.AttributeNotEquals when condition is ValueCondition vc:
                if (vc.Value.Kind == PrimitiveKind.Double)
                {
                    var d = vc.Value.AsDouble;
                    target.MustNot.Add(new QdrantCondition
                        { Field = new FieldCondition { Key = key, Range = new QdrantRange { Gte = d, Lte = d } } });
                }
                else
                {
                    target.MustNot.Add(new QdrantCondition
                        { Field = new FieldCondition { Key = key, Match = PrimitiveToMatch(vc.Value) } });
                }
                break;

            case ConditionType.AttributeGreater when condition is ValueCondition vc:
                target.Must.Add(new QdrantCondition
                    { Field = new FieldCondition { Key = key, Range = new QdrantRange { Gt = ToDouble(vc.Value) } } });
                break;

            case ConditionType.AttributeGreaterOrEqual when condition is ValueCondition vc:
                target.Must.Add(new QdrantCondition
                    { Field = new FieldCondition { Key = key, Range = new QdrantRange { Gte = ToDouble(vc.Value) } } });
                break;

            case ConditionType.AttributeLess when condition is ValueCondition vc:
                target.Must.Add(new QdrantCondition
                    { Field = new FieldCondition { Key = key, Range = new QdrantRange { Lt = ToDouble(vc.Value) } } });
                break;

            case ConditionType.AttributeLessOrEqual when condition is ValueCondition vc:
                target.Must.Add(new QdrantCondition
                    { Field = new FieldCondition { Key = key, Range = new QdrantRange { Lte = ToDouble(vc.Value) } } });
                break;
        }
    }

    private static Match PrimitiveToMatch(Primitive p) => p.Kind switch
    {
        PrimitiveKind.String  => new Match { Keyword = p.AsString },
        PrimitiveKind.Integer => new Match { Integer = p.AsInteger },
        PrimitiveKind.Boolean => new Match { Boolean = p.AsBoolean },
        _                     => new Match { Keyword = string.Empty }
    };

    private static double ToDouble(Primitive p) => p.Kind switch
    {
        PrimitiveKind.Integer => (double)p.AsInteger,
        PrimitiveKind.Double  => p.AsDouble,
        _                     => 0
    };
}
