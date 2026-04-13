// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Interfaces.Enums;

/// <summary>
/// The distance metric used when searching a vector collection.
/// </summary>
/// <remarks>
/// Choose the metric that matches how your embeddings were trained:
/// <list type="bullet">
/// <item><see cref="Cosine"/> — best for most text embedding models (OpenAI, MiniLM, nomic-embed).</item>
/// <item><see cref="DotProduct"/> — use when vectors are already normalised; slightly faster than Cosine.</item>
/// <item><see cref="Euclidean"/> — useful for image embeddings or when absolute magnitude matters.</item>
/// </list>
/// </remarks>
public enum VectorDistanceMetric
{
    /// <summary>Cosine similarity — angle-based, independent of vector magnitude.</summary>
    Cosine,

    /// <summary>Euclidean (L2) distance — sensitive to vector magnitude.</summary>
    Euclidean,

    /// <summary>Dot product — combines magnitude and angle, fast for normalised vectors.</summary>
    DotProduct
}
