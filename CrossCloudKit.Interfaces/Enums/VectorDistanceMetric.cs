// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Interfaces.Enums;

/// <summary>
/// The distance metric used when searching a vector collection.
/// </summary>
public enum VectorDistanceMetric
{
    /// <summary>Cosine similarity — angle-based, independent of vector magnitude.</summary>
    Cosine,

    /// <summary>Euclidean (L2) distance — sensitive to vector magnitude.</summary>
    Euclidean,

    /// <summary>Dot product — combines magnitude and angle, fast for normalised vectors.</summary>
    DotProduct
}
