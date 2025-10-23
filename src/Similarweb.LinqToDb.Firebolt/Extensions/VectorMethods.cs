using LinqToDB;

namespace Similarweb.LinqToDB.Firebolt.Extensions;

/// <summary>
/// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/vector/">vector functions</see> in Firebolt.
/// </summary>
public static class VectorMethods
{
    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/vector/vector-cosine-distance.html">VECTOR_COSINE_DISTANCE</see> Firebolt method.
    /// </summary>
    /// <remarks>
    /// <para>Calculates cosine similarity between two vectors using formula.
    /// <code>
    ///     A · B
    /// —————————————,
    /// ||A|| * ||B||
    /// </code>
    /// where <c>A</c> and <c>B</c> are vectors, <c>i</c> is the index of the vector element; <c>·</c> is the dot product;
    /// <c>||A||</c> is length of <c>A</c>, calculated as <c>√(∑(A²i))</c>.
    /// </para>
    /// <para>LINQ version should be optimized (using ZLinq/SIMD) prior production usage, current implementation is only for reference.</para>
    /// </remarks>
    /// <param name="referenceVector">reference vector.</param>
    /// <param name="measuringVector">vector.</param>
    /// <returns>similarity measure [-1,1].</returns>
    /// <exception cref="ArgumentException">if vector sizes are not equal OR one of vectors has Euclidean length of 0.</exception>
    [Sql.Expression("VECTOR_COSINE_SIMILARITY({0}, {1})", ServerSideOnly = true, IsAggregate = false)]
    public static double VectorCosineSimilarity(
        this double[] referenceVector,
        double[] measuringVector
    )
    {
        ArgumentNullException.ThrowIfNull(referenceVector);
        ArgumentNullException.ThrowIfNull(measuringVector);
        if (referenceVector.Length != measuringVector.Length)
        {
            throw new ArgumentException("The vector sizes do not match.");
        }

        if (referenceVector.Length == 0)
        {
            throw new ArgumentException("Reference vector is empty.", nameof(referenceVector));
        }

        if (measuringVector.Length == 0)
        {
            throw new ArgumentException("Measuring vector is empty.", nameof(measuringVector));
        }

        return measuringVector
            .Zip(referenceVector)
            .Aggregate(
                new Similarity(),
                (similarity, pair) => similarity.Add(pair),
                similarity => similarity.Calc()
            );
    }

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/vector/vector-cosine-distance.html">VECTOR_COSINE_DISTANCE</see> Firebolt method.
    /// </summary>
    /// <remarks>
    /// <para>Calculates cosine similarity between two vectors using formula.
    /// <code>
    ///     A · B
    /// —————————————,
    /// ||A|| * ||B||
    /// </code>
    /// where <c>A</c> and <c>B</c> are vectors, <c>i</c> is the index of the vector element; <c>·</c> is the dot product;
    /// <c>||A||</c> is length of <c>A</c>, calculated as <c>√(∑(A²i))</c>.
    /// </para>
    /// <para>LINQ version should be optimized (using ZLinq/SIMD) prior production usage, current implementation is only for reference.</para>
    /// </remarks>
    /// <param name="referenceVector">reference vector.</param>
    /// <param name="measuringVector">vector.</param>
    /// <returns>similarity measure [-1,1].</returns>
    /// <exception cref="ArgumentException">if vector sizes are not equal OR one of vectors has Euclidean length of 0.</exception>
    [Sql.Expression("VECTOR_COSINE_SIMILARITY({0}, {1})", ServerSideOnly = true, IsAggregate = false)]
    public static float VectorCosineSimilarity(
        this float[] referenceVector,
        float[] measuringVector
    )
    {
        ArgumentNullException.ThrowIfNull(referenceVector);
        ArgumentNullException.ThrowIfNull(measuringVector);
        if (referenceVector.Length != measuringVector.Length)
        {
            throw new ArgumentException("The vector sizes do not match.");
        }

        if (referenceVector.Length == 0)
        {
            throw new ArgumentException("Reference vector is empty.", nameof(referenceVector));
        }

        if (measuringVector.Length == 0)
        {
            throw new ArgumentException("Measuring vector is empty.", nameof(measuringVector));
        }

        return measuringVector
            .Zip(referenceVector)
            .Aggregate(
                new Similarity(),
                (similarity, pair) => similarity.Add(pair),
                similarity => (float)similarity.Calc()
            );
    }

    private sealed class Similarity
    {
        private double _dot = 0;
        private double _aLen = 0;
        private double _bLen = 0;

        public Similarity Add((double A, double B) item)
        {
            _dot += item.A * item.B;
            _aLen += item.A * item.A;
            _bLen += item.B * item.B;
            return this;
        }

        public double Calc()
        {
            if (_aLen == 0 || _bLen == 0)
            {
                throw new ArgumentException("One of the vectors is zero-length, cannot calculate cosine similarity.");
            }

            return 1 - (_dot / (Math.Sqrt(_aLen) * Math.Sqrt(_bLen)));
        }
    }
}
