using LinqToDB.Internal.SqlProvider;

namespace Similarweb.LinqToDB.Firebolt;

/// <inheritdoc/>
internal class SqlOptimizer(
    SqlProviderFlags sqlProviderFlags
) : BasicSqlOptimizer(sqlProviderFlags)
{
    /// <inheritdoc/>
    public override SqlExpressionConvertVisitor CreateConvertVisitor(bool allowModify) => new ConvertVisitor(allowModify);

    private class ConvertVisitor(bool allowModify) : SqlExpressionConvertVisitor(allowModify)
    {
        public override bool LikeIsEscapeSupported => false;
    }
}
