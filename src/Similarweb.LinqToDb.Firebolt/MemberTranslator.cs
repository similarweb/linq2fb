using System.Linq.Expressions;
using LinqToDB;
using LinqToDB.Internal.DataProvider.Translation;
using LinqToDB.Internal.Expressions;
using LinqToDB.Internal.SqlQuery;
using LinqToDB.Linq.Translation;
using LinqToDB.SqlQuery;

namespace Similarweb.LinqToDB.Firebolt;

/// <inheritdoc/>
internal class MemberTranslator : ProviderMemberTranslatorDefault
{
    /// <inheritdoc/>
    protected override IMemberTranslator CreateDateMemberTranslator() =>
        new DateFunctionsTranslator();

    /// <inheritdoc/>
    protected override IMemberTranslator CreateAggregateFunctionsMemberTranslator() =>
        new FireboltAggregateFunctionsMemberTranslator();

    private class FireboltAggregateFunctionsMemberTranslator : AggregateFunctionsMemberTranslatorBase
    {
        public FireboltAggregateFunctionsMemberTranslator()
        {
            // Native group.Select(...).ToArray() → ARRAY_AGG (avoids client-side preambles / transactions).
            Registration.RegisterMethod((IEnumerable<int> e) => e.ToArray(), TranslateToArray, isGenericTypeMatch: true);
            Registration.RegisterMethod((IQueryable<int> e) => e.ToArray(), TranslateToArray, isGenericTypeMatch: true);
        }

        private static Expression? TranslateToArray(
            ITranslationContext translationContext,
            MethodCallExpression methodCall,
#pragma warning disable SA1313
            TranslationFlags _
#pragma warning restore SA1313
        )
        {
            return new AggregateFunctionBuilder()
                .ConfigureAggregate(c => c
                    .HasSequenceIndex(0)
                    .HasValue(hasValue: false)
                    .AllowFilter()
                    .AllowDistinct()
                    .OnBuildFunction(composer =>
                    {
                        var buildInfo = composer.BuildInfo;
                        if (buildInfo.SelectQuery == null || buildInfo.ValueExpression == null)
                        {
                            return;
                        }

                        if (!composer.Translator.TranslateExpression(
                                buildInfo.ValueExpression,
                                out var sql,
                                out SqlErrorExpression? error))
                        {
                            composer.SetError(error!);
                            return;
                        }

                        var factory = buildInfo.Factory;
                        var resultType = factory.GetDbDataType(methodCall.Method.ReturnType);

                        var modifier = buildInfo.IsDistinct
                            ? Sql.AggregateModifier.Distinct
                            : Sql.AggregateModifier.None;

                        var result = factory.Function(
                            resultType,
                            "ARRAY_AGG",
                            [new SqlFunctionArgument(sql, modifier)],
                            [true, true],
                            canBeNull: true,
                            withinGroup: null,
                            partitionBy: null,
                            orderBy: null,
                            frameClause: null,
                            filter: null,
                            isAggregate: true,
                            canBeAffectedByOrderBy: false);

                        composer.SetResult(result);
                    }))
                .Build(translationContext, methodCall);
        }
    }

    private class DateFunctionsTranslator : DateFunctionsTranslatorBase
    {
        protected override ISqlExpression? TranslateDateTimeDatePart(ITranslationContext translationContext, TranslationFlags translationFlag, ISqlExpression dateTimeExpression, Sql.DateParts datepart)
        {
            var factory = translationContext.ExpressionFactory;
            var intDataType = factory.GetDbDataType(typeof(int));

            string partStr;

            switch (datepart)
            {
                case Sql.DateParts.Year: partStr = "year"; break;
                case Sql.DateParts.Quarter: partStr = "quarter"; break;
                case Sql.DateParts.Month: partStr = "month"; break;
                case Sql.DateParts.DayOfYear:
                {
                    return factory.Function(intDataType, "DayOfYear", dateTimeExpression);
                }

                case Sql.DateParts.Day: partStr = "day"; break;
                case Sql.DateParts.Week: partStr = "week"; break;
                case Sql.DateParts.WeekDay:
                {
                    var addDaysFunc = factory.Function(
                        factory.GetDbDataType(dateTimeExpression),
                        functionName: "Date_Add",
                        ParametersNullabilityType.SameAsFirstParameter,
                        dateTimeExpression,
                        factory.NotNullExpression(intDataType, "interval 1 day")
                    );

                    var weekDayFunc = factory.Function(intDataType, "WeekDay", addDaysFunc);

                    return factory.Increment(weekDayFunc);
                }

                case Sql.DateParts.Hour: partStr = "hour"; break;
                case Sql.DateParts.Minute: partStr = "minute"; break;
                case Sql.DateParts.Second: partStr = "second"; break;
                case Sql.DateParts.Millisecond:
                {
                    var microsecondFunc = factory.Div(intDataType, factory.Function(intDataType, "Microsecond", dateTimeExpression), 1000);
                    return microsecondFunc;
                }

                default:
                    return null;
            }

            var resultExpression = factory.Function(intDataType, "Extract", factory.Expression(intDataType, partStr + " from {0}", dateTimeExpression));

            return resultExpression;
        }
    }
}
