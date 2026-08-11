using LinqToDB;
using LinqToDB.Internal.DataProvider.Translation;
using LinqToDB.Internal.SqlQuery;
using LinqToDB.Linq.Translation;

namespace Similarweb.LinqToDB.Firebolt;

/// <inheritdoc/>
internal class MemberTranslator : ProviderMemberTranslatorDefault
{
    /// <inheritdoc/>
    protected override IMemberTranslator CreateDateMemberTranslator() =>
        new DateFunctionsTranslator();

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
