using System.Globalization;
using LinqToDB;
using LinqToDB.Internal.SqlQuery;
using LinqToDB.Mapping;
using LinqToDB.SqlQuery;

namespace Similarweb.LinqToDB.Firebolt.Extensions;

/// <summary>
/// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/date-and-time/">date and time functions</see> in Firebolt.
/// </summary>
public static class DateTimeMethods
{
    /// <summary>
    /// Get date part extension method.
    /// </summary>
    /// <param name="part">What date part should be extracted.</param>
    /// <param name="date">Date.</param>
    /// <returns>Value.</returns>
    /// <exception cref="InvalidOperationException">Invalid or unsupported date part.</exception>
    [Sql.Extension(DataProvider.V2Id, "Extract({part} from {date})", ServerSideOnly = false, PreferServerSide = false, BuilderType = typeof(DatePartBuilder))]
    public static int? DatePart([SqlQueryDependent] Sql.DateParts part, [ExprParameter] DateTime? date)
    {
        if (date == null)
        {
            return null;
        }

        return part switch
        {
            Sql.DateParts.Year => date.Value.Year,
            Sql.DateParts.Quarter => ((date.Value.Month - 1) / 3) + 1,
            Sql.DateParts.Month => date.Value.Month,
            Sql.DateParts.DayOfYear => date.Value.DayOfYear,
            Sql.DateParts.Day => date.Value.Day,
            Sql.DateParts.Week => CultureInfo.CurrentCulture.Calendar.GetWeekOfYear(date.Value, CalendarWeekRule.FirstDay, DayOfWeek.Sunday),
            Sql.DateParts.WeekDay => (((int)date.Value.DayOfWeek + 1 + Sql.DateFirst + 6) % 7) + 1,
            Sql.DateParts.Hour => date.Value.Hour,
            Sql.DateParts.Minute => date.Value.Minute,
            Sql.DateParts.Second => date.Value.Second,
            Sql.DateParts.Millisecond => date.Value.Millisecond,
            _ => throw new InvalidOperationException(),
        };
    }

    /// <summary>
    /// Calculate date diff extension method.
    /// </summary>
    /// <param name="startDate">Start date.</param>
    /// <param name="endDate">End date.</param>
    /// <returns>Diff in days.</returns>
    [Sql.Extension(DataProvider.V2Id, "DateDiff", BuilderType = typeof(DateDiffBuilder))]
    public static int? DateDiff(DateTime? startDate, DateTime? endDate)
    {
        if (startDate == null || endDate == null)
        {
            return null;
        }

        return (int)(endDate - startDate).Value.TotalDays;
    }

    /// <summary>
    /// Get the current date and time.
    /// </summary>
    /// <returns><see cref="DateTime"/>.</returns>
    [Sql.Extension(DataProvider.V2Id, "CURRENT_DATE", ServerSideOnly = true)]
    public static DateTime CurrentDateTime()
    {
        return DateTime.Now;
    }

    #region Builders
    private class DatePartBuilder : Sql.IExtensionCallBuilder
    {
        public void Build(Sql.ISqlExtensionBuilder builder)
        {
            const string part = nameof(part);
            var partValue = builder.GetValue<Sql.DateParts>(part);
            switch (partValue)
            {
                case Sql.DateParts.DayOfYear:
                    builder.Expression = "DayOfYear({date})";
                    break;
                case Sql.DateParts.WeekDay:
                    builder.Expression = "WeekDay(Date_Add({date}, interval 1 day))";
                    if (builder.ConvertToSqlExpression(Precedence.Primary) is not { } expr)
                    {
                        throw new InvalidOperationException("Invalid expression for WeekDay");
                    }

                    builder.ResultExpression = builder.Inc(expr);
                    break;
                default:
                    var partStr = DatePartToStr(partValue);
                    builder.AddFragment(part, partStr);
                    break;
            }
        }

        private static string DatePartToStr(Sql.DateParts part)
        {
            return part switch
            {
                Sql.DateParts.Year => "year",
                Sql.DateParts.Quarter => "quarter",
                Sql.DateParts.Month => "month",
                Sql.DateParts.DayOfYear => "dayofyear",
                Sql.DateParts.Day => "day",
                Sql.DateParts.Week => "week",
                Sql.DateParts.WeekDay => "weekday",
                Sql.DateParts.Hour => "hour",
                Sql.DateParts.Minute => "minute",
                Sql.DateParts.Second => "second",
                Sql.DateParts.Millisecond => "millisecond",
                _ => throw new InvalidOperationException($"Unexpected datepart: {part}"),
            };
        }
    }

    private class DateDiffBuilder : Sql.IExtensionCallBuilder
    {
        public void Build(Sql.ISqlExtensionBuilder builder)
        {
            if (builder.GetExpression(0) is not { } startDate)
            {
                throw new InvalidOperationException("Invalid expression for startDate");
            }

            if (builder.GetExpression(1) is not { } endDate)
            {
                throw new InvalidOperationException("Invalid expression for endDate");
            }

            builder.ResultExpression = new SqlFunction(new DbDataType(typeof(int)), builder.Expression, startDate, endDate);
        }
    }
    #endregion // Builders
}
