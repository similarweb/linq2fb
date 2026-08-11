using System.Text;

namespace Similarweb.LinqToDB.Firebolt.Tests.Seeding;

/// <summary>
/// Splits a SQL script into individual statements, respecting quotes and comments.
/// </summary>
internal static class SqlStatementSplitter
{
    public static IReadOnlyList<string> Split(string text)
    {
        ArgumentNullException.ThrowIfNull(text);

        text = StripBlockComments(text).Replace("\r\n", "\n", StringComparison.Ordinal);

        var statements = new List<string>();
        var buffer = new StringBuilder();
        var inSingleQuote = false;
        var inDoubleQuote = false;
        var i = 0;

        while (i < text.Length)
        {
            var ch = text[i];

            if (!inSingleQuote && !inDoubleQuote && text.AsSpan(i).StartsWith("--"))
            {
                var lineEnd = text.IndexOf('\n', i);
                if (lineEnd < 0)
                {
                    break;
                }

                i = lineEnd + 1;
                continue;
            }

            if (ch == '\'' && !inDoubleQuote)
            {
                if (i + 1 < text.Length && text[i + 1] == '\'')
                {
                    buffer.Append("''");
                    i += 2;
                    continue;
                }

                inSingleQuote = !inSingleQuote;
                buffer.Append(ch);
                i++;
                continue;
            }

            if (ch == '"' && !inSingleQuote)
            {
                inDoubleQuote = !inDoubleQuote;
                buffer.Append(ch);
                i++;
                continue;
            }

            if (ch == ';' && !inSingleQuote && !inDoubleQuote)
            {
                var statement = buffer.ToString().Trim();
                if (statement.Length > 0)
                {
                    statements.Add(statement);
                }

                buffer.Clear();
                i++;
                continue;
            }

            buffer.Append(ch);
            i++;
        }

        var tail = buffer.ToString().Trim();
        if (tail.Length > 0)
        {
            statements.Add(tail);
        }

        return statements;
    }

    private static string StripBlockComments(string text)
    {
        var result = new StringBuilder(text.Length);
        var i = 0;
        while (i < text.Length)
        {
            if (i + 1 < text.Length && text[i] == '/' && text[i + 1] == '*')
            {
                var end = text.IndexOf("*/", i + 2, StringComparison.Ordinal);
                if (end < 0)
                {
                    break;
                }

                i = end + 2;
                continue;
            }

            result.Append(text[i]);
            i++;
        }

        return result.ToString();
    }
}
