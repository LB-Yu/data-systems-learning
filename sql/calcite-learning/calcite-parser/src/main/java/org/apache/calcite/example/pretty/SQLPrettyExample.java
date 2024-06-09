package org.apache.calcite.example.pretty;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.FlinkSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.pretty.FlinkSqlPrettyWriter;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.planner.delegation.FlinkSqlParserFactories;

public class SQLPrettyExample {

    public static void main(String[] args) throws SqlParseException {
        String sql = "insert into tt select `ca`, " +
                "cast(cb as int), " +
                "conv(cc, 16, 10), " +
                "case when aa = 1 then 1 else 2 end " +
                "from t1 join t2 on t1.id = t2.id and t1.ip = t2.ip " +
                "where cast(ca AS INT) = 10 AND YEAR() > 2000";

        SqlConformance flinkSqlConformance = FlinkSqlConformance.DEFAULT;
        SqlParser.Config flinkSqlConfig = SqlParser.config()
                .withParserFactory(FlinkSqlParserFactories.create(flinkSqlConformance))
                .withConformance(flinkSqlConformance)
                .withLex(Lex.JAVA)
                .withIdentifierMaxLength(256);
        SqlParser parser = SqlParser.create(sql, flinkSqlConfig);
        SqlNode sqlNode = parser.parseStmt();

        SqlWriterConfig writerConfig = SqlPrettyWriter.config()
                .withDialect(FlinkSqlDialect.DEFAULT)
                .withCaseClausesOnNewLines(false)
                .withIndentation(2)
                .withClauseStartsLine(true)
                .withFromFolding(SqlWriterConfig.LineFolding.TALL)
                .withQuoteAllIdentifiers(true)
                .withSelectFolding(SqlWriterConfig.LineFolding.TALL)
                .withSelectListItemsOnSeparateLines(true)
                .withWhereFolding(SqlWriterConfig.LineFolding.TALL)
                .withWhereListItemsOnSeparateLines(true)
                .withSelectListExtraIndentFlag(true)
                .withLineFolding(SqlWriterConfig.LineFolding.TALL)
                .withSelectFolding(SqlWriterConfig.LineFolding.TALL)
                .withClauseStartsLine(true)
                .withClauseEndsLine(true);
        SqlPrettyWriter prettyWriter = new FlinkSqlPrettyWriter(writerConfig);

        System.out.println(prettyWriter.format(sqlNode));
    }
}
