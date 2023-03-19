package org.apache.calcite.example.parser;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.CustomSqlParserImpl;

public class CalciteSQLParser {

  public static void main(String[] args) throws SqlParseException {
    String ddl = "CREATE TABLE aa (id INT) WITH ('connector' = 'file')";
    String sql = "select ca, cb, cc from t where cast(ca AS INT) = 10 AND YEAR() > 2000";

    String expr = "1 + 1";

    SqlParser.Config config = SqlParser.config()
            .withParserFactory(CustomSqlParserImpl.FACTORY);

    SqlParser parser = SqlParser.create(expr, config);
    SqlNode sqlNode = parser.parseExpression();
    System.out.println(sqlNode);
  }
}
