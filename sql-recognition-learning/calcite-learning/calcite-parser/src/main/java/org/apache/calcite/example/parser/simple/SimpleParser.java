package org.apache.calcite.example.parser.simple;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

public class SimpleParser {

    public static void main(String[] args) throws SqlParseException {
        // mysql config
        SqlParser.Config mysqlConfig = SqlParser.configBuilder().setLex(Lex.MYSQL).build();
        SqlParser parser = SqlParser.create("", mysqlConfig);
        SqlNode sqlNode = parser.parseQuery("select a, b from t where a >= 1 and b = 2 limit 1");
        System.out.println(sqlNode);
        System.out.println(sqlNode.toSqlString(OracleSqlDialect.DEFAULT));
    }
}
