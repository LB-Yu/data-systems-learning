package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.CustomSqlParserImpl;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.junit.Test;

public class ExampleSqlParserImplTest {

  @Test
  public void testRunExample() throws SqlParseException {
    FrameworkConfig config = Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.configBuilder()
                    .setParserFactory(CustomSqlParserImpl.FACTORY).build()).build();
    String sql = "submit job as 'select * from test'";

    SqlParser sqlParser = SqlParser.create(sql, config.getParserConfig());
    SqlNode sqlNode = sqlParser.parseStmt();
    System.out.println(sqlNode.toString());
  }

}
