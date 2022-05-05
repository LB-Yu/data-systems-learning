package org.apache.calcite.example.parser.ddl;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.NlsString;

import java.util.List;

import static java.util.Objects.requireNonNull;

/** Table options of a DDL, a key-value pair with both key and value as string literal. */
public class SqlTableOption extends SqlCall {
  /** Use this operator only if you don't have a better one. */
  protected static final SqlOperator OPERATOR =
          new SqlSpecialOperator("TableOption", SqlKind.OTHER);

  private final SqlNode key;
  private final SqlNode value;

  public SqlTableOption(SqlNode key, SqlNode value, SqlParserPos pos) {
    super(pos);
    this.key = requireNonNull(key, "Option key is missing");
    this.value = requireNonNull(value, "Option value is missing");
  }

  public SqlNode getKey() {
    return key;
  }

  public SqlNode getValue() {
    return value;
  }

  public String getKeyString() {
    return ((NlsString) SqlLiteral.value(key)).getValue();
  }

  public String getValueString() {
    return ((NlsString) SqlLiteral.value(value)).getValue();
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(key, value);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    key.unparse(writer, leftPrec, rightPrec);
    writer.keyword("=");
    value.unparse(writer, leftPrec, rightPrec);
  }
}

