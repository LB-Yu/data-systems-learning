package org.apache.calcite.example.parser.ddl;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;

/**
 * Parse tree for {@code CREATE TABLE} statement.
 */
public class SqlCreateTable extends SqlCreate {
    public final SqlIdentifier name;
    public final SqlNodeList columnList;
    public final SqlNodeList propertyList;
    public final SqlNode query;

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);

    /**
     * Creates a SqlCreateTable.
     */
    public SqlCreateTable(SqlParserPos pos, boolean replace, boolean ifNotExists,
                          SqlIdentifier name, SqlNodeList columnList, SqlNodeList propertyList,
                          SqlNode query) {
        super(OPERATOR, pos, replace, ifNotExists);
        this.name = Objects.requireNonNull(name);
        this.columnList = columnList; // may be null
        this.propertyList = propertyList; // may be null
        this.query = query; // for "CREATE TABLE ... AS query"; may be null
    }

    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, columnList, propertyList, query);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        writer.keyword("TABLE");
        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS");
        }
        name.unparse(writer, leftPrec, rightPrec);
        if (columnList != null) {
            SqlWriter.Frame frame = writer.startList("(", ")");
            for (SqlNode c : columnList) {
                writer.sep(",");
                c.unparse(writer, 0, 0);
            }
            writer.endList(frame);
        }
        if (propertyList != null) {
            writer.keyword("WITH");
            SqlWriter.Frame frame = writer.startList("(", ")");
            for (SqlNode p : propertyList) {
                writer.sep(",");
                p.unparse(writer, 0, 0);
            }
            writer.endList(frame);
        }
        if (query != null) {
            writer.keyword("AS");
            writer.newlineAndIndent();
            query.unparse(writer, 0, 0);
        }
    }
}
