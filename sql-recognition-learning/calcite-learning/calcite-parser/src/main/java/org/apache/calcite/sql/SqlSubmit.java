package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

public class SqlSubmit extends SqlNode {

    private final String jobString;

    public SqlSubmit(SqlParserPos pos, String jobString) {
        super(pos);
        this.jobString = jobString;
    }

    public String getJobString() {
        System.out.println("getJobString");
        return jobString;
    }

    @Override
    public SqlNode clone(SqlParserPos sqlParserPos) {
        return null;
    }

    @Override
    public void unparse(SqlWriter sqlWriter, int i, int i1) {
        sqlWriter.keyword("submit");
        sqlWriter.keyword("job");
        sqlWriter.print("\n");
    }

    @Override
    public void validate(SqlValidator sqlValidator, SqlValidatorScope sqlValidatorScope) {

    }

    @Override
    public <R> R accept(SqlVisitor<R> sqlVisitor) {
        return null;
    }

    @Override
    public boolean equalsDeep(SqlNode sqlNode, Litmus litmus) {
        return false;
    }
}
