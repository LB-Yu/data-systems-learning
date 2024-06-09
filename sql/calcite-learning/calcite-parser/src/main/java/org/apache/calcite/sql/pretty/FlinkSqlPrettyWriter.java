package org.apache.calcite.sql.pretty;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.flink.sql.parser.dml.RichSqlInsert;

public class FlinkSqlPrettyWriter extends SqlPrettyWriter {

    public FlinkSqlPrettyWriter(SqlWriterConfig config) {
        super(config);
    }

    @Override
    public String format(SqlNode node) {
        assert frame == null;
        if (node instanceof RichSqlInsert) {
            RichSqlInsert richSqlInsert = (RichSqlInsert) node;

            Frame selectFrame = this.startList(FrameTypeEnum.SELECT);
            String insertKeyword = "INSERT INTO";
            if (richSqlInsert.isUpsert()) {
                insertKeyword = "UPSERT INTO";
            } else if (richSqlInsert.isOverwrite()) {
                insertKeyword = "INSERT OVERWRITE";
            }

            this.sep(insertKeyword);
            int opLeft = richSqlInsert.getOperator().getLeftPrec();
            int opRight = richSqlInsert.getOperator().getRightPrec();
            richSqlInsert.getTargetTable().unparse(this, opLeft, opRight);
            if (richSqlInsert.getStaticPartitions() != null &&
                    richSqlInsert.getStaticPartitions().size() > 0) {
                this.keyword("PARTITION");
                richSqlInsert.getStaticPartitions().unparse(this, opLeft, opRight);
                this.newlineAndIndent();
            }

            if (richSqlInsert.getTargetColumnList() != null) {
                richSqlInsert.getTargetColumnList().unparse(this, opLeft, opRight);
            }

            this.newlineAndIndent();
            this.endList(selectFrame);
            richSqlInsert.getSource().unparse(this, 0, 0);
        } else {
            node.unparse(this, 0, 0);
        }
        assert frame == null;
        return toString();
    }
}
