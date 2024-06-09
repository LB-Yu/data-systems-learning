package org.apache.calcite.adapter.hbase;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;

public interface HBaseRel extends RelNode {

    void implement(Implementor implementor);

    Convention CONVENTION = new Convention.Impl("HBASE", HBaseRel.class);

    class Implementor {

        byte[] startRow;
        byte[] endRow;

        final List<String> columns = new ArrayList<>();

        RelOptTable table;
        HBaseTable hBaseTable;

        public void setStartRow(byte[] startRow) {
            this.startRow = startRow;
        }

        public void setEndRow(byte[] endRow) {
            this.endRow = endRow;
        }

        public void addColumn(String column) {
            columns.add(column);
        }

        public void visitChild(int ordinal, RelNode input) {
            assert ordinal == 0;
            ((HBaseRel) input).implement(this);
        }
    }
}
