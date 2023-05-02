package org.apache.calcite.adapter.hbase;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;

public class HBaseTableScan extends TableScan implements EnumerableRel {

    private final HBaseTable hBaseTable;
    private final int[] columns;

    protected HBaseTableScan(
            RelOptCluster cluster,
            RelOptTable table,
            HBaseTable hBaseTable,
            int[] columns) {
        super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), ImmutableList.of(), table);
        this.hBaseTable = hBaseTable;
        this.columns = columns;
    }

    public HBaseTable gethBaseTable() {
        return hBaseTable;
    }

    @Override
    public RelDataType deriveRowType() {
        List<RelDataTypeField> fieldList = table.getRowType().getFieldList();
        RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
        for (int column : columns) {
            builder.add(fieldList.get(column));
        }
        return builder.build();
    }

    @Override
    public void register(RelOptPlanner planner) {
        planner.addRule(HBaseRules.PROJECT_SCAN);
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        PhysType physType = PhysTypeImpl.of(
                implementor.getTypeFactory(),
                getRowType(),
                pref.preferArray());

        List<RelDataTypeField> fieldList = getRowType().getFieldList();
        String[] columnNames = fieldList.stream()
                .map(RelDataTypeField::getName)
                .toArray(String[]::new);

        return implementor.result(
                physType,
                Blocks.toBlock(
                        Expressions.call(
                                table.getExpression(HBaseTranslatableTable.class),
                                "project",
                                implementor.getRootExpression(),
                                Expressions.constant(columnNames))));
    }
}
