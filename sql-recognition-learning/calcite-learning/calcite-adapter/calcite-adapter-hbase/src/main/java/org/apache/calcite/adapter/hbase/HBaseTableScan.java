package org.apache.calcite.adapter.hbase;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;

public class HBaseTableScan extends TableScan implements EnumerableRel {

    protected HBaseTableScan(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table) {
        super(cluster, traitSet, ImmutableList.of(), table);
    }

    @Override
    public void register(RelOptPlanner planner) {
        planner.addRule(HBaseRules.HBASE_TABLE_SCAN);
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        return null;
    }
}
