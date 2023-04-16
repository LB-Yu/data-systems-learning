package org.apache.calcite.adapter.hbase;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableScan;

public class HBaseTableScanRule extends ConverterRule {

    public static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(
                    HBaseTableScan.class,
                    Convention.NONE,
                    EnumerableConvention.INSTANCE,
                    "HBaseTableScanRule")
            .withRuleFactory(HBaseTableScanRule::new);

    protected HBaseTableScanRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalTableScan logicalTableScan = (LogicalTableScan) rel;
        return new HBaseTableScan(
                logicalTableScan.getCluster(),
                logicalTableScan.getTraitSet(),
                logicalTableScan.getTable());
    }
}
