package org.apache.calcite.adapter.hbase;

public abstract class HBaseRules {

    private HBaseRules() {}

    public static final HBaseTableScanRule HBASE_TABLE_SCAN =
            HBaseTableScanRule.DEFAULT_CONFIG.toRule(HBaseTableScanRule.class);
}
