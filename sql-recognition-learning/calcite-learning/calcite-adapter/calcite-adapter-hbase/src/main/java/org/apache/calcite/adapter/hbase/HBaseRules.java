package org.apache.calcite.adapter.hbase;

public abstract class HBaseRules {

    private HBaseRules() {}

    public static final HBaseProjectTableScanRule PROJECT_SCAN =
            HBaseProjectTableScanRule.Config.DEFAULT.toRule();
}
