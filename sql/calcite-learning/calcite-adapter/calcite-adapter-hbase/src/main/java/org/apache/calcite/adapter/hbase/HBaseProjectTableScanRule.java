package org.apache.calcite.adapter.hbase;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.immutables.value.Value;

import java.util.List;

@Value.Enclosing
public class HBaseProjectTableScanRule
        extends RelRule<HBaseProjectTableScanRule.Config> {

    public HBaseProjectTableScanRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        HBaseTableScan scan = call.rel(1);
        int[] columns = getProjectColumns(project.getProjects());
        if (columns == null) {
            return;
        }
        call.transformTo(
                new HBaseTableScan(
                        scan.getCluster(),
                        scan.getTable(),
                        scan.gethBaseTable(),
                        columns));
    }

    private static int[] getProjectColumns(List<RexNode> exps) {
        int[] columns = new int[exps.size()];
        for (int i = 0; i < exps.size(); ++i) {
            RexNode exp = exps.get(i);
            if (exp instanceof RexInputRef) {
                columns[i] = ((RexInputRef) exp).getIndex();
            } else {
                return null;
            }
        }
        return columns;
    }

    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableHBaseProjectTableScanRule.Config.builder()
                .withOperandSupplier(b0 ->
                        b0.operand(LogicalProject.class).oneInput(b1 ->
                                b1.operand(HBaseTableScan.class).noInputs()))
                .build();

        @Override
        default HBaseProjectTableScanRule toRule() {
            return new HBaseProjectTableScanRule(this);
        }
    }
}
