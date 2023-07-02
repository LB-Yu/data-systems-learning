package org.apache.calcite.adapter.hbase;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

import java.util.List;

public class HBaseProject extends Project implements HBaseRel {

    public HBaseProject(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType) {
        super(cluster, traits, ImmutableList.of(), input, projects, rowType);
        assert getConvention() == HBaseRel.CONVENTION;
        assert getConvention() == input.getConvention();
    }

    @Override
    public Project copy(
            RelTraitSet traitSet,
            RelNode input,
            List<RexNode> projects,
            RelDataType rowType) {
        return new HBaseProject(getCluster(), traitSet, input, projects, rowType);
    }

    @Override
    public void implement(Implementor implementor) {
        implementor.visitChild(0, getInput());
        for (Pair<RexNode, String> pair : getNamedProjects()) {
            implementor.addColumn(pair.right);
        }
    }
}
