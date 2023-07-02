package org.apache.calcite.adapter.hbase;

import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.type.RelDataType;

public class HBaseToEnumerableConverter
        extends ConverterImpl implements EnumerableRel {

    protected HBaseToEnumerableConverter(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input) {
        super(cluster, ConventionTraitDef.INSTANCE, traits, input);
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        final BlockBuilder list = new BlockBuilder();
        final HBaseRel.Implementor hbaseImplementor = new HBaseRel.Implementor();
        hbaseImplementor.visitChild(0, getInput());
        final RelDataType rowType = getRowType();
        final PhysType physType = PhysTypeImpl.of(
                implementor.getTypeFactory(),
                rowType,
                pref.prefer(JavaRowFormat.ARRAY));

        return null;
    }
}
