package org.apache.calcite.example.optimizer;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.example.schemas.HrClusteredSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.Collections;

/**
 * An example shows how {@link AbstractConverter} will be used.
 * */
public class AbstractConverterExample {

    public static void main(String[] args) {
        CalciteSchema rootSchema =
                CalciteSchema.createRootSchema(false, false);
        Schema hrSchema = new HrClusteredSchema();
        rootSchema.add("hr", hrSchema);

        RelBuilderFactory relBuilderFactory = RelFactories.LOGICAL_BUILDER;
        RelDataTypeFactory relDataTypeFactory = new JavaTypeFactoryImpl();

        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        // Open the comment to explore when consider Collation trait.
        // It needs to be enabled or disabled simultaneously with line 76.
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        // Open the comment to explore when use top-down algorithm.
        // planner.setTopDownOpt(true);

        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(relDataTypeFactory));

        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
                rootSchema.getSubSchema("hr", false),
                Collections.singletonList(rootSchema.getName()),
                relDataTypeFactory,
                CalciteConnectionConfig.DEFAULT);

        RelBuilder builder = relBuilderFactory.create(cluster, catalogReader);

        RelNode root = builder
                .scan("emps")
                .build();

        planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
        // The following two rules are only necessary when the top-down algorithm is disabled.
        // In top-down algorithm, the EnumerableConvention.enforce() will generate the EnumerableSort.
        planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
        planner.addRule(AbstractConverter.ExpandConversionRule.INSTANCE);

        RelTraitSet desiredTraits =
                root.getCluster().traitSet()
                        .replace(EnumerableConvention.INSTANCE)
                        // Open the comment to explore when consider Collation trait.
                        // It needs to be enabled or disabled simultaneously with line 47.
                        .replace(RelCollations.of(1))
                ;

        root = planner.changeTraits(root, desiredTraits);
        planner.setRoot(root);
        RelNode optimizedRoot = planner.findBestExp();
        System.out.println(RelOptUtil.toString(optimizedRoot));
    }
}
