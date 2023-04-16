package org.apache.calcite.example.optimizer;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schemas.HrClusteredSchema;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.Collections;

public class VolcanoPlannerExample1 {

    public static void main(String[] args) {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        Schema hrSchema = new HrClusteredSchema();
        rootSchema.add("hr", hrSchema);

        RelBuilderFactory relBuilderFactory = RelFactories.LOGICAL_BUILDER;
        RelDataTypeFactory relDataTypeFactory = new JavaTypeFactoryImpl();

        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(relDataTypeFactory));
        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
                rootSchema.getSubSchema("hr", false),
                Collections.singletonList(rootSchema.getName()),
                relDataTypeFactory,
                CalciteConnectionConfig.DEFAULT);

        RelBuilder builder = relBuilderFactory.create(cluster, catalogReader);

        RelNode root = builder
                .scan("emps")
                .filter(
                        builder.call(
                                SqlStdOperatorTable.EQUALS,
                                builder.field("deptno"),
                                builder.literal(10)))
                .build();
        System.out.println(RelOptUtil.toString(root));

        planner.addRule(CoreRules.CALC_MERGE);
        planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);

        RelTraitSet desiredTraits =
                root.getCluster().traitSet().replace(EnumerableConvention.INSTANCE);
        root = planner.changeTraits(root, desiredTraits);
        planner.setRoot(root);
        RelNode optimizedRoot = planner.findBestExp();
        System.out.println(RelOptUtil.toString(optimizedRoot));
    }
}
