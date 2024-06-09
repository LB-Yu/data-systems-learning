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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.Collections;

/**
 * Example to show how {@link VolcanoPlanner} optimize a regular join.
 * */
public class JoinExample {

    public static void main(String[] args) {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        Schema hrSchema = new HrClusteredSchema();
        rootSchema.add("hr", hrSchema);

        RelBuilderFactory relBuilderFactory = RelFactories.LOGICAL_BUILDER;
        RelDataTypeFactory relDataTypeFactory = new JavaTypeFactoryImpl();

        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        // planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        planner.setTopDownOpt(true);

        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(relDataTypeFactory));
        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
                rootSchema.getSubSchema("hr", false),
                Collections.singletonList(rootSchema.getName()),
                relDataTypeFactory,
                CalciteConnectionConfig.DEFAULT);

        RelBuilder builder = relBuilderFactory.create(cluster, catalogReader);

        // select name, dept, salary from emps join depts on emps.deptno = depts.deptno;
        RelNode root = builder
                .scan("depts")
                .scan("emps")
                .join(JoinRelType.INNER, "deptno")
                .project(
                        builder.field("name"),
                        builder.field("deptno"),
                        builder.field("salary"))
                .build();
        System.out.println(RelOptUtil.toString(root));

        planner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
        planner.addRule(AbstractConverter.ExpandConversionRule.INSTANCE);
        //
        //        planner.addRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
        //        planner.addRule(CoreRules.JOIN_COMMUTE);
        // planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);


        RelTraitSet desiredTraits =
                root.getCluster().traitSet()
                        .replace(EnumerableConvention.INSTANCE);
//                        .replace(RelCollations.of(1));
        root = planner.changeTraits(root, desiredTraits);
        planner.setRoot(root);
        RelNode optimizedRoot = planner.findBestExp();
        System.out.println(RelOptUtil.toString(optimizedRoot));
    }
}
