package org.apache.calcite.example.optimizer;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.example.schemas.HrClusteredSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

public class VolcanoPlannerExample {

    public static void main(String[] args) {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        Schema hrSchema = new HrClusteredSchema();
        rootSchema.add("hr", hrSchema);

        FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.Config.DEFAULT)
                .defaultSchema(rootSchema.getSubSchema("hr"))
                .build();
        RelBuilder builder = RelBuilder.create(config);

        RelNode root = builder
                .scan("emps")
                .filter(
                        builder.call(
                                SqlStdOperatorTable.EQUALS,
                                builder.field("deptno"),
                                builder.literal(10)))
                .build();
        System.out.println(RelOptUtil.toString(root));

        RelOptPlanner planner = root.getCluster().getPlanner();
        planner.addRule(CoreRules.CALC_MERGE);
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        RelTraitSet desiredTraits =
                root.getCluster().traitSet().replace(EnumerableConvention.INSTANCE);
        root = planner.changeTraits(root, desiredTraits);
        planner.setRoot(root);
        RelNode optimizedRoot = planner.findBestExp();
        System.out.println(RelOptUtil.toString(optimizedRoot));
    }
}
