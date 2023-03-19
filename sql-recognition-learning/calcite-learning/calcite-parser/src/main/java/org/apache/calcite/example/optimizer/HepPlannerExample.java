package org.apache.calcite.example.optimizer;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schemas.HrClusteredSchema;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

public class HepPlannerExample {

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

        HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
        hepProgramBuilder.addRuleCollection(ImmutableList.of(
                CoreRules.CALC_MERGE));
        HepPlanner planner = new HepPlanner(hepProgramBuilder.build());

        planner.setRoot(root);
        RelNode optimizedRoot = planner.findBestExp();
        System.out.println(RelOptUtil.toString(optimizedRoot));
    }
}
