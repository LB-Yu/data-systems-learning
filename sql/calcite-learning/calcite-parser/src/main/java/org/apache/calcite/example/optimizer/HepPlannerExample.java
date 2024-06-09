package org.apache.calcite.example.optimizer;

import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.example.overall.SimpleDataContext;
import org.apache.calcite.example.schemas.HrClusteredSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import java.util.LinkedHashMap;
import java.util.Map;

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
        hepProgramBuilder.addRuleInstance(CoreRules.FILTER_TO_CALC);
        hepProgramBuilder.addRuleInstance(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
        hepProgramBuilder.addRuleInstance(EnumerableRules.ENUMERABLE_CALC_RULE);
        HepPlanner planner = new HepPlanner(hepProgramBuilder.build());

        planner.setRoot(root);
        RelNode optimizedRoot = planner.findBestExp();
        System.out.println(RelOptUtil.toString(optimizedRoot));

        EnumerableRel enumerable = (EnumerableRel) optimizedRoot;
        Map<String, Object> internalParameters = new LinkedHashMap<>();
        EnumerableRel.Prefer prefer = EnumerableRel.Prefer.ARRAY;
        Bindable bindable = EnumerableInterpretable.toBindable(internalParameters,
                null, enumerable, prefer);
        Enumerable bind = bindable.bind(new SimpleDataContext(rootSchema));
        Enumerator enumerator = bind.enumerator();
        while (enumerator.moveNext()) {
            Object current = enumerator.current();
            Object[] values = (Object[]) current;
            StringBuilder sb = new StringBuilder();
            for (Object v : values) {
                sb.append(v).append(",");
            }
            sb.setLength(sb.length() - 1);
            System.out.println(sb);
        }
    }
}
