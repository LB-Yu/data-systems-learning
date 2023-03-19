package org.apache.calcite.example.rel;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.junit.Test;

import java.util.List;

public class RelBuilderTest {

    private final FrameworkConfig config = config().build();

    @Test
    public void testTableScan() {
        final RelBuilder builder = RelBuilder.create(config);
        final RelNode node = builder
                .scan("EMP")
                .build();
        System.out.println(RelOptUtil.toString(node));
    }

    @Test
    public void testProject() {
        final RelBuilder builder = RelBuilder.create(config);
        final RelNode node = builder
                .scan("EMP")
                .project(builder.field("ID"), builder.field("NAME"))
                .build();
        System.out.println(RelOptUtil.toString(node));
    }

    @Test
    public void testFilterAndAggregate() {
        final RelBuilder builder = RelBuilder.create(config);
        final RelNode node = builder
                .scan("EMP")
                .aggregate(builder.groupKey("ID"),
                        builder.count(false, "C"),
                        builder.sum(false, "S", builder.field("SAL")))
                .filter(
                        builder.call(SqlStdOperatorTable.GREATER_THAN,
                                builder.field("C"),
                                builder.literal(10)))
                .build();
        System.out.println(RelOptUtil.toString(node));
    }

    public static Frameworks.ConfigBuilder config() {
        final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        rootSchema.add("EMP", new AbstractTable() {
            @Override
            public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.Builder builder = typeFactory.builder();

                builder.add("ID", new BasicSqlType(new RelDataTypeSystemImpl() {
                }, SqlTypeName.INTEGER));
                builder.add("NAME", new BasicSqlType(new RelDataTypeSystemImpl() {
                }, SqlTypeName.CHAR));
                builder.add("AGE", new BasicSqlType(new RelDataTypeSystemImpl() {
                }, SqlTypeName.INTEGER));
                builder.add("SAL", new BasicSqlType(new RelDataTypeSystemImpl() {
                }, SqlTypeName.DECIMAL));
                return builder.build();
            }
        });

        rootSchema.add("JOBS", new AbstractTable() {
            @Override
            public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.Builder builder = typeFactory.builder();

                builder.add("ID", new BasicSqlType(new RelDataTypeSystemImpl() {
                }, SqlTypeName.INTEGER));
                builder.add("NAME", new BasicSqlType(new RelDataTypeSystemImpl() {
                }, SqlTypeName.CHAR));
                builder.add("COMPANY", new BasicSqlType(new RelDataTypeSystemImpl() {
                }, SqlTypeName.CHAR));
                return builder.build();
            }
        });
        return Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.Config.DEFAULT)
                .defaultSchema(rootSchema)
                .traitDefs((List<RelTraitDef>) null)
                .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
    }
}
