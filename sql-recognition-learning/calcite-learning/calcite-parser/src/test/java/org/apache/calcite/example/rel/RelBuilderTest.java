package org.apache.calcite.example.rel;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.junit.Test;

import java.util.List;

public class RelBuilderTest {

  @Test
  public void testTableScan() {
    final FrameworkConfig config = config().build();
    final RelBuilder builder = RelBuilder.create(config);
    final RelNode node = builder
            .scan("EMP")
            .build();
    System.out.println(RelOptUtil.toString(node));
  }

  public static Frameworks.ConfigBuilder config() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    rootSchema.add("EMP", new AbstractTable() {
      @Override
      public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();

        builder.add("ID", new BasicSqlType(new RelDataTypeSystemImpl() {}, SqlTypeName.INTEGER));
        builder.add("NAME", new BasicSqlType(new RelDataTypeSystemImpl() {}, SqlTypeName.CHAR));
        builder.add("AGE", new BasicSqlType(new RelDataTypeSystemImpl() {}, SqlTypeName.INTEGER));
        return builder.build();
      }
    });

    rootSchema.add("JOBS", new AbstractTable() {
      @Override
      public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();

        builder.add("ID", new BasicSqlType(new RelDataTypeSystemImpl() {}, SqlTypeName.INTEGER));
        builder.add("NAME", new BasicSqlType(new RelDataTypeSystemImpl() {}, SqlTypeName.CHAR));
        builder.add("COMPANY", new BasicSqlType(new RelDataTypeSystemImpl() {}, SqlTypeName.CHAR));
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
