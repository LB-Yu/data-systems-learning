package org.apache.calcite.adapter.hbase;

import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.junit.Test;

public class CodeGenTest {

    @Test
    public void testBlock() {
        BlockBuilder builder = new BlockBuilder();
        Expression field = builder.append("field", Expressions.constant(1));

        System.out.println(Expressions.toString(field));
        BlockStatement blockStatement = builder.toBlock();
        System.out.println(blockStatement);
    }
}
