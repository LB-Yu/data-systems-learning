package org.antlr.v4.examples.playdb;

/**
 * Simple class to save the information of where expression.
 * */
public class WhereExpr {

    public String columnName;
    public String op;
    public String value;

    public WhereExpr(String columnName, String op, String value) {
        this.columnName = columnName;
        this.op = op;
        this.value = value;
    }
}
