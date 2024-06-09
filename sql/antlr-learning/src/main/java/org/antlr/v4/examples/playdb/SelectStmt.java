package org.antlr.v4.examples.playdb;

import java.util.List;

/**
 * Simple class to save the information of select statement.
 * */
public class SelectStmt {

    public String tableName;
    public List<WhereExpr> whereExprs;

    public SelectStmt(String tableName, List<WhereExpr> whereExprs) {
        this.tableName = tableName;
        this.whereExprs = whereExprs;
    }
}
