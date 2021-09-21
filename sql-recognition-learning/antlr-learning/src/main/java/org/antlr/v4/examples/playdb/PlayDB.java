package org.antlr.v4.examples.playdb;

import org.antlr.v4.examples.playdb.parser.SQLiteLexer;
import org.antlr.v4.examples.playdb.parser.SQLiteParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.HashMap;
import java.util.Map;

public class PlayDB {

    private Map<String, String> region2DB = new HashMap<>();

    public PlayDB() {
        region2DB.put("SDYT", "db1");
        region2DB.put("BJHD", "db2");
        region2DB.put("FJXM", "db3");
        region2DB.put("SZLG", "db4");
    }

    public String getDBName(String sql) {
        SQLiteLexer lexer = new SQLiteLexer(CharStreams.fromString(sql));
        CommonTokenStream tokens = new CommonTokenStream(lexer);

        SQLiteParser parser = new SQLiteParser(tokens);
        ParseTree tree = parser.sql_stmt();

        System.out.println(tree.toStringTree(parser));

        SQLVisitor visitor = new SQLVisitor();
        SelectStmt select = (SelectStmt) visitor.visit(tree);

        String dbName = null;
        if (select.tableName.equals("orders")) {
            if (select.whereExprs != null) {
                for (WhereExpr expr : select.whereExprs) {
                    if (expr.columnName.equals("cust_id") || expr.columnName.equals("order_id")) {
                        String region = expr.value.substring(1, 5);
                        dbName = region2DB.get(region);
                        break;
                    }
                }
            }
        }
        return dbName;
    }

    public static void main(String[] args) {
        PlayDB playDB = new PlayDB();

        String sql = "select order_id from orders where cust_id = 'SDYT987645' and price > 200";
        String dbName = playDB.getDBName(sql);
        System.out.println("db: " + dbName);
    }
}
