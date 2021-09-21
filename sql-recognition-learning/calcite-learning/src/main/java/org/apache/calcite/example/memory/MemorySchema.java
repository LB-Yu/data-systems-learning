package org.apache.calcite.example.memory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.apache.calcite.example.memory.function.TimeOperator;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.util.HashMap;
import java.util.Map;

public class MemorySchema extends AbstractSchema {
    private String dbName;
    public MemorySchema(String dbName) {
        this.dbName = dbName;
    }

    @Override
    public Map<String, Table> getTableMap() {
        Map<String, Table> tables = new HashMap<String, Table>();
        MemoryData.Database database = MemoryData.MAP.get(this.dbName);
        if(database == null)
            return tables;
        for(MemoryData.Table table : database.tables) {
            tables.put(table.tableName, new MemoryTable(table));
        }

        return tables;
    }

    protected Multimap<String, Function> getFunctionMultimap() {
        ImmutableMultimap<String, ScalarFunction> funcs = ScalarFunctionImpl.createAll(TimeOperator.class);
        Multimap<String, Function> functions = HashMultimap.create();
        for(String key : funcs.keySet()) {
            for(ScalarFunction func : funcs.get(key)) {
                functions.put(key, func);
            }
        }

        return functions;
    }

}
