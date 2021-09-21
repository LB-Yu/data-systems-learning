package org.apache.calcite.example.memory;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

public class MemorySchemaFactory implements SchemaFactory {

    public Schema create(SchemaPlus parentSchema, String name,
                         Map<String, Object> operand) {
        System.out.println("param1 : " + operand.get("param1"));
        System.out.println("param2 : " + operand.get("param2"));
        System.out.println("Get database " + name);
        return new MemorySchema(name);
    }
}
