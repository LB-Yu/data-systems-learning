package org.apache.calcite.adapter.hbase;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.io.IOException;
import java.util.Map;

/**
 * Factory that creates a {@link HBaseSchema}.
 */
public class HBaseSchemaFactory implements SchemaFactory {

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        String schemaPath = (String) operand.get("schema");
        try {
            return new HBaseSchema(schemaPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
