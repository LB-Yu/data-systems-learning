package org.apache.calcite.example.overall;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;

import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleDataContext implements DataContext {

  private final SchemaPlus schema;

  public SimpleDataContext(SchemaPlus schema) {
    this.schema = schema;
  }

  @Override
  public SchemaPlus getRootSchema() {
    return schema;
  }

  @Override
  public JavaTypeFactory getTypeFactory() {
    return new JavaTypeFactoryImpl();
  }

  @Override
  public QueryProvider getQueryProvider() {
    throw new RuntimeException("un");
  }

  @Override
  public Object get(String name) {
    if (Variable.CANCEL_FLAG.camelName.equals(name)) {
      return new AtomicBoolean(false);
    }
    return null;
  }
}
