package org.apacge.flink.common.join;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;

public abstract class RowLookupFunction extends ProcessFunction<Row, Row> {

  protected boolean isOuterJoin;
}
