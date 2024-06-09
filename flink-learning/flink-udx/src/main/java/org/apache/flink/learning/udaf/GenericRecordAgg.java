package org.apache.flink.learning.udaf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.AggregateFunction;


public class GenericRecordAgg
        extends AggregateFunction<String, GenericRecordAccumulator> {

    @Override
    public String getValue(GenericRecordAccumulator acc) {
        return acc.getAggValues();
    }

    @Override
    public GenericRecordAccumulator createAccumulator() {
        return new GenericRecordAccumulator();
    }

    public void accumulate(GenericRecordAccumulator acc, String metricData) {
        GenericRecord record = new GenericRecord(metricData);
        acc.acc(record);
    }
}
