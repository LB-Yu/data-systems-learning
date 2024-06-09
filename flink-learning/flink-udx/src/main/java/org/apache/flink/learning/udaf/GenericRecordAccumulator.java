package org.apache.flink.learning.udaf;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class GenericRecordAccumulator {

    public List<Integer> aggValues = new ArrayList<>();

    public void acc(GenericRecord record) {
        if (aggValues.isEmpty()) {
            aggValues = new ArrayList<>();
            for (int i = 0; i < record.getSize(); ++i) {
                aggValues.add(0);
            }
        }

        for (int i = 0; i < record.getSize(); i++) {
            switch (record.getAggType(i)) {
                case "SUM":
                    aggValues.set(i, aggValues.get(i) + record.getValue(i));
                    break;
                case "COUNT":
                    aggValues.set(i, aggValues.get(i) + 1);
                    break;
                default:
                    break;
            }
        }
    }

    public String getAggValues() {
        return aggValues.stream().map(String::valueOf).collect(Collectors.joining(":"));
    }
}
