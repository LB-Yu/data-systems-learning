package org.apache.flink.learning.udaf;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class GenericRecord {

    private final List<String> aggType;
    private final List<Integer> values;

    public GenericRecord(String metricsData) {
        this.aggType = new ArrayList<>();
        this.values = new ArrayList<>();

        String[] items = metricsData.split("\\|");
        for (String s : items) {
            String[] item = s.split(":");
            aggType.add(item[0]);
            values.add(Integer.parseInt(item[1]));
        }
    }

    public String getMetricsData() {
        return values.stream().map(String::valueOf).collect(Collectors.joining(":"));
    }

    public String getAggType(int i) {
        return aggType.get(i);
    }

    public Integer getValue(int i) {
        return values.get(i);
    }

    public int getSize() {
        return aggType.size();
    }
}
