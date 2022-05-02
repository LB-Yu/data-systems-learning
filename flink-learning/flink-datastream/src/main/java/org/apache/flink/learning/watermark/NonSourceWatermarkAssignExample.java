package org.apache.flink.learning.watermark;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WatermarkExample {

  public static void main(String[] args) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStreamSource<String> streamSource = env.socketTextStream(
            "localhost", 7777);

  }
}
