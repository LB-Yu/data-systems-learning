package org.apache.flink.streaming.examples.partition;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class PartitionExampleJob {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    String textPath = PartitionExampleJob.class.getResource("/text_file.txt").getPath();
    DataStream<String> textStream = env.readTextFile(textPath);

    DataStream<String> mappedStream = textStream.map(new MyMap()).setParallelism(4);
    DataStream<String> flatMappedStream = mappedStream.flatMap(new MyFlatMap());

    System.out.println(textStream.getParallelism());
    System.out.println(mappedStream.getParallelism());
    System.out.println(flatMappedStream.getParallelism());
    System.out.println(env.getParallelism());

    env.execute("Partition example job");
  }

  public static class MyMap extends RichMapFunction<String, String> {

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      System.out.println("getIndexOfThisSubtask(): " + getRuntimeContext().getIndexOfThisSubtask() +
              ", getTaskName(): " + getRuntimeContext().getTaskName() +
              ", getNumberOfParallelSubtasks(): " + getRuntimeContext().getNumberOfParallelSubtasks());
    }

    @Override
    public String map(String s) throws Exception {
      return s;
    }
  }

  public static class MyFlatMap extends RichFlatMapFunction<String, String> {

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      System.out.println("getIndexOfThisSubtask(): " + getRuntimeContext().getIndexOfThisSubtask() +
              ", getTaskName(): " + getRuntimeContext().getTaskName() +
              ", getNumberOfParallelSubtasks(): " + getRuntimeContext().getNumberOfParallelSubtasks());
    }

    @Override
    public void flatMap(String s, Collector<String> collector) throws Exception {
      collector.collect(s);
    }
  }
}
