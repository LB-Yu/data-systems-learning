import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PhoneFlowCount {

  public static class PhoneDataMapper extends Mapper<Object, Text, Text, FlowBean> {

    private final Text phoneText = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] items = value.toString().split("\t");
      String phoneNumber = items[1];
      phoneText.set(phoneNumber);
      long upFlow = Long.parseLong(items[8]);
      long downFlow = Long.parseLong(items[9]);
      context.write(phoneText, new FlowBean(upFlow, downFlow));
    }
  }

  public static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
      long totalUpFlow = 0;
      long totalDownFlow = 0;
      for (FlowBean bean : values) {
        totalUpFlow += bean.getUpFlow();
        totalDownFlow += bean.getDownFlow();
      }
      FlowBean result = new FlowBean(totalUpFlow, totalDownFlow, totalUpFlow + totalDownFlow);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: PhoneFlowCount <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "phone flow count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(PhoneDataMapper.class);
    job.setReducerClass(FlowCountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FlowBean.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
            new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class FlowBean implements Writable {

    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public FlowBean() { }

    public FlowBean(long upFlow, long downFlow) {
      this.upFlow = upFlow;
      this.downFlow = downFlow;
    }

    public FlowBean(long upFlow, long downFlow, long sumFlow) {
      this.upFlow = upFlow;
      this.downFlow = downFlow;
      this.sumFlow = sumFlow;
    }

    public void set(long upFlow, long downFlow) {
      this.upFlow = upFlow;
      this.downFlow = downFlow;
    }

    public long getUpFlow() {
      return upFlow;
    }

    public long getDownFlow() {
      return downFlow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeLong(upFlow);
      out.writeLong(downFlow);
      out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.upFlow = in.readLong();
      this.downFlow = in.readLong();
      this.sumFlow = in.readLong();
    }

    @Override
    public String toString() {
      return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }
  }
}
