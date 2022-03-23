package format;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class CustomTextInputFormat implements JobConfigurable, InputFormat<LongWritable, Text> {

  private final TextInputFormat format;

  public CustomTextInputFormat() {
    format = new TextInputFormat();
  }

  @Override
  public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
    return format.getSplits(jobConf, numSplits);
  }

  @Override
  public RecordReader<LongWritable, Text> getRecordReader(
          InputSplit inputSplit,
          JobConf jobConf,
          Reporter reporter) throws IOException {
    reporter.setStatus(inputSplit.toString());
    CustomLineRecordReader reader = new CustomLineRecordReader(
            new LineRecordReader(jobConf, (FileSplit) inputSplit));
    reader.configure(jobConf);
    return reader;
  }

  @Override
  public void configure(JobConf jobConf) {
    format.configure(jobConf);
  }

  public static class CustomLineRecordReader implements
          RecordReader<LongWritable, Text>, JobConfigurable {

    private final LineRecordReader reader;
    private final Text text;

    public CustomLineRecordReader(LineRecordReader reader) {
      this.reader = reader;
      this.text = reader.createValue();
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }

    @Override
    public LongWritable createKey() {
      return reader.createKey();
    }

    @Override
    public Text createValue() {
      return new Text();
    }

    @Override
    public long getPos() throws IOException {
      return reader.getPos();
    }

    @Override
    public float getProgress() throws IOException {
      return reader.getProgress();
    }

    @Override
    public boolean next(LongWritable key, Text value) throws IOException {
      while (reader.next(key, text)) {
        String decodeStr = text.toString().replaceAll("\\s+", "");
        if (decodeStr.length() > 0) {
          value.set(decodeStr);
          return true;
        }
      }
      return false;
    }

    @Override
    public void configure(JobConf job) { }
  }
}
