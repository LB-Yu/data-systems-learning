package format;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Properties;

public class CustomTextOutputFormat<K extends WritableComparable, V extends Writable>
        extends HiveIgnoreKeyTextOutputFormat<K, V> {

  public static class CustomRecordWriter implements RecordWriter,
          JobConfigurable {

    RecordWriter writer;
    Text textWritable;

    public CustomRecordWriter(RecordWriter writer) {
      this.writer = writer;
      textWritable = new Text();
    }

    @Override
    public void write(Writable w) throws IOException {
      textWritable.set(w.toString());
      writer.write(textWritable);
    }

    @Override
    public void close(boolean abort) throws IOException {
      writer.close(abort);
    }

    @Override
    public void configure(JobConf job) { }
  }

  @Override
  public RecordWriter getHiveRecordWriter(JobConf jc,
                                          Path finalOutPath,
                                          Class<? extends Writable> valueClass,
                                          boolean isCompressed,
                                          Properties tableProperties,
                                          Progressable progress) throws IOException {
    CustomRecordWriter writer = new CustomRecordWriter(
            super.getHiveRecordWriter(jc, finalOutPath, Text.class,
                    isCompressed, tableProperties, progress));
    writer.configure(jc);
    return writer;
  }
}