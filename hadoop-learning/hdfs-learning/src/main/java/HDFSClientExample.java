import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;

public class HDFSClientExample {

    @Test
    public void testMkdirs() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new Configuration();

        // FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration);
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), configuration,"liebing");
        fs.mkdirs(new Path("/xiyou/huaguoshan/"));
        fs.close();
    }

    @Test
    public void testWriteData() throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf,"liebing");
        Path path = new Path("/test.txt");
        FSDataOutputStream out = fs.create(path);
        out.writeBytes("test-data");
        out.close();
        fs.close();
    }

    @Test
    public void testReadData() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf,"liebing");
        Path path = new Path("/test.txt");
        FSDataInputStream in = fs.open(path);
        ByteBuffer bb = ByteBuffer.allocate(1024);
        in.read(2, bb);
        System.out.println(new String(bb.array()));
        in.close();
        fs.close();
    }
}
