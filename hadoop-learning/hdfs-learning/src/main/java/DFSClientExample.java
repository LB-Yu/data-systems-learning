import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class DFSClientExample {

    @Test
    public void testWriteData() throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        DFSClient client = new DFSClient(new URI("hdfs://localhost:9000"), conf);
        OutputStream out = client.create("/test.txt", true);
        out.write("DFSClient".getBytes(StandardCharsets.UTF_8));
        out.close();
        client.close();
    }

    @Test
    public void testReadData() throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        DFSClient client = new DFSClient(new URI("hdfs://localhost:9000"), conf);
        DFSInputStream in = client.open("/test.txt");
        ByteBuffer bb = ByteBuffer.allocate(1024);
        int len = in.read(bb);
        System.out.println(new String(bb.array()));
        in.close();
        client.close();
    }
}
