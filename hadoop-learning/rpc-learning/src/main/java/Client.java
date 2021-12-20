import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class Client {

  public static void main(String[] args) {
    try {
      MyInterface proxy = RPC.getProxy(
              MyInterface.class,
              1L,
              new InetSocketAddress("localhost", 12345),
              new Configuration());
      int res = proxy.add(1, 2);
      System.out.println(res);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
