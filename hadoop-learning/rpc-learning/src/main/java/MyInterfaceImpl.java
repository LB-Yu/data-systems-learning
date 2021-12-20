import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

public class MyInterfaceImpl implements MyInterface {

  @Override
  public int add(int num1, int num2) {
    return num1 + num2;
  }

  @Override
  public long getProtocolVersion(String s, long l) throws IOException {
    return MyInterface.versionID;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
    return null;
  }
}
