import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

public class MyUDTF extends GenericUDTF {

  private final List<String> outList = new ArrayList<>();

  public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {List<String> fieldNames = new ArrayList<>();
    List<ObjectInspector> fieldOIs = new ArrayList<>();
    fieldNames.add("lineToWord");
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
  }

  @Override
  public void process(Object[] args) throws HiveException {
    String arg = args[0].toString();
    String splitKey = args[1].toString();
    String[] fields = arg.split(splitKey);
    for (String field : fields) {
      outList.clear();
      outList.add(field);
      forward(outList);
    }
  }

  @Override
  public void close() throws HiveException {

  }
}
