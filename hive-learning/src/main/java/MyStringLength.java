import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class MyStringLength extends GenericUDF {

  public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    // check arguments length
    if (args.length != 1) {
      throw new UDFArgumentLengthException("Input arguments length error");
    }
    // check arguments type
    if (!args[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
      throw new UDFArgumentTypeException(0, "Input arguments type error");
    }
    return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
  }

  public Object evaluate(DeferredObject[] args) throws HiveException {
    if (args[0].get() == null) {
      return 0;
    }
    return args[0].get().toString().length();
  }

  public String getDisplayString(String[] strings) {
    return "";
  }
}
