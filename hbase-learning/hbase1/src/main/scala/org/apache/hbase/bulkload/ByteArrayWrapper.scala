package org.apache.hbase.bulkload

import java.io.Serializable

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes

/**
 * This is a wrapper over a byte array so it can work as
 * a key in a hashMap
 *
 * @param value The Byte Array value
 */
@InterfaceAudience.Public
class ByteArrayWrapper (var value:Array[Byte])
  extends Comparable[ByteArrayWrapper] with Serializable {
  override def compareTo(valueOther: ByteArrayWrapper): Int = {
    Bytes.compareTo(value,valueOther.value)
  }
  override def equals(o2: Any): Boolean = {
    o2 match {
      case wrapper: ByteArrayWrapper =>
        Bytes.equals(value, wrapper.value)
      case _ =>
        false
    }
  }
  override def hashCode():Int = {
    Bytes.hashCode(value)
  }
}
