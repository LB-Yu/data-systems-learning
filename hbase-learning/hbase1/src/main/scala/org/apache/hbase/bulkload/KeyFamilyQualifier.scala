package org.apache.hbase.bulkload

import java.io.Serializable

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes

/**
 * This is the key to be used for sorting and shuffling.
 *
 * We will only partition on the rowKey but we will sort on all three
 *
 * @param rowKey    Record RowKey
 * @param family    Record ColumnFamily
 * @param qualifier Cell Qualifier
 */
@InterfaceAudience.Public
class KeyFamilyQualifier(val rowKey:Array[Byte], val family:Array[Byte], val qualifier:Array[Byte])
  extends Comparable[KeyFamilyQualifier] with Serializable {
  override def compareTo(o: KeyFamilyQualifier): Int = {
    var result = Bytes.compareTo(rowKey, o.rowKey)
    if (result == 0) {
      result = Bytes.compareTo(family, o.family)
      if (result == 0) result = Bytes.compareTo(qualifier, o.qualifier)
    }
    result
  }
  override def toString: String = {
    Bytes.toString(rowKey) + ":" + Bytes.toString(family) + ":" + Bytes.toString(qualifier)
  }
}
