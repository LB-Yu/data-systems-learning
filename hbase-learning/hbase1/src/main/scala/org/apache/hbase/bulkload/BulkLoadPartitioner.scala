package org.apache.hbase.bulkload

import java.util
import java.util.Comparator

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Partitioner

/**
 * A Partitioner implementation that will separate records to different
 * HBase Regions based on region splits
 *
 * @param startKeys   The start keys for the given table
 */
@InterfaceAudience.Public
class BulkLoadPartitioner(startKeys:Array[Array[Byte]])
  extends Partitioner {
  // when table not exist, startKeys = Byte[0][]
  override def numPartitions: Int = if (startKeys.length == 0) 1 else startKeys.length

  override def getPartition(key: Any): Int = {

    val comparator: Comparator[Array[Byte]] = (o1: Array[Byte], o2: Array[Byte]) => {
      Bytes.compareTo(o1, o2)
    }

    val rowKey:Array[Byte] =
      key match {
        case qualifier: KeyFamilyQualifier =>
          qualifier.rowKey
        case wrapper: ByteArrayWrapper =>
          wrapper.value
        case _ =>
          key.asInstanceOf[Array[Byte]]
      }
    var partition = util.Arrays.binarySearch(startKeys, rowKey, comparator)
    if (partition < 0)
      partition = partition * -1 + -2
    if (partition < 0)
      partition = 0
    partition
  }
}
