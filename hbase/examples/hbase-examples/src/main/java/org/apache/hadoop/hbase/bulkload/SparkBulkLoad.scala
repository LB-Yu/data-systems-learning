package org.apache.hadoop.hbase.bulkload

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Yu Liebing
  */
object SparkBulkLoad {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: SparkBulkLoad + {stagingFolder}")
    }

    val tableName = "bulkload-table-test"
    val columnFamily = "f"
    val stagingFolder = args(0)

    // get table info
    val hConf = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection(hConf)
    val table = connection.getTable(TableName.valueOf(tableName))
    val regionLocator = connection.getRegionLocator(TableName.valueOf(tableName))

    val sConf = new SparkConf().setAppName("bulkload").setMaster("local")
    sConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sConf)
    // simple data to bulk load to hbase
    val rdd = sc.parallelize(Array(
      (Bytes.toBytes("1"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("4"), Bytes.toBytes("1")))),
      (Bytes.toBytes("4"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("2"), Bytes.toBytes("2")))),
      (Bytes.toBytes("2"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("8"), Bytes.toBytes("3")))),
      (Bytes.toBytes("3"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("9"), Bytes.toBytes("4")))),
      (Bytes.toBytes("5"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("5"))))
    )).repartition(2) // random partition to simulate a real scene
    // parse the source data and repartition and sort by row key, family, qualify
    val sortedRdd = rdd.map(line => {
      val keyFamilyQualifier = new KeyFamilyQualifier(line._1, line._2(0)._1, line._2(0)._2)
      val value = line._2(0)._3
      (keyFamilyQualifier, value)
    }).repartitionAndSortWithinPartitions(new BulkLoadPartitioner(regionLocator.getStartKeys))
    // reformat the data so that we can save as HFileOutputFormat2
    val hfileRdd = sortedRdd.map(line => {
        val rowKey = new ImmutableBytesWritable(line._1.getRowKey)
        val keyValue = new hbase.KeyValue(line._1.getRowKey, line._1.getFamily, line._1.getQualifier, line._2)
        (rowKey, keyValue)
      })
    // save the rdd as hfile
    hfileRdd.saveAsNewAPIHadoopFile(
      stagingFolder,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2], hConf)
    // load the hfile from hdfs
    val loader = new LoadIncrementalHFiles(hConf)
    loader.doBulkLoad(new Path(stagingFolder), connection.getAdmin, table, regionLocator)
  }
}