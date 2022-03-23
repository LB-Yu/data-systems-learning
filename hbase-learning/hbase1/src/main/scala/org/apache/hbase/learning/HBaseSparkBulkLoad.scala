package org.apache.hbase.learning

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hbase.bulkload.{BulkLoadPartitioner, KeyFamilyQualifier}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * A simple example shows how to use spark to perform bulk load of hbase.
 * */
object HBaseSparkBulkLoad {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: HBaseSparkBulkLoad <tableName> <columnFamily> <stagingFolder>")
      System.exit(1)
    }
    val tableName = args(0)
    val columnFamily = args(1)
    val stagingFolder = args(2)

    // get table info
    val hConf = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection(hConf)
    val table = connection.getTable(TableName.valueOf(tableName))
    val regionLocator = connection.getRegionLocator(TableName.valueOf(tableName))

    val sConf = new SparkConf().setAppName("bulkload")
    sConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sConf)

    // simple data to bulk load to hbase
    val rdd = sc.parallelize(Array(
      (Bytes.toBytes("4"), Bytes.toBytes(columnFamily), Bytes.toBytes("4"), Bytes.toBytes("1")),
      (Bytes.toBytes("1"), Bytes.toBytes(columnFamily), Bytes.toBytes("2"), Bytes.toBytes("2"))
    )).repartition(2) // random partition to simulate a real scene

    // parse the source data and repartition and sort by row key, family, qualify
    val sortedRdd = rdd.map(line => {
      val keyFamilyQualifier = new KeyFamilyQualifier(line._1, line._2, line._3)
      val value = line._4
      (keyFamilyQualifier, value)
    }).repartitionAndSortWithinPartitions(new BulkLoadPartitioner(regionLocator.getStartKeys))
    // reformat the data so that we can save as HFileOutputFormat2
    val hfileRdd = sortedRdd.map(line => {
      val rowKey = new ImmutableBytesWritable(line._1.rowKey)
      val keyValue = new KeyValue(line._1.rowKey, line._1.family, line._1.qualifier, line._2)
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
