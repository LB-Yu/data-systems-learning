import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.util

object InvertedIndex {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      print("Usage: InvertedIndex <file>")
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("InvertedIndex")
    val sc : SparkContext = new SparkContext(sparkConf)
    val textRDD : RDD[String] = sc.textFile(args(0))
    val pairRDD : RDD[(String, Int)] = textRDD.flatMap(line => {
      val items = line.split("\\.")
      val id = Integer.parseInt(items(0))
      val words = items(1).trim().split("\\s+")
      for(i <- 0 until words.length) yield (words(i), id)
    })
    val invertedIndexRDD = pairRDD.groupByKey().map(p => {
      val set = new util.HashSet[Int]()
      p._2.foreach(i => set.add(i))
      (p._1, set)
    })
    invertedIndexRDD.collect().foreach(println)
    sc.stop()
  }
}
