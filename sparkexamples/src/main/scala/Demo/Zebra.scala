package Demo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Zebra {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Wordcount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val input= sc.textFile("/sas/sas")
 val words = input.flatMap { line =>line.split (" ") } 
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
    counts.saveAsTextFile("/home/cloudera/output5")
  }
}