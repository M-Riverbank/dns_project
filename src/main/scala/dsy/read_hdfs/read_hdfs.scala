package dsy.read_hdfs

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object read_hdfs {
  def main(args: Array[String]): Unit = {
    // 1.构建SparkSession实例对象
    val spark: SparkSession = SparkSession
      .builder() // 使用建造者模式构建对象
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[*]")
      //设置shuffle分区数，默认200
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    val data: DataFrame = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("multiLine", "true")
      .option("encoding", "utf-8") //utf-8
      //"D:\\data\\dns_data_test.csv"
      //"/soft/data/DNS_DATA/dns_data_test.csv"
      //"hdfs://dsy:9000/dns_data/dns_data_test.csv"
      .load("D:\\data\\dns_data_test.csv")

    data.show(10, truncate = false)
    println(data.count())
    data.printSchema()

    spark.stop()
  }
}
