package dsy.read_hdfs

import dsy.config.configs
import dsy.config.configs.config
import dsy.utils.SparkUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object read_hdfs {
  def main(args: Array[String]): Unit = {
    // 1.构建SparkSession实例对象
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)

    val data: DataFrame = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("multiLine", "true")
      .option("encoding", "utf-8") //utf-8
      .load(configs.LOAD_FILE)

    data.show(10, truncate = false)
    println(data.count())
    data.printSchema()

    spark.stop()
  }
}
