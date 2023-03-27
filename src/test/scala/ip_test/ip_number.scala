package ip_test

import dsy.config.configs
import dsy.utils.SparkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}


object ip_number {
  def main(args: Array[String]): Unit = {
    // 1.构建SparkSession实例对象
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
    import org.apache.spark.sql.functions._
    import spark.implicits._

    // 2.读取数据
    val data: DataFrame = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("multiLine", "true")
      .option("encoding", "utf-8") //utf-8
      .load(configs.LOAD_FILE)

    //根据ip分组
    val ip_group: DataFrame = data
      .select($"client_ip".as("client_ip"))
      .groupBy($"client_ip")
      .count()

    //取前十条
    val limit_10: DataFrame = ip_group
      .sort($"count".desc)
      .limit(10)

    limit_10.show(10, truncate = false)

    Thread.sleep(1000000000)
    /*
          +--------+
          |ip_count|
          +--------+
          |  196609|
          +--------+
     */

    spark.stop()
  }
}
