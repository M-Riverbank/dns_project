package dsy.read_hdfs

import dsy.config.configs
import dsy.utils.SparkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object read_hdfs_write_to_hbase {
  def main(args: Array[String]): Unit = {
    // 1.构建SparkSession实例对象
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
    //导入隐式转换
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

    //添加序号列
    val new_data: DataFrame = data
      .withColumn("id", monotonically_increasing_id)
    //    new_data.show(10,truncate = false)
    //    new_data.printSchema()
    /*
    root
       |-- client_ip: string (nullable = true)
       |-- domain: string (nullable = true)
       |-- time: string (nullable = true)
       |-- target_ip: string (nullable = true)
       |-- rcode: string (nullable = true)
       |-- query_type: string (nullable = true)
       |-- authority_record: string (nullable = true)
       |-- add_msg: string (nullable = true)
       |-- dns_ip: string (nullable = true)
       |-- id: long (nullable = false)
     */

    /*
    new_data.select(
      $"id",
      $"client_ip",
      $"domain",
      $"time",
      $"target_ip",
      $"rcode",
      $"query_type",
      $"authority_record",
      $"add_msg",
      $"dns_ip"
    ).show(10,truncate = false)
     */

    spark.stop()
  }
}
