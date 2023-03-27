package dsy.etl

import dsy.config.configs
import dsy.utils.SparkUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object save_to_hive {
  // Spark应用程序与hadoop运行的用户,默认为本地用户
  System.setProperty("user.name", configs.HADOOP_USER_NAME)
  System.setProperty("HADOOP_USER_NAME", configs.HADOOP_USER_NAME)

  def main(args: Array[String]): Unit = {
    // 1.构建SparkSession实例对象
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass,is_Hive = true)
    //导入隐式转换

    // 2.读取数据
    val data: DataFrame = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("multiLine", "true")
      .option("encoding", "utf-8") //utf-8
      .csv(configs.LOAD_FILE)

    //添加序号列
    val new_data: DataFrame = data
      .withColumn("id", monotonically_increasing_id.cast(StringType))

    new_data
      .write
      //写入模式
      .mode(SaveMode.Overwrite)
      //保存的表
      .saveAsTable("test.test")

    spark.stop()
  }
}