package drop.date_test

import dsy.config.configs
import dsy.utils.SparkUtils
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

object test {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession.Builder对象
    val spark: SparkSession =
      SparkUtils.createSparkSession(this.getClass)


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

    new_data.show(10, truncate = false)
    new_data.printSchema()

    Thread.sleep(1000000000)

    //任务结束，关闭资源
    spark.stop()
  }
}
