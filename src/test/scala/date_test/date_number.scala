package date_test

import dsy.config.configs
import dsy.utils.SparkUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 看测试数据日期有多少种
 */
object date_number {
  def main(args: Array[String]): Unit = {
    // 1.构建SparkSession实例对象
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 2.读取数据
    val data: DataFrame = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("multiLine", "true")
      .option("encoding", "utf-8") //utf-8
      .load(configs.LOAD_FILE)

    data
      .select($"time".substr(0,10).cast(LongType).as("time"))
      .groupBy($"time")
      .count()
      .orderBy($"time")
      .show(24)
/*
        +----------------+------+
        |            time| count|
        +----------------+------+
        |2022 08 02 10 58|168239|
        |2022 08 02 10 59|285801|
        |2022 08 02 11 00|285699|
        |2022 08 02 11 01|285699|
        |2022 08 02 11 02|285700|
        |2022 08 02 11 03|285699|
        |2022 08 02 11 04|285702|
        |2022 08 02 11 05|285699|
        |2022 08 02 11 06|285800|
        |2022 08 02 11 07|285699|
        |2022 08 02 11 08|285700|
        |2022 08 02 11 09|285696|
        |2022 08 02 11 10|285693|
        |2022 08 02 11 11|285712|
        |2022 08 02 11 12|285697|
        |2022 08 02 11 13|285801|
        |2022 08 02 11 14|285699|
        |2022 08 02 11 15|285697|
        |2022 08 02 11 16|285696|
        |2022 08 02 11 17|285704|
        +----------------+------+
        only showing top 20 rows
 */

    spark.stop()
  }
}
