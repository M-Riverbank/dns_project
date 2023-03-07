package connect_hive

import dsy.utils.SparkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object connect_hive_test {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
    val hive_data: DataFrame = spark.sql("show databases")
    hive_data.show()

    spark.stop()
  }
}
