package connect_hive

import dsy.utils.SparkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object connect_hive_test {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
    spark.sql("show databases").show()

    spark.stop()
  }
}
