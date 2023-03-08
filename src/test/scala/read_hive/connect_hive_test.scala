package read_hive

import dsy.utils.SparkUtils
import org.apache.spark.sql.SparkSession

object connect_hive_test {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
    spark.sql("select count(*) from test.test").show()

    spark.stop()
  }
}
