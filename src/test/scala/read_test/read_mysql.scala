package read_test

import dsy.config.configs
import dsy.utils.SparkUtils
import org.apache.spark.sql.SparkSession

object read_mysql {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
    spark.read
      .format("jdbc")
      .option("driver", configs.MYSQL_JDBC_DRIVER)
      .option("url", configs.MYSQL_JDBC_URL)
      .option("dbtable", configs.sql(1))
      .option("user", configs.MYSQL_JDBC_USERNAME)
      .option("password", configs.MYSQL_JDBC_PASSWORD)
      .load
      .show
  }
}
