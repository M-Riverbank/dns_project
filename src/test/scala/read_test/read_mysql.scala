package read_test

import dsy.config.configs
import dsy.tools.metaUtils
import dsy.tools.metaUtils.logWarning
import dsy.utils.SparkUtils
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object read_mysql {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val mysqlDF: Dataset[Row] = spark.read
      .format("jdbc")
      .option("driver", configs.MYSQL_JDBC_DRIVER)
      .option("url", configs.MYSQL_JDBC_URL)
      .option("dbtable", configs.MYSQL_TABLE)//configs.sql(id)
      .option("user", configs.MYSQL_JDBC_USERNAME)
      .option("password", configs.MYSQL_JDBC_PASSWORD)
      .load
      .where($"id"===100)

    mysqlDF.show
    mysqlDF.printSchema()


    //a.获取规则，解析封装

    val RuleMap: Map[String, String] = mysqlDF
      .head //返回Row对象
      .getAs[String](configs.INPUT_SOURCE_FILE_NAME)
      .split("\\n")
      .map { line =>
        val Array(key, value) = line.trim.split("=")
        (key, value)
      }.toMap
    RuleMap.foreach(println)

  }
}
