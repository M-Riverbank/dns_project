package read_test

import dsy.config.configs
import dsy.meta.HDFSMeta
import dsy.tools.ruleMapUtils
import dsy.utils.SparkUtils
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object read_mysql {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
    import spark.implicits._

    val mysqlDF: Dataset[Row] = spark.read
      .format("jdbc")
      .option("driver", configs.MYSQL_JDBC_DRIVER)
      .option("url", configs.MYSQL_JDBC_URL)
      .option("dbtable", configs.MYSQL_TABLE) //configs.sql(id)
      .option("user", configs.MYSQL_JDBC_USERNAME)
      .option("password", configs.MYSQL_JDBC_PASSWORD)
      .load
      .where($"id" === 100)

    mysqlDF.show
    mysqlDF.printSchema()


    //a.获取规则，解析封装
    val RuleMap: Map[String, String] =
      ruleMapUtils.GetRulesMap(mysqlDF, configs.INPUT_SOURCE_FILE_NAME)

    RuleMap.foreach(println)
    val hdfs = HDFSMeta.getHDFSMeta(RuleMap)
    println(hdfs.hdfsAddress)
    println(hdfs.format)
    hdfs.optionsMap.foreach {
      x =>
        println(x._1, x._2)
    }
  }
}
