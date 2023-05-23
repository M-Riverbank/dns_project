package dsy.tools

import dsy.config.configs
import dsy.meta.save.impl.{HbaseWriteMeta, HiveWriteMeta, MysqlWriteMeta}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 写入数据工具类
 */
class writeDataTools(resultDF: DataFrame, RuleMap: Map[String, String]) {


  /**
   * 保存结果数据至Hive
   */
  def writeHive(): Unit = {
    //封装标签规则中数据源的信息至 HiveMeta 对象中
    val hiveWriteMeta: HiveWriteMeta = HiveWriteMeta.getObject(RuleMap)
    //保存结果数据
    resultDF
      .write
      .mode(hiveWriteMeta.saveMode)
      .saveAsTable(hiveWriteMeta.tableName)
  }

  /**
   * 保存结果数据至 Hbase
   */
  def writeHbase(): Unit = {
    //封装标签规则中数据源的信息至 HiveMeta 对象中
    val hbaseWriteMeta: HbaseWriteMeta = HbaseWriteMeta.getObject(RuleMap)
    //保存结果数据
    HbaseTools
      .write(
        resultDF,
        hbaseWriteMeta.zkHosts,
        hbaseWriteMeta.zkPort,
        hbaseWriteMeta.hbaseTable,
        hbaseWriteMeta.family,
        hbaseWriteMeta.rowKeyColumn
      )
  }

  /**
   * 保存结果数据至 mysql
   */
  def writeMysql(): Unit = {
    //封装标签规则中数据源的信息至 HiveMeta 对象中
    val mysqlWriteMeta: MysqlWriteMeta = MysqlWriteMeta.getObject(RuleMap)
    //保存结果数据
    resultDF.write.format("jdbc")
      .mode(mysqlWriteMeta.saveMode)
      .option("driver", configs.MYSQL_JDBC_DRIVER)
      .option("url", configs.MYSQL_JDBC_URL)
      .option("dbtable", mysqlWriteMeta.tableName)
      .option("user", configs.MYSQL_JDBC_USERNAME)
      .option("password", configs.MYSQL_JDBC_PASSWORD)
      .save()
  }


}
