package dsy.tools

import dsy.meta.save.impl.HiveWriteMeta
import dsy.meta.save.impl.HbaseWriteMeta
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 写入数据工具类
 */
class writeDataTools(resultDF: DataFrame, RuleMap: Map[String, String]) {


  /**
   * 保存结果数据至Hive
   */
  def writeHive(): Unit = {
    //封装标签规则中数据源的信息至 HiveMeta 对象中
    val hdfsWriteMeta: HiveWriteMeta = HiveWriteMeta.getObject(RuleMap)
    //保存结果数据
    resultDF
      .write
      .mode(hdfsWriteMeta.saveMode)
      .save(hdfsWriteMeta.tableName)
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
    new RuntimeException(s"未实现的数据输出 ${RuleMap("inType")}")
  }


}
