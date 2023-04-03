package dsy.tools

import dsy.meta.save.impl.HiveWriteMeta
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 写入数据工具类
 */
class writeDataTools(resultDF: DataFrame, RuleMap: Map[String, String], spark: SparkSession) {


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
}
