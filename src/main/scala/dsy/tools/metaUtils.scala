package dsy.tools

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

object metaUtils extends Logging {
  def GetRulesMap(mysqlDF: DataFrame, inORout: String): Map[String, String] = {
    //a.获取规则，解析封装
    val RuleMap: Map[String, String] = mysqlDF
      .head //返回Row对象
      .getAs[String](inORout)
      .split("\\n")
      .map { line =>
        val Array(key, value) = line.trim.split("=")
        (key, value)
      }.toMap
    logWarning(s"==================< ${RuleMap.mkString(",")} >==================")
    //返回
    RuleMap
  }
}
