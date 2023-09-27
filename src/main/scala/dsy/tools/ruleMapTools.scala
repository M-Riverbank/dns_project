package dsy.tools

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

object ruleMapTools extends Logging {


  /**
   * 解析 mysql rule 字段,封装Map返回
   *
   * @param mysqlDF 读取到的 mysqlDF
   * @param inORout rule字段名
   * @return 封装好的规则 Map
   */
  def GetRulesMap(mysqlDF: DataFrame, inORout: String): Map[String, String] = {
    //a.获取规则，解析封装
    val RuleMap: Map[String, String] = mysqlDF
      .head //返回Row对象
      .getAs[String](inORout)
      .split("\\n")
      .map { line =>
        val result = line.trim.split("=")
        val key = result(0)
        val value = result.drop(1).mkString("=")
        (key, value)
      }.toMap
    logWarning(s"==================< ${RuleMap.mkString(",")} >==================")
    //返回
    RuleMap
  }


}
