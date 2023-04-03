package dsy.meta

import org.apache.spark.internal.Logging

trait AbstractMeta extends Logging {


  /**
   * 解析Map 封装Meta对象
   *
   * @param ruleMap 规则map
   * @return Meta对象
   */
  def getObject(ruleMap: Map[String, String]): Any


}
