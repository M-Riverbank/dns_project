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


  /**
   * 获取返回 Meta 对象成员值,空则异常抛出
   *
   * @param ruleMap 规则 map
   * @param key     获取 value 的 key
   * @return Meta 对象成员值
   */
  def setMetaElementValue(ruleMap: Map[String, String], key: String): String = {
    val value: String = ruleMap(key)
    if (value == null) new RuntimeException(s"必须提供 $key 属性值")
    value
  }

}
