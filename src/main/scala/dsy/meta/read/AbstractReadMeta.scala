package dsy.meta.read

import dsy.meta.AbstractMeta

// 读取元数据封装特质
trait AbstractReadMeta extends AbstractMeta {


  /**
   * 解析Map 封装Meta对象
   *
   * @param ruleMap 规则map
   * @return Meta对象
   */
  override def getObject(ruleMap: Map[String, String]): Any


}
