package dsy.meta.save

import dsy.meta.AbstractMeta

//保存元数据封装特质
abstract class AbstractWriteMeta extends AbstractMeta {


  /**
   * 解析Map 封装Meta对象
   *
   * @param ruleMap 规则map
   * @return Meta对象
   */
  override def getObject(ruleMap: Map[String, String]): Any


}
