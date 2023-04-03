package dsy.meta.read.impl

import dsy.meta.read.AbstractReadMeta
import dsy.tools.ruleMapTools

/**
 * Hive 元数据解析存储
 *
 * outType=hive
 *
 * @param sql 读取 hive 的 sql 语句(hiveSql=xxx)
 */
case class HiveReadMeta(
                         sql: String
                       )


object HiveReadMeta extends AbstractReadMeta {

  /**
   * 将Map集合数据解析到 HiveReadMeta 中封装返回
   *
   * @param ruleMap 规则map集合
   * @return 读取 hive 数据源封装对象 元数据对象
   */
  override def getObject(ruleMap: Map[String, String]): HiveReadMeta = {
    //解析Map进行封装
    val hiveSql: String = ruleMapTools.setMetaElementValue(ruleMap, "hiveSql")

    //封装元数据对象
    HiveReadMeta(hiveSql)
  }
}