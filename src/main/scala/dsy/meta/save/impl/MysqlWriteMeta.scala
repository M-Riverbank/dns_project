package dsy.meta.save.impl

import dsy.meta.save.AbstractWriteMeta

/**
 * Mysql 元数据解析存储，具体数据字段格式如下所示：
 *
 * outType=mysql
 *
 * @param saveMode  保存模式(saveMode=overwrite)
 * @param tableName 保存的表名(tableName=test)
 */
case class MysqlWriteMeta(
                           saveMode: String,
                           tableName: String
                         )

object MysqlWriteMeta extends AbstractWriteMeta {
  /**
   * 解析Map 封装Meta对象
   *
   * @param ruleMap 规则map
   * @return Meta对象
   */
  override def getObject(ruleMap: Map[String, String]): MysqlWriteMeta = {
    //解析Map进行封装
    val saveMode: String = setMetaElementValue(ruleMap, "saveMode").toLowerCase
    val tableName: String = setMetaElementValue(ruleMap, "tableName")

    //封装元数据对象
    MysqlWriteMeta(saveMode, tableName)
  }
}
