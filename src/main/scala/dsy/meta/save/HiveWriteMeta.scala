package dsy.meta.save

import dsy.tools.ruleMapUtils

/**
 * Hive 元数据解析存储，具体数据字段格式如下所示：
 *
 * outType=hive
 *
 * @param saveMode  保存模式(saveMode=overwrite)
 * @param tableName 保存的表名(tableName=test.test)
 */
case class HiveWriteMeta(
                          saveMode: String,
                          tableName: String
                        )

object HiveWriteMeta {


  /**
   * 将Map集合数据解析到 HDFSReadMeta 中封装返回
   *
   * @param ruleMap 规则map集合
   * @return 写入 Hive 数据源封装对象 元数据对象
   */
  def getObject(ruleMap: Map[String, String]): HiveWriteMeta = {
    //解析Map进行封装
    val saveMode: String = ruleMapUtils.setMetaElementValue(ruleMap, "saveMode").toLowerCase
    val tableName: String = ruleMapUtils.setMetaElementValue(ruleMap, "tableName")

    //封装元数据对象
    HiveWriteMeta(saveMode, tableName)
  }


}
