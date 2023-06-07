package dsy.meta.save.impl

import dsy.meta.save.AbstractWriteMeta
import dsy.tools.ruleMapTools

/**
 * HBase 元数据解析存储，具体数据字段格式如下所示：
 *
 * outType=hbase
 *
 * @param zkHosts      zkHosts=dsy(zookeeper主机)
 * @param zkPort       zkPort=2181(zookeeper端口)
 * @param hbaseTable   hbaseTable=tbl_tag_users(表名)
 * @param family       family=detail(类簇)
 * @param rowKeyColumn RowKey字段名称
 */
case class HbaseWriteMeta(
                           zkHosts: String,
                           zkPort: String,
                           hbaseTable: String,
                           family: String,
                           rowKeyColumn: String
                         )

object HbaseWriteMeta extends AbstractWriteMeta {
  /**
   * 解析Map 封装Meta对象
   *
   * @param ruleMap 规则map
   * @return Meta对象
   */
  override def getObject(ruleMap: Map[String, String]): HbaseWriteMeta = {

    //解析Map进行封装
    val zkHost: String = setMetaElementValue(ruleMap, "zkHosts")
    val zkPort: String = setMetaElementValue(ruleMap, "zkPort")
    val hbaseTable: String = setMetaElementValue(ruleMap, "hbaseTable")
    val family: String = setMetaElementValue(ruleMap, "family")
    val rowKeyColumn: String = setMetaElementValue(ruleMap, "RowKey")

    //构建对象返回
    HbaseWriteMeta(zkHost, zkPort, hbaseTable, family, rowKeyColumn)
  }
}
