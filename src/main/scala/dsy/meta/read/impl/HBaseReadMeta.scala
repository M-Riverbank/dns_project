package dsy.meta.read.impl

import dsy.meta.read.AbstractReadMeta
import dsy.tools.ruleMapTools

/**
 * HBase 元数据解析存储，具体数据字段格式如下所示：
 *
 * inType=hbase
 *
 * @param zkHosts          zkHosts=dsy(zookeeper主机)
 * @param zkPort           zkPort=2181(zookeeper端口)
 * @param hbaseTable       hbaseTable=tbl_tag_users(表名)
 * @param family           family=detail(类簇)
 * @param selectFieldNames selectFieldNames=id,gender(读取的字段名列表,逗号分隔)
 */
case class HBaseReadMeta(
                          zkHosts: String,
                          zkPort: String,
                          hbaseTable: String,
                          family: String,
                          selectFieldNames: String
                        )

object HBaseReadMeta extends AbstractReadMeta {


  /**
   * 解析 Map 集合
   *
   * @param ruleMap map集合
   * @return 读取 hbase 数据源封装对象 元数据对象
   */
  override def getObject(ruleMap: Map[String, String]): HBaseReadMeta = {
    //解析Map进行封装
    val zkHost: String = ruleMapTools.setMetaElementValue(ruleMap, "zkHosts")
    val zkPort: String = ruleMapTools.setMetaElementValue(ruleMap, "zkPort")
    val hbaseTable: String = ruleMapTools.setMetaElementValue(ruleMap, "hbaseTable")
    val family: String = ruleMapTools.setMetaElementValue(ruleMap, "family")
    val selectFieldNames: String = ruleMapTools.setMetaElementValue(ruleMap, "selectFieldNames")

    //构建对象返回
    HBaseReadMeta(
      zkHost,
      zkPort,
      hbaseTable,
      family,
      selectFieldNames
    )
  }

}