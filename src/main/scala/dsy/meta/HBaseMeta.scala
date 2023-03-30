package dsy.meta

import dsy.tools.ruleMapUtils

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
case class HBaseMeta(
                      zkHosts: String,
                      zkPort: String,
                      hbaseTable: String,
                      family: String,
                      selectFieldNames: String
                    )

object HBaseMeta {
  /**
   * 将Map集合数据解析到HBaseMeta中
   *
   * @param ruleMap map集合
   * @return HBase 元数据对象
   */
  def getHBaseMeta(ruleMap: Map[String, String]): HBaseMeta = {
    //解析Map进行封装
    val zkHost: String = ruleMapUtils.setMetaElementValue(ruleMap,"zkHosts")
    val zkPort: String = ruleMapUtils.setMetaElementValue(ruleMap,"zkPort")
    val hbaseTable: String = ruleMapUtils.setMetaElementValue(ruleMap,"hbaseTable")
    val family: String = ruleMapUtils.setMetaElementValue(ruleMap,"family")
    val selectFieldNames: String = ruleMapUtils.setMetaElementValue(ruleMap,"selectFieldNames")

    //构建 HBaseMeta 对象
    HBaseMeta(
      zkHost,
      zkPort,
      hbaseTable,
      family,
      selectFieldNames
    )
  }
}