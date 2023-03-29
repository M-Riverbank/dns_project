package dsy.meta

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
    val zkHost: String = ruleMap("zkHosts")
    val zkPort: String = ruleMap("zkPort")
    val hbaseTable: String = ruleMap("hbaseTable")
    val family: String = ruleMap("family")
    val selectFieldNames: String = ruleMap("selectFieldNames")

    if (zkHost == null) new RuntimeException("未提供zookeeper主机")
    if (zkPort == null) new RuntimeException("未提供zookeeper端口")
    if (hbaseTable == null) new RuntimeException("未提供Hbase表名")
    if (family == null) new RuntimeException("未提供HbaseFamily")
    if (selectFieldNames == null) new RuntimeException("未提供Hbase查询字段")


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