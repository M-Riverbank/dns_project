package dsy.tools

import dsy.meta.read.impl.{HBaseReadMeta, HDFSReadMeta, HiveReadMeta}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

/**
 * 读取数据工具类
 */
class readDataTools(RuleMap: Map[String, String], spark: SparkSession) {


  /**
   * 解析 RuleMap 封装 hdfsMeta 读取 hdfs 数据返回 DF
   *
   * @return 读取到的 HDFS DF
   */
  def readHdfs: DataFrame = {
    //封装标签规则中数据源的信息至 HDFSMeta 对象中
    val hdfsReadMeta: HDFSReadMeta = HDFSReadMeta.getObject(RuleMap)
    //读取数据
    val reader: DataFrameReader = spark
      .read
      .format(hdfsReadMeta.format)
    if (hdfsReadMeta.optionsMap.nonEmpty) {
      //option 写入
      hdfsReadMeta.optionsMap
        .foreach {
          keyValue =>
            reader.option(keyValue._1, keyValue._2)
        }
    }
    reader.load(hdfsReadMeta.hdfsAddress) //加载数据
  }


  /**
   * 解析 RuleMap 封装 hbaseMeta 读取 hbase 数据返回 DF
   *
   * @return 读取到的 Hbase DF
   */
  def readHbase: DataFrame = {
    //封装标签规则中数据源的信息至 HBaseMeta 对象中
    val hbaseReadMeta: HBaseReadMeta = HBaseReadMeta.getObject(RuleMap)
    //读取数据
    HbaseTools
      .read(
        spark,
        zkHosts = hbaseReadMeta.zkHosts,
        zkPort = hbaseReadMeta.zkPort,
        table = hbaseReadMeta.hbaseTable,
        family = hbaseReadMeta.family,
        fields = hbaseReadMeta.selectFieldNames
      )
  }


  /**
   * 解析 RuleMap 封装 hiveMeta 读取 hive 数据返回 DF
   *
   * @return 读取到的 hive DF
   */
  def readHive: DataFrame = {
    //封装标签规则中数据源的信息至 HBaseMeta 对象中
    val hiveReadMeta: HiveReadMeta = HiveReadMeta.getObject(RuleMap)
    //读取数据
    spark
      .sql(hiveReadMeta.sql)
  }
}
