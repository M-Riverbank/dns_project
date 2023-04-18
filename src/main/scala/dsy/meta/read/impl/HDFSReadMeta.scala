package dsy.meta.read.impl

import dsy.meta.read.AbstractReadMeta
import dsy.tools.ruleMapTools

/**
 * HDFS 元数据解析存储，具体数据字段格式如下所示：
 *
 * inType=hdfs
 *
 * @param hdfsAddress hdfsAddress=hdfs://主机名:9000/xxx(本地测试可以填本地文件系统路径)
 * @param format      format=csv(文件格式，必须指定)
 * @param optionsMap  参数设置例如:header=true,multiLine=true,encoding=utf-8
 */
case class HDFSReadMeta(
                     hdfsAddress: String,
                     format: String,
                     optionsMap: Map[String, String]
                   )

object HDFSReadMeta extends AbstractReadMeta{


  /**
   * 将Map集合数据解析到 HDFSReadMeta 中封装返回
   *
   * @param ruleMap 规则map集合
   * @return 读取 hdfs 数据源封装对象 元数据对象
   */
  override def getObject(ruleMap: Map[String, String]): HDFSReadMeta = {
    //解析Map进行封装
    val hdfsAddress: String = setMetaElementValue(ruleMap, "hdfsAddress")
    val format: String = setMetaElementValue(ruleMap, "format")
    val optionsMap: Map[String, String] = ruleMap
      .-("inType")
      .-("hdfsAddress")
      .-("format")

    //封装元数据对象
    HDFSReadMeta(hdfsAddress, format, optionsMap)
  }


}

