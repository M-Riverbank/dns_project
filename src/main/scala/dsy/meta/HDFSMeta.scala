package dsy.meta

import dsy.tools.ruleMapUtils

/**
 * HDFS 元数据解析存储，具体数据字段格式如下所示：
 *
 * inType=hdfs
 *
 * @param hdfsAddress hdfsAddress=hdfs://主机名:9000/xxx(本地测试可以填本地文件系统路径)
 * @param format      format=csv(文件格式，必须指定)
 * @param optionsMap  参数设置例如:header=true,multiLine=true,encoding=utf-8
 */
case class HDFSMeta(
                     hdfsAddress: String,
                     format: String,
                     optionsMap: Map[String, String]
                   )

object HDFSMeta {


  /**
   * 将Map集合数据解析到 HDFSMeta 中封装返回
   *
   * @param ruleMap 规则map集合
   * @return HDFS元数据对象
   */
  def getHDFSMeta(ruleMap: Map[String, String]): HDFSMeta = {
    //解析Map进行封装
    val hdfsAddress: String = ruleMapUtils.setMetaElementValue(ruleMap, "hdfsAddress")
    val format: String = ruleMapUtils.setMetaElementValue(ruleMap, "format")
    val optionsMap: Map[String, String] = ruleMap
      .-("hdfsAddress")
      .-("format")

    //封装元数据对象
    HDFSMeta(hdfsAddress, format, optionsMap)
  }


}

