package read_test

import dsy.config.configs
import dsy.utils.SparkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object hdfs_test {
  def main(args: Array[String]): Unit = {
    // 1.构建SparkSession实例对象
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)

    // 2.读取数据
    val data: DataFrame = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("multiLine", "true")
      .option("encoding", "utf-8") //utf-8
      .load(configs.LOAD_FILE)

    data.show(10, truncate = false)
    println(data.count)
    println(data.dropDuplicates.count)
    data.printSchema()
/*
    +-------------+-------------------------------------------------------------+--------------+---------------+-----+----------+-------------------------------------------------------------------------------+-------+-------------+
    |client_ip    |domain                                                       |time          |target_ip      |rcode|query_type|authority_record                                                               |add_msg|dns_ip       |
    +-------------+-------------------------------------------------------------+--------------+---------------+-----+----------+-------------------------------------------------------------------------------+-------+-------------+
    |111.0.186.221|HtTPDnS.bCElIve.COM.                                         |20220802105824|110.242.68.62  |0    |1         |httpdns.lss.baidu.n.shifen.COM.                                                |null   |123.59.182.42|
    |111.0.223.8  |nEws.YOuTh.Cn.                                               |20220802105824|103.254.188.166|0    |1         |news.youth.cn.wswebcdn.com.                                                    |null   |123.59.182.42|
    |211.0.201.117|pull-FlV-l1-SouRcE.DOuYiNCDn.cOm.WSDVS.cOm.cHnc.cLOuDcsP.Com.|20220802105824|null           |2    |1         |null                                                                           |null   |123.59.182.42|
    |111.0.234.144|crYstaL-CONFiG.miGUviDEO.COM.                                |20220802105824|169.254.169.254|0    |1         |null                                                                           |null   |123.59.182.42|
    |111.0.179.96 |TXDWK.A.YXImGS.CoM.                                          |20220802105824|218.29.204.70  |0    |1         |txdwk.a.yximgs.com.cdn.dnsv1.CoM.;1036149.sched.kslego-dk.tdnsstic1.cn.        |null   |123.59.182.42|
    |211.0.237.245|Pull-flV-f1-PrOxY.DoUyINcDN.CoM.                             |20220802105824|106.120.178.68 |0    |1         |pull-flv-f1-proxy.douyincdn.com.wsdvs.CoM.                                     |null   |123.59.182.42|
    |211.0.214.219|txCMvOd.A.EtOoTe.Com.                                        |20220802105824|106.120.158.248|0    |1         |txcmvod.a.etoote.com.cdn.dnsv1.Com.;txcmvod.sched.vip-dk.tdnsvod1.cn.          |null   |123.59.182.42|
    |211.0.131.56 |PUlL-rtmP-l6-sOurCe.DouyIncdn.CoM.                           |20220802105824|42.81.245.65   |0    |1         |pull-rtmp-l6-source.douyincdn.com.rtmpvcloud.ks-cdn.CoM.;s03.gslb.ksyuncdn.CoM.|null   |123.59.182.42|
    |211.0.205.84 |aPI.AD.XIAOMI.COm.                                           |20220802105824|111.206.101.180|0    |1         |null                                                                           |null   |123.59.182.42|
    |111.0.151.196|aeWEBProxy.wEiXin.qQ.COM.                                    |20220802105824|58.246.163.67  |0    |1         |null                                                                           |null   |123.59.182.42|
    +-------------+-------------------------------------------------------------+--------------+---------------+-----+----------+-------------------------------------------------------------------------------+-------+-------------+
    only showing top 10 rows

    8933307
      root
           |-- client_ip: string (nullable = true)
           |-- domain: string (nullable = true)
           |-- time: string (nullable = true)
           |-- target_ip: string (nullable = true)
           |-- rcode: string (nullable = true)
           |-- query_type: string (nullable = true)
           |-- authority_record: string (nullable = true)
           |-- add_msg: string (nullable = true)
           |-- dns_ip: string (nullable = true)
 */
    spark.stop()
  }
}
