package dsy.model.dnsDailyRecord

import dsy.model.AbstractModel
import org.apache.spark.sql.DataFrame

class client_ip_count extends AbstractModel("每个client_ip上网次数统计") {
  override def handle(businessDF: DataFrame, mysqlDF: DataFrame): DataFrame = {
    //以 client_ip 分组并统计每个ip的上网次数
    val countDF = businessDF
      .groupBy("client_ip")
      .count()

    //对 count 进行降序排序，取前十条
    val resultDF = countDF
      .orderBy(countDF("count").desc)
      .limit(10)

    resultDF.show
    resultDF.printSchema
    /*
           +-------------+-----+
           |    client_ip|count|
           +-------------+-----+
           |111.0.161.163|  804|
           | 211.0.138.43|  796|
           |  85.0.191.97|  791|
           |211.0.181.142|  790|
           | 85.0.244.172|  790|
           | 85.0.231.178|  787|
           |211.0.214.223|  786|
           | 211.0.188.46|  784|
           | 211.0.201.62|  784|
           | 211.0.237.43|  783|
           +-------------+-----+

           root
              |-- client_ip: string (nullable = true)
              |-- count: long (nullable = false)
      */
    null
    //    resultDF
  }
}

object client_ip_count {
  def main(args: Array[String]): Unit = {
    new client_ip_count().execute(2, isHive = true)
  }
}
