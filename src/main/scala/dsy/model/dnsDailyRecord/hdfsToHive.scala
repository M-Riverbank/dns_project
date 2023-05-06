package dsy.model.dnsDailyRecord

import dsy.config.configs
import dsy.model.AbstractModel
import dsy.tools.ruleMapTools
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.types.StringType


class hdfsToHive extends AbstractModel("读取hdfs文件存储hive数据库") {
  override def handle(businessDF: DataFrame, mysqlDF: DataFrame): DataFrame = {
    val resultDF: DataFrame = businessDF
      .withColumn("id", monotonically_increasing_id.cast(StringType))
      .drop("time")
    //    resultDF.show(10, truncate = false)
    //    resultDF.printSchema()
    resultDF
//    null
  }
}

object hdfsToHive {
  def main(args: Array[String]): Unit = {
    new hdfsToHive().execute(100, isHive = true)
  }
}

