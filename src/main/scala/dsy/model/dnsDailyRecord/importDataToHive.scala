package dsy.model.dnsDailyRecord

import dsy.model.AbstractModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.types.StringType


class importDataToHive extends AbstractModel("将hdfs文件导入存储到hive") {
  override def handle(businessDF: DataFrame, mysqlDF: DataFrame): DataFrame = {
    //数据预处理，赋予id字段，从1开始单调递增，部分数据无参考意义删除time字段，
    val resultDF: DataFrame = businessDF
      .withColumn("id", monotonically_increasing_id.cast(StringType))
      .drop("time")
    //    resultDF.show(10, truncate = false)
    //    resultDF.printSchema()
    resultDF
    //    null
  }
}

object importDataToHive {
  def main(args: Array[String]): Unit = {
    new importDataToHive().execute(1, isHive = true)
  }
}

