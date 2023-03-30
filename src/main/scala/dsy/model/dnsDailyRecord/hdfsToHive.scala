package dsy.model.dnsDailyRecord

import dsy.model.AbstractModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.types.StringType


class hdfsToHive extends AbstractModel {
  override def handle(businessDF: DataFrame, mysqlDF: DataFrame): DataFrame = {
    val sourceDF: DataFrame = businessDF
      .withColumn("id", monotonically_increasing_id.cast(StringType))
    sourceDF.show()
    sourceDF.printSchema()
    null
  }
}

object hdfsToHive {
  def main(args: Array[String]): Unit = {
    new hdfsToHive().execute(100, isHive = true)
  }
}

