package read

import org.apache.spark.sql.{DataFrame, SparkSession}

object readCsv {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local")
      .getOrCreate()

    val df: DataFrame = spark.read
      .option("header","true")
      .option("encoding","GBK")
      .csv("datas/tbl_goods.csv")

    df.show
    df.printSchema

    spark.stop
  }
}
