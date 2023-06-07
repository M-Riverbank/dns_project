package dsy.model.spark.sql

import dsy.config.configs
import dsy.utils.SparkUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 测试自定义外部数据源读取数据接口
 */
object HbaseRelationTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)

    // 读取数据
    val usersDF: DataFrame = spark.read
      .format("hbase")
      .option("zkHosts", "hadoop01")
      .option("zkPort", "2181")
      .option("hbaseTable", "tbl_tag_users")
      .option("family", "detail")
      .option("selectFields", "id,gender")
      .load()

    usersDF.printSchema()
    usersDF.show(100, truncate = false)
    //     保存数据
//    usersDF.write
//      .mode(SaveMode.Overwrite)
//      .format("hbase")
//      .option("zkHosts", "bigdata-cdh01.itcast.cn")
//      .option("zkPort", "2181")
//      .option("hbaseTable", "tbl_users")
//      .option("family", "info")
//      .option("rowKeyColumn", "id")
//      .save()
    spark.stop()
  }
}
