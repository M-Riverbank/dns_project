package dsy.read

import dsy.utils.SparkUtils
import org.apache.spark.sql.SparkSession

object read_hive {
  def main(args: Array[String]): Unit = {
    // 1.构建SparkSession实例对象
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass,is_Hive = true)

    spark.sql("select * from test.test limit 10").show()

    //程序结束，关闭资源
    spark.stop()
  }
}
