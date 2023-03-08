package rename_coluom

import dsy.config.configs
import dsy.utils.SparkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._

object renamecoloum {
  def main(args: Array[String]): Unit = {
    // 1.构建SparkSession实例对象
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
    //导入隐式转换
    import spark.implicits._

    // 2.读取数据
    val data: DataFrame = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("multiLine", "true")
      .option("encoding", "utf-8") //utf-8
      .load(configs.LOAD_FILE)

    //添加序号列
    val new_data: DataFrame = data
      .withColumn("id", monotonically_increasing_id.cast(StringType))
      //重命名列名
      .select(
        $"id",
        $"client_ip".alias("clientIP"),
        $"domain",
        $"time",
        $"target_ip".alias("targetIP"),
        $"rcode",
        $"query_type".alias("queryType"),
        $"authority_record".alias("authorityRecord"),
        $"add_msg".alias("addMsg"),
        $"dns_ip".alias("dnsIp"))

    new_data.show
    new_data.printSchema

    spark.stop
  }
}
