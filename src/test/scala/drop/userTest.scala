package drop

import dsy.config.configs
import dsy.meta.read.impl.HDFSReadMeta
import dsy.tools.readDataTools
import dsy.utils.SparkUtils
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset, Row}

object userTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.createSparkSession(this.getClass, is_Hive = true)
    import spark.implicits._

    //读取规则数据返回
    val sqlDF: DataFrame = spark.read
      .format("jdbc")
      .option("driver", configs.MYSQL_JDBC_DRIVER)
      .option("url", configs.MYSQL_JDBC_URL)
      .option("dbtable", configs.MYSQL_TABLE) //configs.sql(id)
      .option("user", configs.MYSQL_JDBC_USERNAME)
      .option("password", configs.MYSQL_JDBC_PASSWORD)
      .load()
    val mysqlDF: Dataset[Row] = sqlDF
      .where($"id" === 100)

    //a.获取规则
    val RuleMap: Map[String, String] = mysqlDF
      .head //返回Row对象
      .getAs[String](configs.INPUT_SOURCE_FILE_NAME)
      .split("\\n")
      .map { line =>
        val Array(key, value) = line.trim.split("=")
        (key, value)
      }.toMap
    var SourceDF: DataFrame = null

    //b.获取读取执行对象
    val readDataTools: readDataTools = new readDataTools(RuleMap, spark)
    //封装标签规则中数据源的信息至 HDFSMeta 对象中
    SourceDF = {
      val hdfsReadMeta: HDFSReadMeta = HDFSReadMeta.getObject(RuleMap)
      //读取数据
      val reader: DataFrameReader = spark
        .read
        .format(hdfsReadMeta.format)
      if (hdfsReadMeta.optionsMap.nonEmpty) {
        //option 写入
        hdfsReadMeta.optionsMap
          .foreach {
            keyValue =>
              reader.option(keyValue._1, keyValue._2)
          }
      }
      reader.load(hdfsReadMeta.hdfsAddress) //加载数据
    }

    val resultDF: DataFrame = SourceDF
      .withColumn("id", monotonically_increasing_id.cast(StringType))
      .drop("time")


    if (spark != null) spark.stop
  }
}
