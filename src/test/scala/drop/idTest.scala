package drop

import dsy.config.configs
import dsy.utils.SparkUtils
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object idTest {
  def main(args: Array[String]): Unit = {
    // Spark应用程序与hadoop运行的用户,默认为当前系统用户
    System.setProperty("user.name", configs.HADOOP_USER_NAME)
    System.setProperty("HADOOP_USER_NAME", configs.HADOOP_USER_NAME)

    // 1.构建SparkSession实例对象
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass, is_Hive = true)
    import org.apache.spark.sql.functions._

    // 2.读取数据
    val data: DataFrame = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("multiLine", "true")
      .option("encoding", "utf-8") //utf-8
      .load(configs.LOAD_FILE)

    data
      .withColumn("id", monotonically_increasing_id.cast(StringType))
      .drop("time")
      //      .limit(10)
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("test.test")


    spark.stop
  }
}
