package dsy.read_hdfs

import dsy.config.configs
import dsy.utils.SparkUtils
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object read_hdfs_write_to_hbase {
  // Spark应用程序与hadoop运行的用户,默认为本地用户
  System.setProperty("user.name", configs.HADOOP_USER_NAME)
  System.setProperty("HADOOP_USER_NAME", configs.HADOOP_USER_NAME)

  def main(args: Array[String]): Unit = {
    // 1.构建SparkSession实例对象
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
    //导入隐式转换
    import org.apache.spark.sql.functions._

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
      .withColumn("id", monotonically_increasing_id)
      .withColumn("id", col("id").cast(StringType))
    //    new_data.show(10,truncate = false)
    //    new_data.printSchema()
    /*
    root
           |-- client_ip: string (nullable = true)
           |-- domain: string (nullable = true)
           |-- time: string (nullable = true)
           |-- target_ip: string (nullable = true)
           |-- rcode: string (nullable = true)
           |-- query_type: string (nullable = true)
           |-- authority_record: string (nullable = true)
           |-- add_msg: string (nullable = true)
           |-- dns_ip: string (nullable = true)
           |-- id: string (nullable = false)
        new_data.select(
          $"id",
          $"client_ip",
          $"domain",
          $"time",
          $"target_ip",
          $"rcode",
          $"query_type",
          $"authority_record",
          $"add_msg",
          $"dns_ip"
        ).show(10,truncate = false)
     */
    /**
     * 将数据保存到HBase表中
     *
     * dataFrame    保存的数据
     * zkHosts      zookeeper地址
     * zkPort       zookeeper端口号
     * table        Hbase表名称
     * family       列簇名
     * rowKeyColumn RowKey字段名称
     */
    //    HbaseTools.write(
    //      new_data,
    //      "dsy",
    //      "2181",
    //      "test",
    //      "info",
    //      "id"
    //    )
    new_data
      .write
      //写入模式
      .mode(SaveMode.Overwrite)
      //保存的表
      .saveAsTable("test.test")
    spark.stop()
  }
}