package dsy.read_hdfs

import dsy.config.configs
import dsy.utils.SparkUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

object read_hdfs_write_to_hbase {
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

    //写入Hbase
    //a.Hbase的连接信息
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "dsy") //zookeeper地址
    conf.set("hbase.zookeeper.property.clientPort", "2181") //zookeeper端口号
    conf.set(TableOutputFormat.OUTPUT_TABLE, "test") //写入的表名称
    //b.将DataFrame类型转换为RDD[(ImmutableBytesWritable, Put)]
    val columns: Array[String] = new_data.columns
    val familyBytes: Array[Byte] = Bytes.toBytes("info")

    val datasRDD: RDD[(ImmutableBytesWritable, Put)] = new_data.rdd
      .map {
        row => {
          //获取rowKey的值
          val rowKey: Array[Byte] = Bytes.toBytes(row.getAs[String]("id"))
          //构建Put对象
          val put: Put = new Put(rowKey)
          //获取key与value的字节数组值,批量写入put对象
          columns
            .foreach(file => {
              //将字段名与字段值转换为字节数组
              val key: Array[Byte] = Bytes.toBytes(file)
              var value: Array[Byte] = null
              val value_String = row.getAs[String](file)
              if (value_String != null)
                value = Bytes.toBytes(value_String)
              //写入put对象
              put.addColumn(familyBytes, key, value)
            })
          (new ImmutableBytesWritable(rowKey), put)
        }
      }

    //c.开始写入
    datasRDD.saveAsNewAPIHadoopFile(
      s"hbase/tmp/output-${System.nanoTime()}",
      classOf[ImmutableBytesWritable],
      classOf[Put],
      classOf[TableOutputFormat[ImmutableBytesWritable]],
      conf
    )

    spark.stop()
  }
}
