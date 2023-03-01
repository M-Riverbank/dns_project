package dsy.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat

object HbaseTools {
  /**
   * 将数据保存到HBase表中
   *
   * @param dataFrame    保存的数据
   * @param zkHosts      zookeeper地址
   * @param zkPort       zookeeper端口号
   * @param table        Hbase表名称
   * @param family       列簇名
   * @param rowKeyColumn RowKey字段名称
   */
  def write(
             dataFrame: DataFrame,
             zkHosts: String,
             zkPort: String,
             table: String,
             family: String,
             rowKeyColumn: String
           ): Unit = {
    //1.获取写入的字段列表与列簇
    val fields: Array[String] = dataFrame.columns
    val familyBytes: Array[Byte] = Bytes.toBytes(family)

    //2.写入HBase配置项
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zkHosts) //zookeeper地址
    conf.set("hbase.zookeeper.property.clientPort", zkPort) //zookeeper端口号
    conf.set(TableOutputFormat.OUTPUT_TABLE, table) //写入的表名称

    //3.将DataFrame类型转换为RDD[(ImmutableBytesWritable, Put)]
    val datasRDD: RDD[(ImmutableBytesWritable, Put)] = dataFrame.rdd
      .map { row =>
        //获取rowKey的值
        val rowKey: Array[Byte] = Bytes.toBytes(row.getAs[String](rowKeyColumn))
        //构建Put对象
        val put: Put = new Put(rowKey)
        //设置列值
        fields.foreach { field =>
          //获取key与value的值并转换为字节数组
          val key: Array[Byte] = Bytes.toBytes(field)
          val value: Array[Byte] = Bytes.toBytes(row.getAs[String](field))
          //写入put对象
          put.addColumn(familyBytes, key, value)
        }
        (new ImmutableBytesWritable(rowKey), put)
      }

    //4.写入HBase表
    datasRDD.saveAsNewAPIHadoopFile(
      s"hbase/tmp/output-${System.nanoTime()}",
      classOf[ImmutableBytesWritable],
      classOf[Put],
      classOf[TableOutputFormat[ImmutableBytesWritable]],
      conf
    )
  }
}
