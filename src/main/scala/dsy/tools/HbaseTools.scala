package dsy.tools

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object HbaseTools {


  /**
   * 读取HBase表数据,包装为DataFrame返回
   *
   * @param spark   SparkSession对象
   * @param zkHosts zookeeper地址
   * @param zkPort  zookeeper端口号
   * @param table   Hbase表名称
   * @param family  列簇名
   * @param fields  读取的字段
   * @return DataFrame类型返回
   */
  def read(
            spark: SparkSession,
            zkHosts: String,
            zkPort: String,
            table: String,
            family: String,
            fields: String
          ): DataFrame = {
    //1.获取sparkContext执行对象,转换列簇为字节数组
    val sc: SparkContext = spark.sparkContext
    val familyBytes: Array[Byte] = Bytes.toBytes(family)

    //2.配置连接HBase配置项，如端口号、zookeeper地址与列簇字段等
    val conf: Configuration = HBaseConfiguration.create()
    //设置scan过滤,只查询对应列簇需要的字段
    val scan: Scan = new Scan()
    scan.addFamily(familyBytes)

    fields
      .split(",")
      .foreach(field =>
        //循环字段列表添加查询的字段
        scan.addColumn(familyBytes, Bytes.toBytes(field))
      )
    conf.set("hbase.zookeeper.quorum", zkHosts) //zookeeper地址
    conf.set("hbase.zookeeper.property.clientPort", zkPort) //zookeeper端口号
    conf.set("zookeeper.znode.parent", "/hbase") //Hbase节点位置
    conf.set(TableInputFormat.INPUT_TABLE, table) //读取的表
    conf.set(
      TableInputFormat.SCAN,
      //Scan过滤对象
      Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray)
    )

    //3.使用sparkContext对象读取HBase表
    val datasRDD: RDD[(ImmutableBytesWritable, Result)] = sc
      .newAPIHadoopRDD(
        conf, //conf配置
        classOf[TableInputFormat], //读取数据的存储格式
        classOf[ImmutableBytesWritable], //key类型
        classOf[Result] //value类型
      )

    //=======================================
    //    DataFrame = RDD[Row] + Schema
    //=======================================
    //4.将读取到的数据转换为DataFrame返回
    //4.1.将结果转换为RowRDD
    val RowRDD: RDD[Row] = datasRDD
      //row 每行数据 转换为 二元组(RowKey,Put)
      .map { case (_, result) => //每一条数据对应一个主键查询到的字段值
        val Seq: Seq[String] = fields
          //根据字段数组从 Result 中提取出对应字段数据,
          .map { field =>
            //根据列簇与字段名提取
            val valueBytes: Array[Byte] = result.getValue(familyBytes, Bytes.toBytes(field))
            //转换字符串输出
            Bytes.toString(valueBytes)
          }
        Row.fromSeq(Seq)
      }
    //4.2.根据字段数组设置Schema信息
    val schema: StructType = StructType(
      fields
        .split(",")
        .map(field =>
          StructField(field, StringType, nullable = true)
        )
    )

    //返回DataFrame
    spark.createDataFrame(RowRDD, schema)
  }

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
          var value: Array[Byte] = null
          val key_String: String = row.getAs[String](field)
          //如果字段值不为空则写入，否则写入 null
          if (key_String != null) value = Bytes.toBytes(key_String)
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
