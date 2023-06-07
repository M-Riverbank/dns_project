package dsy.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
 * spark 自定义外部数据源,实现对 hbase 的读写操作的 Relation实现
 */
class HbaseRelation(
                     context:SQLContext,
                     params:Map[String,String],
                     userSchema:StructType
                   )
  extends BaseRelation
  with TableScan
  with InsertableRelation
  with Serializable {
  // 连接HBase数据库的属性名称
  private val HBASE_ZK_QUORUM_KEY: String = "hbase.zookeeper.quorum"
  private val HBASE_ZK_QUORUM_VALUE: String = "zkHosts"
  private val HBASE_ZK_PORT_KEY: String = "hbase.zookeeper.property.clientPort"
  private val HBASE_ZK_PORT_VALUE: String = "zkPort"

  private val HBASE_TABLE: String = "hbaseTable"
  private val HBASE_TABLE_FAMILY: String = "family"
  val SPLIT: String = ","
  val HBASE_TABLE_SELECT_FIELDS: String = "selectFields"
  val HBASE_TABLE_ROWKEY_NAME: String = "rowKeyColumn"

  /**
   *  sparkSql 加载与保存数据的入口，相当于 sparkSession
   *
   * @return 执行对象
   */
  override def sqlContext: SQLContext = context

  /**
   *  sparkSql 中数据封装在 DataFrame 或者 Dataset 中的 schema 信息
   *
   * @return schema信息
   */
  override def schema: StructType = userSchema

  /**
   * 从数据源加载数据,封装至RDD中,每条数据在 Row 中,结合 schema 信息,转换为 DataFrame
   *
   * @return 获取到的数据
   */
  override def buildScan(): RDD[Row] = {
    // 1. 设置HBase配置信息
    val conf: Configuration = new Configuration()
    // a. 创建scan对象过滤读取的字段
    val scan: Scan = new Scan()
    // b. 设置读取的列簇
    val familyBytes = Bytes.toBytes(params(HBASE_TABLE_FAMILY))
    scan.addFamily(familyBytes)
    // c. 设置读取的列名称
    val fields: Array[String] = params(HBASE_TABLE_SELECT_FIELDS).split(SPLIT)
    fields.foreach { field =>
      scan.addColumn(familyBytes, Bytes.toBytes(field))
    }
    conf.set(HBASE_ZK_QUORUM_KEY, params(HBASE_ZK_QUORUM_VALUE)) //zookeeper集群地址
    conf.set(HBASE_ZK_PORT_KEY, params(HBASE_ZK_PORT_VALUE)) //zookeeper端口
    conf.set(TableInputFormat.INPUT_TABLE, params(HBASE_TABLE)) //读HBase表的名称
    conf.set(
      TableInputFormat.SCAN,
      //scan过滤
      Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray)
    )

    // 2. 调用底层API，读取HBase表的数据
    val datasRDD: RDD[(ImmutableBytesWritable, Result)] =
      sqlContext.sparkContext
        .newAPIHadoopRDD(
          conf,
          classOf[TableInputFormat],
          classOf[ImmutableBytesWritable],
          classOf[Result]
        )

    // 3. 转换为RDD[Row]
    val rowsRDD: RDD[Row] = datasRDD.map { case (_, result) =>
      // 3.1. 列的值
      val values: Seq[String] = fields.map { field =>
        Bytes.toString(
          result.getValue(familyBytes, Bytes.toBytes(field))
        )
      }
      // 3.2. 生成Row对象
      Row.fromSeq(values)
    }

    // 4. 返回
    rowsRDD
  }

  /**
   * 将 DataFrame 数据保存至数据源
   *
   * @param data 保存的数据集
   * @param overwrite 是否为覆写模式
   */
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    //1.获取写入的字段列表与列簇
    val fields: Array[String] = data.columns
    val familyBytes: Array[Byte] = Bytes.toBytes(params(HBASE_TABLE_FAMILY))

    //2.写入HBase配置项
    val conf: Configuration = HBaseConfiguration.create()
    conf.set(HBASE_ZK_QUORUM_KEY, params(HBASE_ZK_QUORUM_VALUE)) //zookeeper地址
    conf.set(HBASE_ZK_PORT_KEY, params(HBASE_ZK_PORT_VALUE)) //zookeeper端口号
    conf.set(TableOutputFormat.OUTPUT_TABLE, params(HBASE_TABLE)) //写入的表名称

    //3.将DataFrame类型转换为RDD[(ImmutableBytesWritable, Put)]
    val datasRDD: RDD[(ImmutableBytesWritable, Put)] = data.rdd
      .map { row =>
        //获取rowKey的值
        val rowKey: Array[Byte] = Bytes.toBytes(row.getAs[String](params(HBASE_TABLE_ROWKEY_NAME)))
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
      s"datas/hbase/output-${System.nanoTime()}",
      classOf[ImmutableBytesWritable],
      classOf[Put],
      classOf[TableOutputFormat[ImmutableBytesWritable]],
      conf
    )
  }
}
