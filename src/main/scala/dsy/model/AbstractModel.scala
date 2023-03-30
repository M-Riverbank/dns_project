package dsy.model

import dsy.config.configs
import dsy.meta.read.{HBaseReadMeta, HDFSReadMeta}
import dsy.meta.save.HiveWriteMeta
import dsy.tools.{HbaseTools, ruleMapUtils}
import dsy.utils.SparkUtils
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.apache.spark.storage.StorageLevel

abstract class AbstractModel {
  // Spark应用程序与hadoop运行的用户,默认为当前系统用户
  System.setProperty("user.name", configs.HADOOP_USER_NAME)
  System.setProperty("HADOOP_USER_NAME", configs.HADOOP_USER_NAME)

  // 变量声明
  var spark: SparkSession = _
  var mysqlDF: DataFrame = _

  /**
   * 初始化：构建SparkSession实例对象
   *
   * @param isHive 集成hive
   */
  private def init(isHive: Boolean): Unit = {
    spark = SparkUtils.createSparkSession(this.getClass, isHive)
  }

  /**
   * 2. 读取 mysql 规则数据返回
   *
   * @param id 查询后端数据的条件(主键)
   * @return 规则数据
   */
  private def get_RuleData(id: Long): DataFrame = {

    //读取规则数据返回
    val sqlDF: DataFrame = spark.read
      .format("jdbc")
      .option("driver", configs.MYSQL_JDBC_DRIVER)
      .option("url", configs.MYSQL_JDBC_URL)
      .option("dbtable", configs.MYSQL_TABLE) //configs.sql(id)
      .option("user", configs.MYSQL_JDBC_USERNAME)
      .option("password", configs.MYSQL_JDBC_PASSWORD)
      .load()
    import sqlDF.sparkSession.implicits._
    sqlDF
      .where($"id" === id)
  }

  /**
   * 获取业务数据
   *
   * @param mysqlDF 后端数据(获取数据源信息)
   * @return 业务数据
   */
  private def getSourceData(mysqlDF: DataFrame): DataFrame = {
    //a.获取规则，解析封装
    val RuleMap: Map[String, String] =
      ruleMapUtils.GetRulesMap(mysqlDF, configs.INPUT_SOURCE_FILE_NAME)

    //b.读取数据源
    var SourceDF: DataFrame = null
    RuleMap("inType").toLowerCase match {
      case "hdfs" =>
        //封装标签规则中数据源的信息至 HDFSMeta 对象中
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
        SourceDF = reader.load(hdfsReadMeta.hdfsAddress) //加载数据

      case "hive" => return null
      case "hbase" =>
        //封装标签规则中数据源的信息至 HBaseMeta 对象中
        val hbaseReadMeta: HBaseReadMeta = HBaseReadMeta.getObject(RuleMap)
        //读取数据
        SourceDF = HbaseTools
          .read(
            spark,
            zkHosts = hbaseReadMeta.zkHosts,
            zkPort = hbaseReadMeta.zkPort,
            table = hbaseReadMeta.hbaseTable,
            family = hbaseReadMeta.family,
            fields = hbaseReadMeta.selectFieldNames
          )

      case _ => new RuntimeException("未提供数据源信息，获取不到原始数据，无法计算")
    }
    //c.返回业务数据
    SourceDF
  }

  /**
   * 抽象方法，对数据的具体处理,由实现类完善
   *
   * @param businessDF 业务数据
   * @param mysqlDF    后端数据
   * @return 处理后的数据
   */
  def handle(businessDF: DataFrame, mysqlDF: DataFrame): DataFrame


  /**
   * 保存处理后的数据
   *
   * @param resultDF 要保存的DF
   * @param mysqlDF  后端数据(包含输出源信息)
   */
  private def saveDF(resultDF: DataFrame, mysqlDF: DataFrame): Unit = {
    if (resultDF != null) {
      //a.获取规则，解析封装
      val RuleMap: Map[String, String] =
        ruleMapUtils.GetRulesMap(mysqlDF, configs.OUTPUT_SOURCE_FILE_NAME)

      RuleMap("outType") match {
        case "hive" =>
          val hdfsWriteMeta: HiveWriteMeta = HiveWriteMeta.getObject(RuleMap)
          resultDF
            .write
            .mode(hdfsWriteMeta.saveMode)
            .save(hdfsWriteMeta.tableName)

        case "hbase" => null
        case "mysql" => null
        case _ => new RuntimeException(s"未支持的保存格式 ${RuleMap("outType")}")
      }
    }
  }

  /**
   * 关闭资源:应用结束,关闭会话实例对象
   */
  private def close(): Unit = {
    if (spark != null) spark.stop()
  }

  /**
   * 模型执行流程
   *
   * @param id     唯一主键
   * @param isHive 是否集成 hive 默认不集成
   */
  def execute(id: Long, isHive: Boolean = false): Unit = {
    // a. 初始化
    init(isHive)
    try {
      // b. 获取mysql数据并缓存
      mysqlDF = get_RuleData(id)
      mysqlDF.persist(StorageLevel.MEMORY_AND_DISK) //多次使用,缓存处理
      mysqlDF.count()
      // c. 获取业务数据
      val SourceDF: DataFrame = getSourceData(mysqlDF)
      // d. 处理数据
      val resultDF: DataFrame = handle(SourceDF, mysqlDF)
      // e. 保存数据
      saveDF(resultDF, mysqlDF)
      mysqlDF.unpersist()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      //f.关闭资源
      close()
    }
  }
}
