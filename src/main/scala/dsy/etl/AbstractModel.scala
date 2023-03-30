package dsy.etl

import dsy.config.configs
import dsy.meta.HBaseMeta
import dsy.tools.{HbaseTools, metaUtils}
import dsy.utils.SparkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

abstract class AbstractModel() {
  // 设置Spark应用程序运行的用户：root, 默认情况下为当前系统用户
  // Spark应用程序与hadoop运行的用户,默认为本地用户
  System.setProperty("user.name", configs.HADOOP_USER_NAME)
  System.setProperty("HADOOP_USER_NAME", configs.HADOOP_USER_NAME)

  // 变量声明
  var spark: SparkSession = _

  /**
   *  1. 初始化：构建SparkSession实例对象
   */
  private def init(isHive: Boolean): Unit = {
    spark = SparkUtils.createSparkSession(this.getClass, isHive)
  }

  /**
   * 2. 读取 mysql 规则数据返回
   *
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

  private def getSourceData(mysqlDF: DataFrame): DataFrame = {
    //a.获取规则，解析封装
    val RuleMap: Map[String, String] =
      metaUtils.GetRulesMap(mysqlDF, configs.INPUT_SOURCE_FILE_NAME)

    //b.读取数据源
    var SourceDF: DataFrame = null
    RuleMap("inType").toLowerCase match {
      case "hfds" => null
      case "hive" => null
      case "hbase" =>
        //封装标签规则中数据源的信息至HBaseMeta对象中
        val hbaseMeta: HBaseMeta = HBaseMeta.getHBaseMeta(RuleMap)
        SourceDF = HbaseTools
          .read(
            spark,
            zkHosts = hbaseMeta.zkHosts,
            zkPort = hbaseMeta.zkPort,
            table = hbaseMeta.hbaseTable,
            family = hbaseMeta.family,
            fields = hbaseMeta.selectFieldNames
          )

      case _ => new RuntimeException("未提供数据源信息，获取不到原始数据，无法计算")
    }
    //c.返回业务数据
    SourceDF
  }

  def handle(businessDF: DataFrame, tagDF: DataFrame): DataFrame


  private def saveDF(resultDF: DataFrame, mysqlDF: DataFrame): Unit = {
    if (resultDF != null) {
      //a.获取规则，解析封装
      val RuleMap: Map[String, String] =
        metaUtils.GetRulesMap(mysqlDF, configs.OUTPUT_SOURCE_FILE_NAME)

      RuleMap("outType") match {
        case "hfds" => null
        case "hive" => null
        case "hbase" => null
        case "mysql" => null
        case _ => new RuntimeException("未知保存格式")
      }
    }
  }

  /**
   * 关闭资源:应用结束,关闭会话实例对象
   */
  private def close(): Unit = {
    if (spark != null) spark.stop()
  }

  def execute(id: Long, isHive: Boolean = false): Unit = {
    // a. 初始化
    init(isHive)
    try {
      // b. 获取mysql数据并缓存
      val mysqlDF: DataFrame = get_RuleData(id)
      mysqlDF.persist(StorageLevel.MEMORY_AND_DISK)
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
