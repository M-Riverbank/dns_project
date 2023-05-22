package dsy.model

import dsy.config.configs
import dsy.tools.{readDataTools, ruleMapTools, writeDataTools}
import dsy.utils.SparkUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

abstract class AbstractModel(message: String) extends Logging with Serializable  {


  // Spark应用程序与hadoop运行的用户,默认为当前系统用户
  System.setProperty("user.name", configs.HADOOP_USER_NAME)
  System.setProperty("HADOOP_USER_NAME", configs.HADOOP_USER_NAME)
  logWarning(s"==========user.name与HADOOP_USER_NAME的用户为${configs.HADOOP_USER_NAME}==========")


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
    //a.获取规则
    val RuleMap: Map[String, String] =
      ruleMapTools.GetRulesMap(mysqlDF, configs.INPUT_SOURCE_FILE_NAME)
    var SourceDF: DataFrame = null

    //b.获取读取执行对象
    val readDataTools: readDataTools = new readDataTools(RuleMap, spark)

    //b.匹配读取源
    RuleMap("inType").toLowerCase
    match {
      case "hdfs" => SourceDF = readDataTools.readHdfs
      case "hive" => SourceDF = readDataTools.readHive
      case "hbase" => SourceDF = readDataTools.readHbase
      case _ => new RuntimeException(s"未实现的数据源 ${RuleMap("inType")}")
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
      //a.获取规则
      val RuleMap: Map[String, String] =
        ruleMapTools.GetRulesMap(mysqlDF, configs.OUTPUT_SOURCE_FILE_NAME)

      //b.获取保存执行对象
      val writeDataTools: writeDataTools = new writeDataTools(resultDF, RuleMap)

      //c.匹配输出
      RuleMap("outType") match {
        case "hive" => writeDataTools.writeHive()
        case "hbase" => writeDataTools.writeHbase()
        case "mysql" => writeDataTools.writeMysql()
        case _ => new RuntimeException(s"未实现的输出方式 ${RuleMap("outType")}")
      }
    }
  }


  /**
   * 关闭资源:应用结束,关闭会话实例对象
   */
  private def close(): Unit = {
    if (spark != null) spark.stop
  }


  /**
   * 模型执行流程
   *
   * @param id     唯一主键
   * @param isHive 是否集成 hive 默认不集成
   */
  def execute(id: Long, isHive: Boolean = false): Unit = {
    logWarning(s"message--------------->$message")
    // a. 初始化
    init(isHive)
    try {
      // b. 获取mysql数据并缓存
      mysqlDF = get_RuleData(id)
      mysqlDF.persist(StorageLevel.MEMORY_AND_DISK) // 调优:多次使用,缓存处理
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
