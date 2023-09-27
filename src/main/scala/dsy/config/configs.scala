package dsy.config

import com.typesafe.config.{Config, ConfigFactory}

/**
 * 读取配置文件信息config.properties，获取属性值
 */
object configs {
  // 构建Config对象，读取配置文件
  private val config: Config = ConfigFactory.load("config.properties")
  // spark 读取文件位置,此时与生产不一样,local为测试
  lazy val SPARK_CONF_FILE: String = config.getString("spark.conf.file")
  // spark是否为 local 模式 ---------- 测试环境为true,生产环境为false
  lazy val SPARK_IS_LOCAL: Boolean = config.getBoolean("spark.is.local")
  lazy val SPARK_MASTER: String = config.getString("spark.master")
  // 读取数据的位置 ---------- 测试环境为本地路径,生产环境为hdfs路径
  lazy val LOAD_FILE: String = config.getString("spark.load.file")
  // spark 连接 hive metastore
  lazy val SPARK_HIVE_METASTORE_URIS: String = config.getString("spark.hive.metastore.uris") //元数据服务连接地址
  //Spark应用程序与hadoop运行的用户,默认为本地用户
  lazy val HADOOP_USER_NAME: String = config.getString("hadoop.user.name")
  // MySQL Config
  lazy val MYSQL_JDBC_DRIVER: String = config.getString("mysql.jdbc.driver")
  lazy val MYSQL_JDBC_URL: String = config.getString("mysql.jdbc.url")
  lazy val MYSQL_JDBC_USERNAME: String = config.getString("mysql.jdbc.username")
  lazy val MYSQL_JDBC_PASSWORD: String = config.getString("mysql.jdbc.password")
  //后端表输入输出源字段名
  lazy val MYSQL_TABLE:String = config.getString("mysql_table")
  lazy val HANDLE_RULE_FILENAME:String = config.getString("handle_rule_filename")
  lazy val INPUT_SOURCE_FILE_NAME: String = config.getString("inputSource_field_name")
  lazy val OUTPUT_SOURCE_FILE_NAME: String = config.getString("outputSource_field_name")

  //读取数据参数默认值
  lazy val MYSQL_DEFAULT_DRIVER: String = config.getString("read.impl.mysql.default.driver")
  lazy val MYSQL_DEFAULT_USER: String = config.getString("read.impl.mysql.default.user")
  lazy val MYSQL_DEFAULT_PASSWORD: String = config.getString("read.impl.mysql.default.password")

  /**
   * 查询id匹配的数据(因执行时spark会在语句后添加where 1=0导致语法错误暂时弃用)
   * @param id 唯一标识
   * @return 与id匹配的数据
   */
  def sql(id: Long): String = {
    s"""
       |SELECT
       |  *
       |FROM
       |  db01.tb_model
       |WHERE
       |  id = $id
       |""".stripMargin
  }
}
