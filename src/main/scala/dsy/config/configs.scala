package dsy.config

import com.typesafe.config.{Config, ConfigFactory}

/**
 * 读取配置文件信息config.properties，获取属性值
 */
object configs {
  // 构建Config对象，读取配置文件
  private val config: Config = ConfigFactory.load("config.properties")

  // spark
  lazy val SPARK_CONF_FILE: String = config.getString("spark.conf.file")

  // spark是否为 local 模式 ---------- 测试环境为true,生产环境为false
  lazy val SPARK_IS_LOCAL: Boolean = config.getBoolean("spark.is.local")
  lazy val SPARK_MASTER: String = config.getString("spark.master")

  // 读取数据的位置 ---------- 测试环境为本地路径,生产环境为hdfs路径
  lazy val LOAD_FILE: String = config.getString("spark.load.file")

  // spark是否集成 Hbase 与 Hbase 配置
  lazy val SPARK_ADD_HBASE: Boolean = config.getBoolean("spark.add.hbase")
  lazy val SPARK_HADOOP_VALIDATEOUTPUTSPECS: String = config.getString("spark.hadoop.validateOutputSpecs") //验证输出参数为否
  lazy val SPARK_SERIALIZER: String = config.getString("spark.serializer") //序列化

}
