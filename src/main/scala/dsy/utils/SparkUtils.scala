package dsy.utils

import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import dsy.config.configs
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util

/**
 * 创建 sparkSession 的工具类
 */
object SparkUtils {
  /**
   * 加载Spark Application默认配置文件，设置到SparkConf中
   *
   * @param resource 资源配置文件名称
   * @return SparkConf对象
   */
  private def loadConf(resource: String): SparkConf = {
    // 1. 创建SparkConf 对象
    val sparkConf = new SparkConf()
    // 2. 使用ConfigFactory加载配置文件(spark.properties)
    val config: Config = ConfigFactory.load(resource)
    // 3. 获取加载配置信息,以键值对方式存储
    val entrySet: util.Set[util.Map.Entry[String, ConfigValue]] = config.entrySet()
    // 4. 循环遍历设置属性值到SparkConf中
    import scala.collection.JavaConverters._
    entrySet.asScala.foreach { entry =>
      sparkConf.set(entry.getKey, entry.getValue.unwrapped().toString)
    }
    // 5. 返回SparkConf对象
    sparkConf
  }

  /**
   * 构建SparkSession实例对象，如果是本地模式，设置master
   *
   * @param clazz 传入类对象，应用名为类名称
   * @return sparkSession对象
   */
  def createSparkSession(clazz: Class[_]): SparkSession = {
    // 1. 构建SparkConf对象
    val sparkConf: SparkConf = loadConf(resource = configs.SPARK_CONF_FILE)
    // 2. 判断应用是否是本地模式运行，如果是设置
    if (configs.SPARK_IS_LOCAL) {
      sparkConf.setMaster(configs.SPARK_MASTER)
    }
    sparkConf
      //不验证输出参数
      .set("spark.hadoop.validateOutputSpecs", "False")
      // 设置使用Kryo序列
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 注册哪些类型使用Kryo序列化, 最好注册RDD中类型
      .registerKryoClasses(
        Array(classOf[ImmutableBytesWritable], classOf[Put])
      )
    // 3. 创建SparkSession.Builder对象
    val builder: SparkSession.Builder = SparkSession
      .builder()
      .appName(clazz.getSimpleName.stripSuffix("$"))
      .config(sparkConf)

    // 4. 获取SparkSession对象
    val session = builder.getOrCreate()
    // 5. 返回
    session
  }
}
