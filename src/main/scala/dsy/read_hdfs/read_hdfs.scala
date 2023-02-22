package dsy.read_hdfs

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object read_hdfs {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root");
    System.setProperty("user.name", "root");
    val value = this.getClass.getClassLoader.loadClass("org.apache.spark.scheduler.cluster.YarnClusterManager")

    val spark: SparkSession = {
      val conf: SparkConf = new SparkConf()
        // 设置yarn-client模式提交
        .setMaster("yarn")
        //App名字
        .set("spark.app.name", this.getClass.getSimpleName.stripSuffix("$"))
        // 设置resourcemanager的ip
        .set("yarn.resourcemanager.hostname", "dsy")
        // 设置executor的个数
        .set("spark.executor.instance", "2")
        // 设置executor的内存大小
        .set("spark.executor.memory", "1024M")
        // 设置提交任务的yarn队列
        .set("spark.yarn.queue", "default")
        // 设置driver的ip地址
        .set("spark.driver.host", "localhost")
        // 设置jar包的路径,如果有其他的依赖包,可以在这里添加,逗号隔开
        .set("spark.yarn.jars", "C:\\Users\\han\\Desktop\\test\\dns_project\\target\\dns_project.jar,hdfs://dsy:9000/spark-yarn/jars/*.jar")
        // 序列化
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      SparkSession
        .builder()
        .config(conf)
        .getOrCreate()
    }

    val data: DataFrame = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("multiLine", "true")
      .option("encoding", "utf-8") //utf-8
      //"D:\\data\\dns_data_test.csv"
      //"/soft/data/DNS_DATA/dns_data_test.csv"
      //"hdfs://dsy:9000/dns_data/dns_data_test.csv"
      .load("hdfs://dsy:9000/dns_data/dns_data_test.csv")

    data.show(1000, truncate = false)
    println(data.count())
    data.printSchema()

    spark.stop()
  }
}
