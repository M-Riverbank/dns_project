import org.apache.spark.sql.{DataFrame, SparkSession}

object hdfs_test {
  def main(args: Array[String]): Unit = {
    // 1.构建SparkSession实例对象
    val spark: SparkSession = SparkSession
      .builder() // 使用建造者模式构建对象
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("yarn")
      //设置shuffle分区数，默认200
      .config("spark.sql.shuffle.partitions", "4")
      //TODO: 继承Hive,配置metastore地址信息
      .config("hive.metastore.uris", "thrift://dsy:9083")
      .enableHiveSupport() //表示集成Hive,显示指定集成
      .getOrCreate()


    val data: DataFrame = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("multiLine", "true")
      .option("encoding", "utf-8") //utf-8
      //"D:\\data\\dns_data_test.csv"
      //"/soft/data/DNS_DATA/dns_data_test.csv"
      .load("/soft/data/DNS_DATA/dns_data_test.csv")

    data.show(1000, truncate = false)
    println(data.count())
    data.printSchema()

    spark.stop()
  }
}
