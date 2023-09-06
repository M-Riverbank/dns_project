package drop


object pro {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.{SparkSession, SaveMode}

    // 创建 SparkSession 对象
    val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    // 设置 MySQL 连接信息
    val mysqlHost = "hadoop01"
    val mysqlPort = "3306"
    val mysqlDatabase = "question"
    val mysqlUser = "root"
    val mysqlPassword = "3388047303"

    // 定义要读取和保存的表的名称
    val tableNames = Seq("base_province", "base_region", "order_info","show_dim_machine","show_fact_change_record","show_fact_environment_data","show_fact_produce_record","sku_info","user_info")

    // 定义 MySQL 连接 URL
    val mysqlUrl = s"jdbc:mysql://$mysqlHost:$mysqlPort/$mysqlDatabase?useSSL=false&user=$mysqlUser&password=$mysqlPassword"

    // 遍历每个表名，读取数据并保存为 JSON 文件
    tableNames.foreach { tableName =>
      val df = spark.read
        .format("jdbc")
        .option("url", mysqlUrl)
        .option("dbtable", tableName)
        .load()

      // 将数据保存为 JSON 文件
      df.write
        .mode(SaveMode.Overwrite)
        .json(s"path/to/$tableName.json")
    }

    // 关闭 SparkSession
    spark.stop()

  }
}