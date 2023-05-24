package dsy.model.ml.features


import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 读取鸢尾花数据集，封装特征值features和标签处理label
 *
 * TODO 基于DataFrame API实现
 */
object irisFeaturesDemo {
  def main(args: Array[String]): Unit = {
    //构建sparkSession实例对象
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()

    //TODO 1.加载鸢尾花数据集
    val irisSchema: StructType =
      new StructType()
        .add("sepal_length", DoubleType, nullable = true)
        .add("sepal_width", DoubleType, nullable = true)
        .add("petal_length", DoubleType, nullable = true)
        .add("petal_width", DoubleType, nullable = true)
        .add("class", StringType, nullable = true)
    val iris: DataFrame = spark.read
      .option("sep", ",")
      //当csv文件首行不是列名称时，需要自定义schema
      .option("header", "false")
      .option("inferSchema", "false")
      .schema(irisSchema)
      .csv("datas/iris/iris.data")
    //    iris.show
    //    iris.printSchema

    //TODO 2.将尊片长度、宽度及花瓣长度、宽度封装值特征features向量中
    val assembler = new VectorAssembler()
      .setInputCols(iris.columns.dropRight(1))
      .setOutputCol("features") //添加一列，类型为向量
    val df1: DataFrame = assembler.transform(iris)
    df1.show
    df1.printSchema



    //应用结束，关闭资源
    spark.stop
  }
}
