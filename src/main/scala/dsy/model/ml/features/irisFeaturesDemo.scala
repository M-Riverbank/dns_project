package dsy.model.ml.features


import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel, LogisticRegressionSummary}
import org.apache.spark.ml.feature.{StandardScaler, StringIndexer, VectorAssembler}
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

    //TODO 2.将萼片长度、宽度及花瓣长度、宽度封装值特征features向量中
    //向量装配器
    val assembler = new VectorAssembler()
      .setInputCols(iris.columns.dropRight(1)) //对哪些列进行封装
      .setOutputCol("features") //添加一列，类型为向量
    val df1: DataFrame = assembler.transform(iris)
    //    df1.show
    //    df1.printSchema

    //TODO 3.转换类别字符串数据为数值类型数据
    val indexer: StringIndexer = new StringIndexer()
      .setInputCol("class") //对哪里列数据进行索引化
      .setOutputCol("label") //数据索引化后列名

    val df2: DataFrame = indexer
      .fit(df1) //fit表示条用函数，传递 DataFrame 获取模型
      .transform(df1)
    //    df2.show
    //    df2.printSchema

    /*
      root
       |-- sepal_length: double (nullable = true)
       |-- sepal_width: double (nullable = true)
       |-- petal_length: double (nullable = true)
       |-- petal_width: double (nullable = true)
       |-- class: string (nullable = true)
       |-- features: vector (nullable = true) x
       |-- label: double (nullable = false)   y
       算法 y=kx+b
     */

    //TODO 3. 将特征数据features标准化处理
    /*
        - 机器学习核心三要素 数据(features)+算法=模型(最佳)
        - 调优中，最重要的就是特征数据features,如果特征数据比较好，处理恰当，可能得到较好模型
        TODO 在实际开发中，特征数据features需要进行各个转换操作，比如正则、归一化或标准化等等
        不同维度特征值，值的范围跨度不一样,导致模型异常
     */
    val scaler: StandardScaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scale_Features")
      .setWithStd(true) //使用标准差缩放
      .setWithMean(false) //不使用平均值缩放
    // Compute summary statistics by fitting the StandardScaler.
    val df3: DataFrame = scaler
      .fit(df2)
      .transform(df2)
    //    df3.show()
    //    df3.printSchema()

    //TODO: 4.选择分类算法,构建分类模型
    /*
      监督学习
          label属于离散值使用分类算法
                  分类算法属于最多的算法,比如:
                  1. 决策树分类算法
                  2. 朴素贝叶斯算法,适合构建文本数据特征分类,比如垃圾邮件,情感分析
                  3. 逻辑回归算法
                  4. 线性支持向量机分类算法
                  5. 神经网络相关分类算法，比如多层感知肌算法 --> 深度学习算法
                  6. 集成融合算法:随机森林算法(RF算法)、梯度提升树(GBT算法)
          label属于连续值使用回归算法
                  线性回归算法(代价函数j(θ)求最小)
                    -最小二乘法（矩阵相乘,交替最小二乘法(ALS):多用于推荐系统）RDD
                    -梯度下降法(求导微积分)RDD
                    -牛顿迭代法(泰勒公式)DataFrame
     */
    val lr: LogisticRegression = new LogisticRegression()
      //设置特征值的列名称与标签值的列名称
      .setFeaturesCol("scale_Features") //x -> 特征
      .setLabelCol("label") //y -> 标签
      //每个算法都有自己的超参数设置,比较关键,合理的设置会获得较好的模型
      .setMaxIter(30) //最大迭代次数
      .setFamily("multinomial") //设置分类属于二分类还是多分类(就是标签label有多少个值)
      .setStandardization(true) //是否对特征数据进行标准化
      .setRegParam(0) //正则化参数,优化
      .setElasticNetParam(0) //弹性化参数,优化

    //TODO: 将数据运用于算法中,训练模型
    val lrModel: LogisticRegressionModel = lr.fit(df3)


    //TODO: 评估模型
    println(s"多分类混淆矩阵:\n${lrModel.coefficientMatrix}")
    val summary: LogisticRegressionSummary = lrModel.summary
    //模型整体的预测准确度，0.98
    println(s"模型准确度:${summary.accuracy}") //模型准确度%
    //模型中每个target的预测准确度，1.0,0.9795918367346939,0.9607843137254902
    println(s"每个target的精度:${summary.precisionByLabel.mkString(",")}") //每个target的精度

    //应用结束，关闭资源
    spark.stop
  }
}
