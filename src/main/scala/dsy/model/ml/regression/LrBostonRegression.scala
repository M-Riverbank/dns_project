package dsy.model.ml.regression

import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * 使用线性回归算法对波士顿房价数据集构建一个回归数据集,评估模型性能
 * TODO: 波士顿房价数据集，共506条数据，13个属性（特征值，features)，1个房价（预测值，target)
 */
object LrBostonRegression {
  def main(args: Array[String]): Unit = {
    //构建sparksession实例对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    import spark.implicits._

    //TODO: 1,加载波士顿房价数据集
    val bostonPriceDF: Dataset[String] = spark.read
      .textFile("datas/housing/housing.data")
      //过滤数据
      .filter(line =>
        line != null && line.trim.split("\\s+").length == 14
      )

    //TODO: 2,获取特征features与标签label
    val bostonDF: DataFrame = bostonPriceDF
      .mapPartitions { iter =>
        iter.map { line =>
          val parts: Array[String] = line.trim.split("\\s+")
          //获取标签target
          val target: Double = parts(parts.length - 1).toDouble
          //获取特征
          val values: Array[Double] = parts.dropRight(1).map(_.toDouble)
          val features: linalg.Vector = Vectors.dense(values)
          //返回二元组
          (features, target)
        }
      } //调用toDF指定列名称
      .toDF("features", "label")
    //bostonDF.printSchema()
    //bostonDF.show()

    //TODO:需要将数据集分为训练数据集与测试数据集
    val Array(trainingDF, testingDF) = bostonDF.randomSplit(Array(0.8, 0.2))
    trainingDF.persist(StorageLevel.MEMORY_AND_DISK).count() //触发缓存

    //TODO: 3,特征数据转换处理(归一化)等
    //由于线性回归算法默认情况下会对features数据进行标准化转换,所以在此不再考虑

    /*
       监督学习·
           label属于离散值使用分类算法
                   分类算法属于最多的算法,比如:
                   1. 决策树分类算法
                   2. 朴素贝叶斯算法,适合构建文本数据特征分类,比如垃圾邮件,情感分析
                   3. 逻辑回归算法
                   4. 线性支持向量机分类算法
                   5. 神经网络相关分类算法，比如多层感知肌算法 --> 深度学习算法
                   6. 集成融合算法:随机森林算法(RF算法)、梯度提升树(GBT算法)
           label属于连续值使用线性回归算法
                   线性回归算法(代价函数j(θ)求最小)
                   -最小二乘法（矩阵相乘,交替最小二乘法(ALS):多用于推荐系统）RDD
                   -梯度下降法(求导微积分)RDD
                   -牛顿迭代法(泰勒公式)DataFrame
       非监督学习
            ....
  */
    //TODO: 4,创建线性回归算法实例对象,设置相关参数,并且应用数据训练模型
    val lr: LinearRegression = new LinearRegression()
      //设置特征列和标签列的名称
      .setFeaturesCol("features")
      .setLabelCol("label")
      //是否对数据进行标准化转化处理
      .setStandardization(true)
      //设置算法底层的求解方式,要么是最小二乘法,要么是拟牛顿法(默认)
      .setSolver("auto")
      //设置相关超参数值
      .setMaxIter(30)
      .setRegParam(1)
      .setElasticNetParam(0.4)
    val lrModel: LinearRegressionModel = lr.fit(trainingDF)//训练模型

    //TODO: 5,评估模型
    //Coefficients:斜率k  Intercept:截距b
    println(s"Coefficients: ${lrModel.coefficients}，Intercept: ${lrModel.intercept}")
    // Summarize the model over the training set and print out some metricsval
    val trainingSummary = lrModel.summary
    //RMSE:均方根误差
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

    //TODO 6．模型预测
    lrModel.transform(testingDF).show(numRows = 10, truncate = false)


    //应用结束,关闭资源你
    spark.close
  }
}
