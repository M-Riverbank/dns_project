package dsy.model.ml.clustering

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 使用鸢尾花数据集基于KMeans聚类算法构建聚类模型，并对模型进行评估
 */
object IrisClusterTest {
  def main(args: Array[String]): Unit = {
    //构建sparksession实例对象
    val spark: SparkSession = SparkSession.builder
      .master("local")
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate
    import spark.implicits._
    import org.apache.spark.sql.functions._


    //1.读取数据
    val irisDF: DataFrame = spark.read
      .format("libsvm")
      .option("numFeatures", "4")
      .load("datas/iris_kmeans.txt")
    /*
        inputDF.printSchema
        inputDF.show(10, truncate = false)
        root
         |-- label: double (nullable = true)
         |-- features: vector (nullable = true)

        +-----+-------------------------------+
        |label|features                       |
        +-----+-------------------------------+
        |1.0  |(4,[0,1,2,3],[5.1,3.5,1.4,0.2])|
        |1.0  |(4,[0,1,2,3],[4.9,3.0,1.4,0.2])|
        |1.0  |(4,[0,1,2,3],[4.7,3.2,1.3,0.2])|
        |1.0  |(4,[0,1,2,3],[4.6,3.1,1.5,0.2])|
        |1.0  |(4,[0,1,2,3],[5.0,3.6,1.4,0.2])|
        |1.0  |(4,[0,1,2,3],[5.4,3.9,1.7,0.4])|
        |1.0  |(4,[0,1,2,3],[4.6,3.4,1.4,0.3])|
        |1.0  |(4,[0,1,2,3],[5.0,3.4,1.5,0.2])|
        |1.0  |(4,[0,1,2,3],[4.4,2.9,1.4,0.2])|
        |1.0  |(4,[0,1,2,3],[4.9,3.1,1.5,0.1])|
        +-----+-------------------------------+
        only showing top 10 rows
     */

    //2.构建KMeans算法
    val kMeans: KMeans = new KMeans()
      //设置特征列与预测列名称
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      //设置K为3
      .setK(3)
      //设置迭代次数
      .setMaxIter(20)
      //设置KMeans算法底层:random,k-means||
      .setInitMode("k-means||")

    //3.训练数据获得模型
    val kMeansModel: KMeansModel = kMeans.fit(irisDF)
    //获取聚类的簇中心点
    kMeansModel.clusterCenters.foreach(println)


    // 关闭资源
    spark.stop
  }
}
