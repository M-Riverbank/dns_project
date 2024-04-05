package dsy.model.ml.clustering

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.mllib.clustering.DistanceMeasure
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable

/**
 * 针对鸢尾花数据集进行聚类，使用KMeans算法，采用肘部法则Elbow获取K的值
 */
object IrisElbowClustering {
  def main(args: Array[String]): Unit = {
    // 构建SparkSession实例对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    import spark.implicits._
    // 1. 加载鸢尾花数据，使用libsvm格式
    val irisDF: DataFrame = spark.read
      .format("libsvm")
      .load("datas/iris_kmeans.txt")
    /*
        root
        |-- label: double (nullable = true)
        |-- features: vector (nullable = true)
    */
    // 2. 设置不同K，从2开始到6，采用肘部法确定K值
    val clusters: immutable.Seq[(Double, Int, String)] = (2 to 6).map {
      k =>
        // a. 构建KMeans算法实例
        val kmeans: KMeans = new KMeans()
          .setFeaturesCol("features")
          .setPredictionCol("prediction")
          // 设置K
          .setK(k)
          .setMaxIter(20)
          // 设置距离计算方式：欧式距离和余弦距离
          //.setDistanceMeasure(DistanceMeasure.EUCLIDEAN)
          .setDistanceMeasure(DistanceMeasure.COSINE)
        // b. 算法应用数据训练模型
        val kmeansModel: KMeansModel = kmeans.fit(irisDF)
        // c. 模型预测，对数据聚类
        val predictionDF: DataFrame = kmeansModel.transform(irisDF)
        // d. 模型评估器
        val evaluator: ClusteringEvaluator = new ClusteringEvaluator()
          .setPredictionCol("prediction")
          //.setDistanceMeasure("squaredEuclidean") // 欧式距离(默认)
          .setDistanceMeasure("cosine") // 余弦距离
          .setMetricName("silhouette") // 轮廓系数
        // e. 计算轮廓系数和统计各个类别个数
        val silhouette: Double = evaluator.evaluate(predictionDF)
        val preResult: String = predictionDF
          .groupBy($"prediction")
          .count
          .select($"prediction", $"count")
          .as[(Int, Long)]
          .rdd
          .collect
          .mkString("[", ",", "]")
        // f. 返回二元组
        (silhouette, k, preResult)
    }
    // 打印聚类中K值及SH 轮廓系数
    /*
        (0.8501515983265806,2,[(0,53),(1,97)])
        (0.7342113066202725,3,[(2,39),(1,50),(0,61)])
        (0.6748661728223084,4,[(2,28),(1,50),(3,43),(0,29)])
        (0.5593200358940349,5,[(4,17),(2,30),(1,33),(3,47),(0,23)])
        (0.5157126401818913,6,[(5,18),(4,13),(2,47),(1,19),(0,30),(3,23)])
    从上述结果可知，当K=3时，聚类是比较好的
    使用余弦距离计算，结果如下，同样表明K=3时，聚类效果最好
        (0.9579554849242657,2,[(1,50),(0,100)])
        (0.7484647230660575,3,[(2,46),(1,50),(0,54)])
        (0.5754341193280768,4,[(2,46),(1,19),(3,31),(0,54)])
        (0.6430770644178772,5,[(2,23),(4,22),(1,50),(3,28),(0,27)])
        (0.4512255960897416,6,[(5,21),(2,43),(4,18),(1,29),(3,15),(0,24)])
    */
    clusters.foreach(println)
    // 应用结束，关闭资源    spark.stop()
  }
}
