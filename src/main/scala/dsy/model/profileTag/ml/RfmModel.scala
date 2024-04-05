package dsy.model.profileTag.ml

import dsy.model.AbstractModel
import dsy.tools.profileTag.tagTools
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.storage.StorageLevel


/**
 * 挖掘类型模型开发: 客户价值模型RFM
 */
class RfmModel extends AbstractModel("RFM标签") {
  /**
   * 抽象方法，对数据的具体处理,由实现类完善
   *
   * @param businessDF 业务数据
   * @param mysqlDF    后端数据
   * @return 处理后的数据
   */
  override def handle(businessDF: DataFrame, mysqlDF: DataFrame): DataFrame = {
    /*
          0       高价值
          1       中上价值
          2       中价值
          3       中下价值
          4       超低价值
     */
    import businessDF.sparkSession.implicits._
    logWarning("testing..................")
    /*
        businessDF.show
        businessDF.printSchema
          +--------+---------------+-----------+----------+
          |memberId|        orderSn|orderAmount|finishTime|
          +--------+---------------+-----------+----------+
          |     938|8tphZfeEgru2i3T|    1099.42|1680163933|
          |     410|DmRnDsdoTkvN9gr|       58.0|1679386427|
          |     662|xJJyebBYf2nP2X6|        7.0|1679386427|
          |     291|dQET33DPMjKaApm|      299.0|1683497056|
          |     842|HJNpJ7cD5oaPOf6|      499.0|1679386427|
          |     368|yzFqdUpe9BelX2T|     1299.0|1679386427|
          |     471|7wifY8VbqIg0i6L|     1999.0|1679386427|
          |     478|PkfChFF93kwQUQm|       58.0|1679386427|
          |      74|diUreCgtWEBw1r5|     2488.0|1679386427|
          |     609|kt67m72QMhbyZ4p|        7.0|1683748878|
          |     927|pzfXqKz2DWqT97i|     2488.0|1679386427|
          |     688|XOANbR715Bi1uF9|     1699.0|1684571470|
          |      31|ZL78To4IPRXP2yr|    2479.45|1679386427|
          |     490|kQ5H0EwPazlXkKA|     1699.0|1679386427|
          |     631|iPUWgKgL5ce0GJl|       58.0|1679386427|
          |       3|v0Jb14EYeE2wXh1|     1558.0|1679386427|
          |     213|vT6ulO5SYLEMrEg|     1899.0|1679386427|
          |     363|7KmowpC5pwADohV|      499.0|1680194661|
          |     564|FMREvvijCAuCOU3|        7.0|1679386427|
          |     543|kTWYconl7K0GRij|     1558.0|1690425344|
          +--------+---------------+-----------+----------+
          only showing top 20 rows

          root
           |-- memberId: string (nullable = true)
           |-- orderSn: string (nullable = true)
           |-- orderAmount: string (nullable = true)
           |-- finishTime: string (nullable = true)
     */

    /*
        TODO: 1·计算每个用户RFM值
            按照用户memberid分组·然后进行娶合函数娶合统计
            R:消费周期·finishtime
                日期时间函数: current_timestamp · from_unixtimestamp · datediff
            F:消费次数ordersn
                count
            M:消费金额orderamount
                sum
     */
    val rfmDF: DataFrame = businessDF
      //a.按照用户id分组,对每个用户的订单数据进行操作
      .groupBy($"memberid")
      .agg(
        max($"finishtime").as("max_finishtime"), //最近消费
        count($"ordersn").as("frequency"), //订单数量
        sum(
          $"orderamount"
            //订单金额String类型转换
            .cast(DataTypes.createDecimalType(10, 2))
        ).as("monetary") //订单总额
      ) //计算R值
      .select(
        $"memberid".as("userId"),
        //计算R值:消费周期
        datediff(
          current_timestamp(), from_unixtime($"max_finishtime")
        ).as("recency"),
        $"frequency",
        $"monetary"
      )
    //        rfmDF.printSchema()
    //        rfmDF
    //          .orderBy($"monetary")
    //          .show(10, truncate = false)

    /*
    TODO: 2·按照规则给RFM进行打分（RFM_SCORE)
        R: 1-3天=5分·4-6天=4分·7-9天=3分·10-15天=2分﹐大于16天=1分
        F: >200=5分·150-199=4分·100-149=3分·50-99=2分·1-49=1分
        M: >20w=5分·10-19w=4分·5-9w=3分·1-4w=2分，<1w=1分
        使用CASE WHEN .. WHEN... ELSE .... END
     */
    // R 打分条件表达式
    val rWhen: Column =
    when($"recency".between(1, 3), 5.0)
      .when($"recency".between(4, 6), 4.0)
      .when($"recency".between(7, 9), 3.0)
      .when($"recency".between(10, 15), 2.0)
      .when($"recency".geq(16), 1.0)
    // F 打分条件表达式
    val fWhen: Column =
      when($"frequency".between(1, 49), 1.0)
        .when($"frequency".between(50, 99), 2.0)
        .when($"frequency".between(100, 149), 3.0)
        .when($"frequency".between(150, 199), 4.0)
        .when($"frequency".geq(200), 5.0)
    // M 打分条件表达式
    val mWhen: Column =
      when($"monetary".lt(10000), 1.0)
        .when($"monetary".between(10000, 49999), 2.0)
        .when($"monetary".between(50000, 99999), 3.0)
        .when($"monetary".between(100000, 199999), 4.0)
        .when($"monetary".geq(200000), 5.0)
    val rfmScoreDF: DataFrame = rfmDF.select(
      $"userId",
      rWhen.as("r_score"),
      fWhen.as("f_score"),
      mWhen.as("m_score")
    )
    //    rfmScoreDF.printSchema
    //    rfmScoreDF.show(100, truncate = false)


    /*
    TODO:3. 训练模型
        KMeans算法，其中K=5
     */
    // 3.1组合R\F\M列为特征值features
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("r_score", "f_score", "m_score"))
      .setOutputCol("features")
    val featuresDF: DataFrame = assembler.transform(rfmScoreDF)
    //将训练数据缓存
    featuresDF.persist(StorageLevel.MEMORY_AND_DISK)
    // 3.2 使用KMeans聚类算法模型训并获取模型
    val kMeansModel: KMeansModel = trainModel(featuresDF)
    // 使用模型预测
    val predictionDF: DataFrame = kMeansModel.transform(featuresDF)

    //TODO: 4 打标签
    //4.1聚类类簇关联属性标签数据rule，对应聚类类簇与标签tagName
    val indexTagMap: Map[Int, String] =
    tagTools.convertIndexMap(kMeansModel.clusterCenters, mysqlDF)
    //使用KMeansModel预测值prediction打标签
    // a.将索引标签Map集合厂广播变量广播出去
    val indexTagMapBroadcast = spark.sparkContext.broadcast(indexTagMap)
    // b.自定义UDF函数,传递预测值 prediction ,返回标签名称 tagName
    val field_to_tag: UserDefinedFunction = udf(
      (clusterIndex: Int) => indexTagMapBroadcast.value(clusterIndex)
    )
    //c.打标签
    val modelDF: DataFrame = predictionDF
      .select(
        $"userId", //用户ID
        field_to_tag(col("prediction")).as("rfm")
      )
    modelDF.printSchema()
    modelDF.show(100, truncate = false)

    modelDF
  }

  /**
   * 使用KMeans算法训练模型
   *
   * @param dataframe 数据集
   * @return KMeansModel模型
   */
  private def trainModel(dataframe: DataFrame): KMeansModel = {
    //使用KMeans聚类算法模型训练
    val kMeansModel: KMeansModel = new KMeans()
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      .setK(5) // 设置列簇个数：5
      .setMaxIter(20) // 设置最大迭代次数
      .fit(dataframe)
    //均方根误差(越小越好)
    //println(s"WSSSE = ${kMeansModel.computeCost(featuresDF)}")
    //WSSSE = 4.614836295542919E-28
    // 返回
    kMeansModel
  }
}

object RfmModel {
  def main(args: Array[String]): Unit = {
    new RfmModel().execute(18)
  }
}