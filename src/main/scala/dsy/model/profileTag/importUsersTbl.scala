package dsy.model.profileTag

import dsy.model.AbstractModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import scala.util.Random

class importUsersTbl extends AbstractModel("导入users数据至hbase") {
  /**
   * 抽象方法，对数据的具体处理,由实现类完善
   *
   * @param businessDF 业务数据
   * @param mysqlDF    后端数据
   * @return 处理后的数据
   */
  override def handle(businessDF: DataFrame, mysqlDF: DataFrame): DataFrame = {
    // udf:生成a-b的随机数,越小的数字出现概率越高
    val generateRandom = udf((minValue: Int, maxValue: Int) => {
      val range = maxValue - minValue + 1
      // 生成0到1之间的随机小数
      val randomValue = Random.nextDouble()
      // 使用指数函数调整随机数分布
      val adjustedValue = minValue + (range * math.pow(randomValue, 2)).toInt
      adjustedValue.toString
    })
    // udf:生成min-max的纯随机数
    val RandomNumber = udf((minValue: Int, maxValue: Int) =>
      (minValue + Random.nextInt(maxValue - minValue + 1)).toString
    )

    // 增加或修改字段
    val resultDF = businessDF
      // 新增教育字段，范围1-7
      .withColumn("edu", generateRandom(lit(1), lit(7)))
      // 新增民族字段,范围0-6
      .withColumn("nation", generateRandom(lit(0), lit(6)))
      // 新增籍贯字段，范围1-6(纯随机)
      .withColumn("nativePlace", RandomNumber(lit(1), lit(6)))
      // 新增政治面貌字段，范围1-6
      .withColumn("politicalface", generateRandom(lit(1), lit(3)))
      //修改国籍字段，范围1-5
      .withColumn("nationality", generateRandom(lit(1), lit(5)))
    //        resultDF.groupBy("edu").count().show
    //        resultDF.groupBy("nation").count().show
    //        resultDF.groupBy("nationality").count().show
    //    resultDF.groupBy("nativePlace").count().show
    //        resultDF.groupBy("politicalface").count().show
    //        null
    resultDF
  }
}

object importUsersTbl {
  def main(args: Array[String]): Unit = {
    new importUsersTbl().execute(4)
  }
}
