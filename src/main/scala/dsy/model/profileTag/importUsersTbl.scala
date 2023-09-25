package dsy.model.profileTag

import dsy.model.AbstractModel
import dsy.tools.RandomNumberTools._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class importUsersTbl extends AbstractModel("导入users数据至hbase") {
  /**
   * 抽象方法，对数据的具体处理,由实现类完善
   *
   * @param businessDF 业务数据
   * @param mysqlDF    后端数据
   * @return 处理后的数据
   */
  override def handle(businessDF: DataFrame, mysqlDF: DataFrame): DataFrame = {


    // 增加或修改字段
    val resultDF = businessDF
      // 新增教育字段，范围1-7
      .withColumn("edu", generateRandom_possibility(lit(1), lit(7)))
      // 新增民族字段,范围0-6
      .withColumn("nation", generateRandom_possibility(lit(0), lit(6)))
      // 新增籍贯字段，范围1-6(纯随机)
      .withColumn("nativePlace", RandomNumber(lit(1), lit(6)))
      // 新增政治面貌字段，范围1-6
      .withColumn("politicalface", generateRandom_possibility(lit(1), lit(3)))
      //修改国籍字段，范围1-5
      .withColumn("nationality", generateRandom_possibility(lit(1), lit(5)))
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
