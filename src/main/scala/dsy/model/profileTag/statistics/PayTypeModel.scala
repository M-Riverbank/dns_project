package dsy.model.profileTag.statistics

import dsy.model.AbstractModel
import dsy.tools.profileTag.tagTools
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * 标签模型开发：支付方式标签模型
 */
class PayTypeModel extends AbstractModel("支付方式标签") {
  /**
   * 抽象方法，对数据的具体处理,由实现类完善
   *
   * @param businessDF 业务数据
   * @param mysqlDF    后端数据
   * @return 处理后的数据
   */
  override def handle(businessDF: DataFrame, mysqlDF: DataFrame): DataFrame = {
    import businessDF.sparkSession.implicits._
    val rnkDF: DataFrame = businessDF
      .groupBy($"memberId", $"paymentCode")
      .count
      /*
          .show
          +--------+-----------+-----+
          |memberId|paymentCode|count|
          +--------+-----------+-----+
          |     939|        cod|   34|
          |     374|     alipay|   33|
          |     200|        cod|   31|
          |     793|     alipay|   32|
          |     586|        cod|   33|
          +--------+-----------+-----+
       */
      //开窗分析
      .withColumn(
        "rnk",
        row_number.over(Window.partitionBy($"memberId").orderBy($"count".desc)) // 使用窗口函数按照次数降序进行排序
      )
      /*
          .show
          +--------+-----------+-----+---+
          |memberId|paymentCode|count|rnk|
          +--------+-----------+-----+---+
          |       1|        cod|   30|  1|
          |       1|   chinapay|   30|  2|
          |       1|      wxpay|   30|  3|
          |       1|     alipay|   29|  4|
          |     102|      wxpay|   40|  1|
          |     102|     alipay|   38|  2|
          |     102|   chinapay|   34|  3|
          |     102|        cod|   33|  4|
          |     107|     alipay|   33|  1|
          +--------+-----------+-----+---+
       */
      //取使用次数最多的支付方式
      .where($"rnk" === 1)
      .select(
        $"memberId".as("id"),
        $"paymentCode".as("payment")
      )

    // 2. 计算标签，规则匹配
    val resultDF: DataFrame = tagTools.ruleMatchTag(rnkDF, "payment", mysqlDF)
    //    resultDF.groupBy($"payment").count.show
    //    resultDF.printSchema

    // 返回结果
    //    null
    resultDF
  }
}

object PayTypeModel {
  def main(args: Array[String]): Unit = {
    new PayTypeModel().execute(17)
  }
}
