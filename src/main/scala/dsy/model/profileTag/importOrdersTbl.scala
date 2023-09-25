package dsy.model.profileTag

import dsy.model.AbstractModel
import dsy.tools.RandomNumberTools._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction

class importOrdersTbl extends AbstractModel("导入orders数据至hbase") {
  /**
   * 抽象方法，对数据的具体处理,由实现类完善
   *
   * @param businessDF 业务数据
   * @param mysqlDF    后端数据
   * @return 处理后的数据
   */
  override def handle(businessDF: DataFrame, mysqlDF: DataFrame): DataFrame = {
    import businessDF.sparkSession.implicits._
    import org.apache.spark.sql.functions._

    // 自定义UDF函数，处理 paymentCode
    val payCodeList = List("alipay", "wxpay", "chinapay", "cod")
    val pay_code_udf: UserDefinedFunction = udf(
      (paymentCode: String) => payCodeList(paymentCode.toInt)
    )

    // 自定义UDF函数，处理paymentName
    val payMap: Map[String, String] = Map(
      "alipay" -> "支付宝", "wxpay" -> "微信支付",
      "chinapay" -> "银联支付", "cod" -> "货到付款"
    )
    val pay_name_udf = udf(
      (paymentCode: String) => payMap(paymentCode)
    )

    val min_time: Long = System.currentTimeMillis() / 1000 - 15638400
    val max_time: Long = System.currentTimeMillis() / 1000
    // 将会员ID值和支付方式值，使用UDF函数
    businessDF
      // 成员id赋值
      .withColumn("memberId", RandomNumber(lit(1), lit(950)))
      // 支付方式赋值
      .withColumn("paymentCode", RandomNumber(lit(0), lit(3)))
      .withColumn("paymentCode", pay_code_udf($"paymentCode"))
      .withColumn("paymentName", pay_name_udf($"paymentCode"))
      // 修改订单时间
      .withColumn("finishTime",
        generateRandom_possibility(
          lit(min_time),
          lit(max_time),
          lit(50)
        )
      )
      // 去除空值
      .where($"id".isNotNull)
  }
}

object importOrdersTbl {
  def main(args: Array[String]): Unit = {
    new importOrdersTbl().execute(6)
  }
}
