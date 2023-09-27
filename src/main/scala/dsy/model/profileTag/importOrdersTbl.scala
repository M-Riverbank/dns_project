package dsy.model.profileTag

import dsy.model.AbstractModel
import dsy.tools.RandomNumberTools._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.LongType

import scala.util.Random

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

    // 订单编号:生成随机数字字符串的函数
    def generateRandomString(length: Int): String = {
      val random = new Random()
      val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
      val randomString = (1 to length).map(_ => chars(random.nextInt(chars.length))).mkString
      randomString
    }

    val generateRandomNumberUDF = udf(generateRandomString(_: Int): String)

    // 订单金额
    val data = List(
      7.0, 58.0, 289.0, 299.0, 499.0, 899.0, 1099.42,
      1299.0, 1558.0, 1649.0, 1699.0, 1899.0, 1899.0,
      1999.0, 1999.0, 2449.0, 2479.45, 2488.0, 3149.0, 3449.0
    )
    val generateRandomValueUDF = udf(
      (index: Int) => data(index).toString
    )

    //自定义UDF函数，处理UserId: 订单表数据中会员ID -> memberid
    val user_id_udf: UserDefinedFunction = udf(
      (userId: String) => {
        if (userId.toInt >= 950) {
          val id = new Random().nextInt(950) + 1
          id.toString
        } else {
          userId
        }
      }
    )


    // 添加新列到 DataFrame
    val min_time: Long = System.currentTimeMillis() / 1000 - 15638400
    val max_time: Long = System.currentTimeMillis() / 1000
    val fix_time: Long = System.currentTimeMillis() / 1000 -
      businessDF
        .select(max($"finishTime").as("maxTime"))
        .first
        .getAs[Integer]("maxTime")
    // 将会员ID值和支付方式值，使用UDF函数
    businessDF
      // 会员id赋值
      .withColumn("memberId", user_id_udf($"memberId"))
      // 支付方式赋值
      //      .withColumn("paymentCode", RandomNumber(lit(0), lit(3)))
      //      .withColumn("paymentCode", pay_code_udf($"paymentCode"))
      //      .withColumn("paymentName", pay_name_udf($"paymentCode"))
      // 修改订单时间
      .withColumn("finishTime", $"finishTime".cast(LongType) + lit(fix_time))
    //      .withColumn("finishTime",
    //        generateRandom_possibility(
    //          lit(min_time),
    //          lit(max_time),
    //          lit(50)
    //        )
    //      )
    //订单编号赋值
    //      .withColumn("orderSn", generateRandomNumberUDF(lit(15)))
    // 订单金额产生
    //      .withColumn("orderAmount", generateRandom_possibility(lit(0), lit(data.length - 1), lit(2)))
    //      .withColumn("orderAmount", generateRandomValueUDF($"orderAmount"))
    // 去除空值
    //      .where($"id".isNotNull)
    //      .select($"id", $"orderAmount")
    //              .show
    //      .printSchema
    //    null
  }
}

object importOrdersTbl {
  def main(args: Array[String]): Unit = {
    new importOrdersTbl().execute(6)
  }
}
