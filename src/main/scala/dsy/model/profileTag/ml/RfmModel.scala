package dsy.model.profileTag.ml

import dsy.model.AbstractModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes


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
        rfmDF.printSchema()
        rfmDF.orderBy($"frequency").show(10, truncate = false)
    null
  }
}

object RfmModel {
  def main(args: Array[String]): Unit = {
    new RfmModel().execute(18)
  }
}