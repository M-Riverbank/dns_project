package dsy.model.profileTag.statistics

import dsy.model.AbstractModel
import dsy.tools.profileTag.tagTools
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.time.{Instant, LocalDateTime, ZoneOffset}

/**
 * 标签模型开发：消费周期标签模型
 */
class ConsumeCycleModel extends AbstractModel("消费周期标签") {
  /**
   * 抽象方法，对数据的具体处理,由实现类完善
   *
   * @param businessDF 业务数据
   * @param mysqlDF    后端数据
   * @return 处理后的数据
   */
  override def handle(businessDF: DataFrame, mysqlDF: DataFrame): DataFrame = {
    import businessDF.sparkSession.implicits._
    /*
              消费周期
                  近7天     0-7
                  近2周     8-14
                  近1月     15-30
                  近2月     31-60
                  近3月     61-90
                  近4月     91-120
                  近5月     121-150
                  近半年     151-180
                  超过半年    181-100000
     */
    //1.使用udf提取start与end
    val TagRuleDF: DataFrame = tagTools.convertTuple(mysqlDF)

    // 2. 订单数据按照会员ID：memberId分组，获取最近一次订单完成时间： finish_time
    val dayDF: DataFrame = businessDF
      // 2.1. 分组，获取最新订单时间，并转换格式
      .groupBy($"memberId")
      .agg(
        //取多个订单中最近的一次时间转换为日期格式
        from_unixtime(
          //将Long类型转换日期时间类型,计算天数可以不需要时间，可以指定格式为年月日格式
          max($"finishTime"), "yyyy-MM-dd"
        ).as("finish_time")
      ) // 2.2. 计算用户最新订单距今天数
      .select(
        $"memberId".as("userId"),
        datediff(current_date(), $"finish_time").as("consumer_days")
      )

    // 3. 关联属性标签数据和消费天数数据，加上判断条件，进行打标签
    val resultDF: DataFrame = dayDF
      .join(TagRuleDF)
      .where($"consumer_days".between($"start", $"end"))
      .select($"userId", $"name".as("consumerCycle"))

    // 4. 返回标签数据
    //    resultDF.show
    //    resultDF.printSchema()
    //    resultDF.groupBy($"consumerCycle").count.show
    //    modelDF.groupBy("consumerCycle").count.show
    //    modelDF.printSchema()
    //    val function = udf(
    //      (finishTime: String) => {
    //        val instant = Instant.ofEpochSecond(finishTime.toInt)
    //        val localDateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC)
    //
    //        val year = localDateTime.getYear
    //        val month = localDateTime.getMonthValue
    //        val dayOfMonth = localDateTime.getDayOfMonth
    //        val hour = localDateTime.getHour
    //        val minute = localDateTime.getMinute
    //        val second = localDateTime.getSecond
    //
    //        s"$year/$month/$dayOfMonth $hour:$minute:$second"
    //      }
    //    )
    //    val x = businessDF
    //      .groupBy($"memberId")
    //      .agg(max($"finishTime").as("finish_time"))
    //      .select($"memberId", function($"finish_time").as("time"))
    //
    //
    //    x.orderBy($"time".desc).show(100,truncate = false)
    //    x.orderBy($"time".asc).show(100,truncate = false)


    //    null
    resultDF
  }
}

object ConsumeCycleModel {
  def main(args: Array[String]): Unit = {
    new ConsumeCycleModel().execute(16)
  }
}
