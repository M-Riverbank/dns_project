package dsy.model.profileTag.statistics

import dsy.model.AbstractModel
import org.apache.spark.sql.DataFrame
/**
 * 标签模型开发：消费周期标签模型
 */
class ConsumeCycleModel extends AbstractModel("消费周期标签"){
  /**
   * 抽象方法，对数据的具体处理,由实现类完善
   *
   * @param businessDF 业务数据
   * @param mysqlDF    后端数据
   * @return 处理后的数据
   */
  override def handle(businessDF: DataFrame, mysqlDF: DataFrame): DataFrame = {
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
    businessDF.show
    businessDF.printSchema
    null
  }
}

object ConsumeCycleModel{
  def main(args: Array[String]): Unit = {
    new ConsumeCycleModel().execute(16)
  }
}
