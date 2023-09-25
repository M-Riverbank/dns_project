package dsy.model.profileTag.ml

import dsy.model.AbstractModel
import org.apache.spark.sql.DataFrame

/**
 * 挖掘类型模型开发客户价值模型RFM
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
    null
  }
}

object RfmModel{
  def main(args: Array[String]): Unit = {
    new RfmModel().execute(null)
  }
}