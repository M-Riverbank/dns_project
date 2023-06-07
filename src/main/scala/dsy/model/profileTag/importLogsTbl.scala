package dsy.model.profileTag

import dsy.model.AbstractModel
import org.apache.spark.sql.DataFrame

class importLogsTbl extends AbstractModel("导入logs数据至hbase") {
  /**
   * 抽象方法，对数据的具体处理,由实现类完善
   *
   * @param businessDF 业务数据
   * @param mysqlDF    后端数据
   * @return 处理后的数据
   */
  override def handle(businessDF: DataFrame, mysqlDF: DataFrame): DataFrame = {
    businessDF
  }
}
object importLogsTbl{
  def main(args: Array[String]): Unit = {
    new importLogsTbl().execute(5)
  }
}
