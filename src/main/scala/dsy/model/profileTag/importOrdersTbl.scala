package dsy.model.profileTag

import dsy.model.AbstractModel
import org.apache.spark.sql.DataFrame

class importOrdersTbl extends AbstractModel("导入orders数据至hbase") {
  /**
   * 抽象方法，对数据的具体处理,由实现类完善
   *
   * @param businessDF 业务数据
   * @param mysqlDF    后端数据
   * @return 处理后的数据
   */
  override def handle(businessDF: DataFrame, mysqlDF: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    import businessDF.sparkSession.implicits._
    //去除id为空的字段
    businessDF.where($"id".isNotNull)
  }
}
object importOrdersTbl{
  def main(args: Array[String]): Unit = {
    new importOrdersTbl().execute(6)
  }
}
