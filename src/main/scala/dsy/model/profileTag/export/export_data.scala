package dsy.model.profileTag.export

import dsy.model.AbstractModel
import org.apache.spark.sql.DataFrame

class export_data extends AbstractModel("导出Hbase用户画像数据"){
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

object export_data{
  def main(args: Array[String]): Unit = {
    new export_data().execute(37)
  }
}
