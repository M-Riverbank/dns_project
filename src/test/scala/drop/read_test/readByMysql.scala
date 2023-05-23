package drop.read_test

import dsy.model.AbstractModel
import org.apache.spark.sql.DataFrame

class readByMysql extends AbstractModel("测试读取新数据库"){
  /**
   * 抽象方法，对数据的具体处理,由实现类完善
   *
   * @param businessDF 业务数据
   * @param mysqlDF    后端数据
   * @return 处理后的数据
   */
  override def handle(businessDF: DataFrame, mysqlDF: DataFrame): DataFrame = {
    businessDF.show()
    businessDF.printSchema()
    null
  }
}

object readByMysql{
  def main(args: Array[String]): Unit = {
    new readByMysql().execute(1,isHive = true)
  }
}
