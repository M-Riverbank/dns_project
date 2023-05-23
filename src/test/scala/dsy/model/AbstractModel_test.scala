package dsy.model
import org.apache.spark.sql.DataFrame

class AbstractModel_test extends AbstractModel("测试新的mysql服务器是否读取正常") {

  /**
   * 抽象方法，对数据的具体处理,由实现类完善
   *
   * @param businessDF 业务数据
   * @param mysqlDF    后端数据
   * @return 处理后的数据
   */
  override def handle(businessDF: DataFrame, mysqlDF: DataFrame): DataFrame = {
    mysqlDF.show()
    null
  }
}

object AbstractModel_test{
  def main(args: Array[String]): Unit = {
    new AbstractModel_test().execute(1,isHive = true)
  }
}
