package dsy.model.profileTag.rule

import dsy.model.AbstractModel
import dsy.tools.tagTools
import org.apache.spark.sql.DataFrame

/**
 * 标签模型开发：国籍标签模型
 */
class NationalityModel extends AbstractModel("国籍标签") {
  /**
   * 抽象方法，对数据的具体处理,由实现类完善
   *
   * @param businessDF 业务数据
   * @param mysqlDF    后端数据
   * @return 处理后的数据
   */
  override def handle(businessDF: DataFrame, mysqlDF: DataFrame): DataFrame = {
    /*
          国籍
            中国大陆      1
            中国香港      2
            中国澳门      3
            中国台湾      4
            其他         5
     */
    tagTools.ruleMatchTag(businessDF, "nationality", mysqlDF)
  }
}

object NationalityModel {
  def main(args: Array[String]): Unit = {
    new NationalityModel().execute(11)
  }
}