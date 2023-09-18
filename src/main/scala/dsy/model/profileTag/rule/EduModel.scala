package dsy.model.profileTag.rule

import dsy.model.AbstractModel
import dsy.tools.profileTag.tagTools
import org.apache.spark.sql.DataFrame

/**
 * 标签模型开发：教育标签模型
 */
class EduModel extends AbstractModel("教育标签") {
  /**
   * 抽象方法，对数据的具体处理,由实现类完善
   *
   * @param businessDF 业务数据
   * @param mysqlDF    后端数据
   * @return 处理后的数据
   */
  override def handle(businessDF: DataFrame, mysqlDF: DataFrame): DataFrame = {
    /*
          学历
             小学     1
             初中     2
             高中     3
             大专     4
             本科     5
             研究生    6
             博士     7
     */
    tagTools.ruleMatchTag(businessDF, "edu", mysqlDF)
  }
}

object EduModel {
  def main(args: Array[String]): Unit = {
    new EduModel().execute(8)
  }
}
