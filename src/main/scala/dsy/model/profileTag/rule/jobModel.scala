package dsy.model.profileTag.rule

import dsy.model.AbstractModel
import dsy.tools.tagTools
import org.apache.spark.sql.DataFrame

/**
 * 标签模型开发：职业标签模型
 */
class jobModel extends AbstractModel("职业标签") {
  /**
   * 抽象方法，对数据的具体处理,由实现类完善
   *
   * @param businessDF 业务数据
   * @param mysqlDF    后端数据
   * @return 处理后的数据
   */
  override def handle(businessDF: DataFrame, mysqlDF: DataFrame): DataFrame = {
    /*
        职业
            学生      1
            公务员     2
            军人      3
            警察      4
            教师      5
            白领      6
     */
    tagTools.ruleMatchTag(businessDF, "job", mysqlDF)
  }
}

object jobModel {
  def main(args: Array[String]): Unit = {
    new jobModel().execute(9)
  }
}
