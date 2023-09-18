package dsy.model.profileTag.rule

import dsy.model.AbstractModel
import dsy.tools.profileTag.tagTools
import org.apache.spark.sql.DataFrame

/**
 * 标签模型开发：政治面貌标签模型
 */
class PoliticalModel extends AbstractModel("政治面貌标签") {
  /**
   * 抽象方法，对数据的具体处理,由实现类完善
   *
   * @param businessDF 业务数据
   * @param mysqlDF    后端数据
   * @return 处理后的数据
   */
  override def handle(businessDF: DataFrame, mysqlDF: DataFrame): DataFrame = {
    /*
          政治面貌
               1    群众
               2    党员
               3    无党派人士
     */
    tagTools.ruleMatchTag(businessDF, "politicalface", mysqlDF)
  }
}

object PoliticalModel {
  def main(args: Array[String]): Unit = {
    new PoliticalModel().execute(14)
  }
}