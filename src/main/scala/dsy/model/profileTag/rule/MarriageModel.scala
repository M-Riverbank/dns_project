package dsy.model.profileTag.rule

import dsy.model.AbstractModel
import dsy.tools.profileTag.tagTools
import org.apache.spark.sql.DataFrame
/**
 * 标签模型开发：婚姻状况标签模型
 */
class MarriageModel extends AbstractModel("婚姻状况标签"){
  /**
   * 抽象方法，对数据的具体处理,由实现类完善
   *
   * @param businessDF 业务数据
   * @param mysqlDF    后端数据
   * @return 处理后的数据
   */
  override def handle(businessDF: DataFrame, mysqlDF: DataFrame): DataFrame = {
    /*
          婚姻状况
              1    未婚
              2    已婚
              3    离异
     */
    tagTools.ruleMatchTag(businessDF, "marriage", mysqlDF)
  }
}

object MarriageModel{
  def main(args: Array[String]): Unit = {
    new MarriageModel().execute(10)
  }
}