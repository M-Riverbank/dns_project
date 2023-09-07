package dsy.model.profileTag.rule

import dsy.model.AbstractModel
import dsy.tools.tagTools
import org.apache.spark.sql.DataFrame

/**
 * 标签模型开发：民族标签模型
 */
class NationModel extends AbstractModel("民族标签"){
  /**
   * 抽象方法，对数据的具体处理,由实现类完善
   *
   * @param businessDF 业务数据
   * @param mysqlDF    后端数据
   * @return 处理后的数据
   */
  override def handle(businessDF: DataFrame, mysqlDF: DataFrame): DataFrame = {
    /*
         民族
             汉族      0
             蒙古族    1
             回族      2
             藏族      3
             维吾尔族   4
             苗族      5
             满族      6
     */
    tagTools.ruleMatchTag(businessDF,"nation",mysqlDF)
    businessDF.show
    null
  }
}

object NationModel {
  def main(args: Array[String]): Unit = {
    new NationModel().execute(13)
  }
}
