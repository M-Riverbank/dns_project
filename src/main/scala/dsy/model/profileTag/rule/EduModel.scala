package dsy.model.profileTag.rule


import dsy.model.AbstractModel
import dsy.tools.tagTools
import org.apache.spark.sql.DataFrame


/**
 * 标签模型开发：学历标签模型
 */
class EduModel extends AbstractModel("学历标签") {
  /*
    学历
        1小学    小学
        2初中    初中
        3高中    高中
        4大专    大专
        5本科    本科
        6研究生   研究生
        7博士    博士
   */

  /**
   * 抽象方法，对数据的具体处理,由实现类完善
   *
   * @param businessDF 业务数据
   * @param mysqlDF    后端数据
   * @return 处理后的数据
   */
  override def handle(businessDF: DataFrame, mysqlDF: DataFrame): DataFrame = {
    //    tagTools.ruleMatchTag(businessDF,"edu",mysqlDF)
    mysqlDF.printSchema()
    println(mysqlDF)
    null
  }
}

object EduModel {
  def main(args: Array[String]): Unit = {
    new EduModel().execute(7)
  }
}

