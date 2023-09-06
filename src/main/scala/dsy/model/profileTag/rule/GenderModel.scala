package dsy.model.profileTag.rule


import dsy.model.AbstractModel
import dsy.tools.tagTools
import org.apache.spark.sql.DataFrame


/**
 * 标签模型开发：学历标签模型
 */
class GenderModel extends AbstractModel("学历标签") {

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

object GenderModel {
  def main(args: Array[String]): Unit = {
    new GenderModel().execute(7)
  }
}

