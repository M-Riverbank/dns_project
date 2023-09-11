package dsy.model.profileTag.rule

import dsy.model.AbstractModel
import dsy.tools.tagTools
import org.apache.spark.sql.DataFrame

/**
 * 标签模型开发：籍贯标签模型
 */
class NativePlaceModel extends AbstractModel("籍贯标签") {
  /**
   * 抽象方法，对数据的具体处理,由实现类完善
   *
   * @param businessDF 业务数据
   * @param mysqlDF    后端数据
   * @return 处理后的数据
   */
  override def handle(businessDF: DataFrame, mysqlDF: DataFrame): DataFrame = {
    /*
        籍贯标签
            1    北京
            2    上海
            3    广州
            4    深圳
            5    杭州
            6    苏州
     */
    tagTools.ruleMatchTag(businessDF, "nativePlace", mysqlDF)
  }
}

object NativePlaceModel {
  def main(args: Array[String]): Unit = {
    new NativePlaceModel().execute(13)
  }
}


