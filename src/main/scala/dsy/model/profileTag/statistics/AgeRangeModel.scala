package dsy.model.profileTag.statistics

import dsy.model.AbstractModel
import dsy.tools.profileTag.tagTools
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/**
 * 标签模型开发：年龄段标签模型
 */
class AgeRangeModel extends AbstractModel("年龄段标签") {
  /**
   * 抽象方法，对数据的具体处理,由实现类完善
   *
   * @param businessDF 业务数据
   * @param mysqlDF    后端数据
   * @return 处理后的数据
   */
  override def handle(businessDF: DataFrame, mysqlDF: DataFrame): DataFrame = {
    import businessDF.sparkSession.implicits._
    /*
         年龄段
               50后   19500101-19591231
               60后   19600101-19691231
               70后   19700101-19791231
               80后   19800101-19891231
               90后   19900101-19991231
               00后   20000101-20091231
               10后   20100101-20191231
               20后   20200101-20291231
     */
    //1.针对属性标签数据中的规则rule使用UDF函数，提取start与end
    val TagRuleDF: DataFrame = tagTools.convertTuple(mysqlDF)
    /*
                  +----+--------+--------+
                  |name|   start|     end|
                  +----+--------+--------+
                  |70后|19700101|19791231|
                  |60后|19600101|19691231|
                  |00后|20000101|20091231|
                  |90后|19900101|19991231|
                  |50后|19500101|19591231|
                  |20后|20200101|20291231|
                  |80后|19800101|19891231|
                  |10后|20100101|20191231|
                  +----+--------+--------+

                  root
                   |-- name: string (nullable = true)
                   |-- start: integer (nullable = true)
                   |-- end: integer (nullable = true)
     */
    //2. 业务数据与标签规则关联JOIN，比较范围
    /*
        attrTagDF： attr
        businessDF: business
        SELECT t2.userId, t1.name FROM attr t1 JOIN business t2
        WHERE t1.start <= t2.birthday AND t1.end >= t2.birthday ;
    */
    // 3.1. 转换日期格式： 1982-01-11 -> 19820111
    //a.使用正则函数转换日期并且返回
    businessDF.select(
      $"id",
      regexp_replace($"birthday", "-", "")
        .cast(IntegerType).as("bornDate")
    ) //b.关联属性标签规则数据
      .join(TagRuleDF)
      .where($"bornDate".between($"start", $"end"))
      //c.选取字段
      .select(
        $"id".as("userId"),
        $"name".as("ageRange")
      )
  }
}

object AgeRangeModel {
  def main(args: Array[String]): Unit = {
    new AgeRangeModel().execute(15)
  }
}
