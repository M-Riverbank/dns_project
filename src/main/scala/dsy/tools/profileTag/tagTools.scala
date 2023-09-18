package dsy.tools.profileTag

import dsy.config.configs.HANDLE_RULE_FILENAME
import dsy.tools.ruleMapTools.GetRulesMap
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object tagTools {

  /**
   * 依据[标签业务字段的值]与[标签规则]匹配，进行打标签（userId, tagName)
   * 适用规则标签
   *
   * @param dataframe 标签业务数据
   * @param field     标签业务字段
   * @param mysqlDF   标签数据
   * @return 标签模型数据
   */
  def ruleMatchTag(
                    dataframe: DataFrame,
                    field: String,
                    mysqlDF: DataFrame
                  ): DataFrame = {
    val spark: SparkSession = dataframe.sparkSession
    import spark.implicits._
    // 1. 获取规则rule与tagId集合
    val attrTagRuleMap: Map[String, String] = GetRulesMap(mysqlDF, HANDLE_RULE_FILENAME)
    // 2. 将Map集合数据广播出去
    val attrTagRuleMapBroadcast = spark.sparkContext.broadcast(attrTagRuleMap)
    // 3. 自定义UDF函数, 依据Job职业和属性标签规则进行标签化
    val field_to_tag: UserDefinedFunction = udf(
      (field: String) => attrTagRuleMapBroadcast.value(field)
    )
    // 4. 计算标签，依据业务字段值获取标签ID
    val modelDF: DataFrame = dataframe
      .select(
        $"id".as("userId"),
        field_to_tag(col(field)).as(field)
      )
    // 5. 返回计算标签数据
    modelDF
  }


  /**
   * 将标签数据中属性标签规则rule拆分为范围: start, end
   *
   * @param mysqlDF 标签数据
   * @return 数据集DataFrame
   */
  def convertTuple(mysqlDF: DataFrame): DataFrame = {
    // 导入隐式转换和函数库
    val spark: SparkSession = mysqlDF.sparkSession
    import spark.implicits._
    //1.自定义udf函数,解析属性标签规则rule
    val rule_to_tuple: UserDefinedFunction = udf(
      (rule: String) => {
        val Array(start, end) = rule.trim.split("-").map(_.toInt)
        (start, end)
      }
    )

    //2.针对属性标签数据中的规则rule使用UDF函数，提取start与end并返回数据
    GetRulesMap(mysqlDF, HANDLE_RULE_FILENAME)
      .toList.toDF("name", "rule")
      .select(
        $"name",
        rule_to_tuple($"rule").as("rules")
      )
      // 获取起始start和结束end
      .select(
        $"name",
        $"rules._1".as("start"),
        $"rules._2".as("end")
      )
  }

}
