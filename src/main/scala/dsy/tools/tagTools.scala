package dsy.tools

import dsy.config.configs.HANDLE_RULE_FILENAME
import dsy.tools.ruleMapTools.GetRulesMap
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object tagTools {

  /**
   * 依据[标签业务字段的值]与[标签规则]匹配，进行打标签（userId, tagName)
   *
   * @param dataframe 标签业务数据
   * @param field     标签业务字段
   * @param mysqlDF   5级标签数据
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

}
