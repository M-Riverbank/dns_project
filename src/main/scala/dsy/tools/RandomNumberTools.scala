package dsy.tools

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import scala.util.Random

object RandomNumberTools {
  // udf:生成a-b的随机数,possibility越大，小数值出现的概率越高
  val generateRandom_possibility: UserDefinedFunction =
    udf(
      (minValue: Int, maxValue: Int, possibility: Int) => {
        val p: Int = if (possibility > 0) possibility else 10
        val range: Int = maxValue - minValue + 1
        // 生成0到1之间的随机小数
        val randomValue = Random.nextDouble()
        // 使用指数函数调整随机数分布
        val adjustedValue = minValue + (range * math.pow(randomValue, p)).toInt
        adjustedValue.toString
      }
    )

  // udf:生成min-max的纯随机数
  val RandomNumber: UserDefinedFunction =
    udf(
      (minValue: Int, maxValue: Int) =>
        (minValue + Random.nextInt(maxValue - minValue + 1)).toString
    )
}
