package dsy.model

object money {
  def main(args: Array[String]): Unit = {
    val data = List(
      7.0,
      58.0,
      289.0,
      299.0,
      499.0,
      899.0,
      1099.42,
      1299.0,
      1558.0,
      1649.0,
      1699.0,
      1899.0,
      1899.0,
      1999.0,
      1999.0,
      2449.0,
      2479.45,
      2488.0,
      3149.0,
      3449.0
    )

    val sortedData = data.sorted

    // 输出排序后的结果
    for (num <- sortedData) {
      println(num)
    }
  }
}
