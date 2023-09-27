package dsy.model

object stringTest {
  def main(args: Array[String]): Unit = {
    //    val result = "1=2=3=4".trim.split("=")
    //    val key = result(0)
    //    var value = result(1)
    //    for (i <- 2 until result.length) {
    //      value = s"$value=${result(i)}"
    //    }
    val result = "1=2=3=4".trim.split("=")
    val key = result(0)
    val value = result.drop(1).mkString("=")
    println(key)
    println(value)
  }
}
