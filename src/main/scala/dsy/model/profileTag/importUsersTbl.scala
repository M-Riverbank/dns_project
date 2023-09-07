package dsy.model.profileTag

import dsy.model.AbstractModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class importUsersTbl extends AbstractModel("导入users数据至hbase") {
  /**
   * 抽象方法，对数据的具体处理,由实现类完善
   *
   * @param businessDF 业务数据
   * @param mysqlDF    后端数据
   * @return 处理后的数据
   */
  override def handle(businessDF: DataFrame, mysqlDF: DataFrame): DataFrame = {
    // 增加edu字段，数值随机1-7
    val resultDF = businessDF.withColumn("edu", lit((rand() * 7 + 1).cast("Int")).cast("String"))
    resultDF
  }
}

object importUsersTbl {
  def main(args: Array[String]): Unit = {
    new importUsersTbl().execute(4)
  }
}
