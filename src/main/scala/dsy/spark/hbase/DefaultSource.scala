package dsy.spark.hbase

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * 默认数据源提供 Relation 对象,分别为加载数据和保存提供 Relation 对象
 */
class DefaultSource extends RelationProvider
  with CreatableRelationProvider
  with DataSourceRegister {
  /**
   *  从数据源加载数据时,使用简称，不需要再写包名称
   *
   * @return 简称
   */
  override def shortName(): String = "hbase"


  private val SPLIT: String = ","
  private val HBASE_TABLE_SELECT_FIELDS: String = "selectFields"

  /**
   * 从数据源读加载读取数据时,创建 Relation 对象, 此Relation实现 BaseRelation 和 TableScan
   *
   * @param sqlContext spark执行对象
   * @param parameters 连接数据源参数,通过 option 设置
   * @return 读取操作的 Relation 对象
   */
  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]
                             ): BaseRelation = {
    //1. 自定义schema信息
    val userSchema: StructType = StructType(
      //遍历封装读取字段schema信息
      parameters(HBASE_TABLE_SELECT_FIELDS)
        .split(SPLIT)
        .map { field =>
          StructField(field, StringType, nullable = true)
        }
    )
    //2. 创建读取操作的 Relation 对象,传递参数
    new HbaseRelation(sqlContext, parameters, userSchema)
  }

  /**
   * 将数据集保存至数据源时,创建 Relation 对象, 此 Relation 对象实现 BaseRelation 和 InsertableRelation
   *
   * @param sqlContext spark执行对象
   * @param mode       保存模式
   * @param parameters 连接数据源参数,通过 option 设置
   * @param data       保存的数据集
   * @return 写入操作的 Relation 对象
   */
  override def createRelation(
                               sqlContext: SQLContext,
                               mode: SaveMode,
                               parameters: Map[String, String],
                               data: DataFrame
                             ): BaseRelation = {
    //1.创建 HBaseRelation 对象
    val relation: HbaseRelation = new HbaseRelation(sqlContext, parameters, data.schema)
    //2.保存数据
    relation.insert(data, overwrite = true)
    //3.返回 Relation 对象
    relation
  }

}
