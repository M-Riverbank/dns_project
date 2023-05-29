package dsy.model.ml.logisticRegression

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.{DataFrame, SparkSession}

object TitanicLrClassification {
  def main(args: Array[String]): Unit = {
    //获取执行对象
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //TODO: 1.加载数据，数据过滤与基本转换
    val TitanicDF: DataFrame = spark
      .read
      .option("header", "true")
      .option("seq", ",")
      .option("infertSchema", "true")
      .csv("datas/titanic/train.csv")
    /*
    TitanicDF.show(10)
    TitanicDF.printSchema
      +-----------+--------+------+--------------------+------+----+-----+-----+----------------+-------+-----+--------+
      |PassengerId|Survived|Pclass|                Name|   Sex| Age|SibSp|Parch|          Ticket|   Fare|Cabin|Embarked|
      +-----------+--------+------+--------------------+------+----+-----+-----+----------------+-------+-----+--------+
      |          1|       0|     3|Braund, Mr. Owen ...|  male|  22|    1|    0|       A/5 21171|   7.25| null|       S|
      |          2|       1|     1|Cumings, Mrs. Joh...|female|  38|    1|    0|        PC 17599|71.2833|  C85|       C|
      |          3|       1|     3|Heikkinen, Miss. ...|female|  26|    0|    0|STON/O2. 3101282|  7.925| null|       S|
      |          4|       1|     1|Futrelle, Mrs. Ja...|female|  35|    1|    0|          113803|   53.1| C123|       S|
      |          5|       0|     3|Allen, Mr. Willia...|  male|  35|    0|    0|          373450|   8.05| null|       S|
      |          6|       0|     3|    Moran, Mr. James|  male|null|    0|    0|          330877| 8.4583| null|       Q|
      |          7|       0|     1|McCarthy, Mr. Tim...|  male|  54|    0|    0|           17463|51.8625|  E46|       S|
      |          8|       0|     3|Palsson, Master. ...|  male|   2|    3|    1|          349909| 21.075| null|       S|
      |          9|       1|     3|Johnson, Mrs. Osc...|female|  27|    0|    2|          347742|11.1333| null|       S|
      |         10|       1|     2|Nasser, Mrs. Nich...|female|  14|    1|    0|          237736|30.0708| null|       C|
      +-----------+--------+------+--------------------+------+----+-----+-----+----------------+-------+-----+--------+
      only showing top 10 rows

    root
     |-- PassengerId: string (nullable = true)
     |-- Survived: string (nullable = true)
     |-- Pclass: string (nullable = true)
     |-- Name: string (nullable = true)
     |-- Sex: string (nullable = true)
     |-- Age: string (nullable = true)
     |-- SibSp: string (nullable = true)
     |-- Parch: string (nullable = true)
     |-- Ticket: string (nullable = true)
     |-- Fare: string (nullable = true)
     |-- Cabin: string (nullable = true)
     |-- Embarked: string (nullable = true)

     Passengerld 整型变量，标识乘客的ID,递增变量，对预测无帮助
      Survived 整型变量，标识该乘客是否幸存。0表示遇难，1表示幸存。将其转换为factor变量比较方便处理
      Pclass 整型变量，标识乘客的社会经济状态，1代表Upper, 2代表Middle, 3代表Lower
      Name 字符型变量，除包含姓和名以外，还包含Mr.Mrs.Dr.这样的具有西方文化特点的信息
      Sex 字符型变量,标识乘客性别，适合转换为factor类型变量
      Age 整型变量，标识乘客年龄，有缺失值
      SibSp 整型变量，代表兄弟姐妹及配偶的个数。其中Sib代表Sibling也即兄弟姐妹，Sp代表Spouse 也即配偶
      Parch 整型变量,代表父母或子女的个数。其中Par代表Parent也即父母， Ch代表Child也即子女
      Ticket 字符型变量,代表乘客的船票号Fare数值型,代表乘客的船票价
      Cabin 字符型，代表乘客所在的舱位,有缺失值
      Embarked 字符型，代表乘客登船口岸，适合转换为factor型变量
     */
    //TODO: 2.数据准备:特征工程（提取·转换与选择)
    //2.1 Age年龄字段有缺省值，填充为年龄字段平均值
    val avgAge: Double = TitanicDF  //提取平均年龄
      .select("Age")
      .filter($"Age".isNotNull)
      .select(round(avg($"Age"), 2).as("avgAge"))
      .first()
      .getAs[Double]("avgAge")
    //提取特征features列并替换Age的空值
    val ageTitanicDF: DataFrame = TitanicDF
      .select(
        //标签label
        $"Survived".as("label"),
        //特征features
        $"Pclass",
        $"Sex",
        $"SibSp",
        $"Parch",
        $"Fare",
        $"Age",
        //当年龄为null时使用 avgAge 代替,否则为$"Age"
        when($"Age".isNotNull, $"Age").otherwise(avgAge).as("defaultAge")
      )
    // 2.2 对Sex字段类别特征换换，使用StringIndexer和OneHotEncoder
    // male ->0  ,female -> 1
    val indexer: StringIndexer = new StringIndexer()
      .setInputCol("Sex")
      .setOutputCol("sexIndex")
    val indexerTitanicDF: DataFrame = indexer
      .fit(ageTitanicDF)
      .transform(ageTitanicDF)
    // male -> [1.0, 0.0]    female -> [0.0, 1.0]7
    indexerTitanicDF.show
    indexerTitanicDF. printSchema


    spark.stop
  }
}
