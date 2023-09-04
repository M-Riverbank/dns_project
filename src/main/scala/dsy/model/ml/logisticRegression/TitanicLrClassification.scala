package dsy.model.ml.logisticRegression

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
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
      .option("inferSchema", "true")
      .csv("datas/titanic/train.csv")
    /*
    root
               |-- PassengerId: integer (nullable = true)
               |-- Survived: integer (nullable = true)
               |-- Pclass: integer (nullable = true)
               |-- Name: string (nullable = true)
               |-- Sex: string (nullable = true)
               |-- Age: double (nullable = true)
               |-- SibSp: integer (nullable = true)
               |-- Parch: integer (nullable = true)
               |-- Ticket: string (nullable = true)
               |-- Fare: double (nullable = true)
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
    // male -> [1.0, 0.0]    female -> [0.0, 1.0]7 独热编码
    val encoder: OneHotEncoder = new OneHotEncoder()
      .setInputCol("sexIndex")
      .setOutputCol("sexVector")
      .setDropLast(false)
    val sexTitanicDF: DataFrame = encoder.transform(indexerTitanicDF)


//    // 2.3 将特征值组合, 使用VectorAssembler
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(
        Array("Pclass", "sexVector", "SibSp", "Parch", "Fare", "defaultAge")
      )
      .setOutputCol("features")
    val titanicDF: DataFrame = assembler.transform(sexTitanicDF)

//    // 2.4 划分数据集为训练集和测试集
    val Array(trainingDF, testingDF) = titanicDF.randomSplit(Array(0.8, 0.2))
    trainingDF.cache().count()

//    // TODO: 3. 使用算法和数据构建模型：算法参数
    val logisticRegression: LogisticRegression = new LogisticRegression() // 逻辑回归
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setPredictionCol("prediction") // 使用模型预测时，预测值的列名称
      // 二分类
      .setFamily("binomial")
      .setStandardization(true)
      // 超参数
      .setMaxIter(100) //最大迭代
      .setRegParam(0.1)
      .setElasticNetParam(0.8)
    val lrModel: LogisticRegressionModel = logisticRegression.fit(trainingDF)
    // y = θ0 + θ1x1+ θ2x2+ θ3x4+ θ4x4+ θ5x6+ θ6x6
    println(s"coefficients: ${lrModel.coefficientMatrix}") // 斜率, θ1 ~ θ6
    println(s"intercepts: ${lrModel.interceptVector}") // 截距, θ0

//    // TODO: 4. 模型评估,使用测试数据集结果与模型预测结果比对测试评估模型
    val predictionDF: DataFrame = lrModel.transform(testingDF)
    predictionDF.printSchema()
    predictionDF
      .select("label", "prediction", "probability", "features")
      .show(40, truncate = false)
    // 分类中的ACCU、Precision、Recall、F-measure、Accuracy
    val accuracy = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      // 四个指标名称："f1", "weightedPrecision", "weightedRecall", "accuracy"
      .setMetricName("accuracy")
      .evaluate(predictionDF)
    println(s"accuracy(精确性) = $accuracy")

    spark.stop()
  }
}
