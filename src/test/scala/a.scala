import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object a {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    val input = args(0)
    sc.setLogLevel("WARN")
    val data = sc.textFile(input)


    val split: RDD[(String, Int)] =
      data
        .map { x =>
          x.split(",")(4)
        }
        .map { s =>
          s.split(" ")
        }
        .map { y =>
          y(0).split("/")
        }
        .map { z =>
          (z(0) + "-" + z(1), 1)
        }
        .reduceByKey(_ + _)


    split.collect().foreach(println)
    sc.stop()
  }
}
