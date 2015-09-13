package basics

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SparkWordCount {

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("Scala Spark example"))

    val inputRDD =
      sc.textFile("hdfs://localhost/user/clojspark/basics/inputdata/wordcount-input.txt")

    val wordsRDD = inputRDD.flatMap(x => x.split(" "))
    val countRDD = wordsRDD.map(x => (x, 1))
      .reduceByKey((x, y) => x + y)


    println(countRDD.collect().mkString(", "))

  }

}
