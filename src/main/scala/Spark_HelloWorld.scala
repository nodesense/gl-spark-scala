// Spark_HelloWorld.scala
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark._
import org.apache.spark.rdd.RDD

// Spark driver program
// application
object Spark_HelloWorld extends App {
  println("Hello Spark")

  val conf = new SparkConf()
    .setAppName("Spark Hello World")
    .setMaster("local") // single thread

  val sc = new SparkContext(conf)

  val data = Array(1, 2, 3, 4, 5)

  // parent rdd
  val distDataRDD = sc.parallelize(data)

  // transform
  // child rdd, child of distDataRDD
  val mulBy2RDD = distDataRDD.map ( n => {
    println("Map ", n)
    n * 2
  })


  // When to cache?
  // 1. Reuse the previously computed result in multiple
  //      child RDD
  // 2. Purpose is to save CPU

  mulBy2RDD.cache()


  val filterBy2Rdd = mulBy2RDD.filter(n => {
    println("Filter ", n)
    n % 4 == 0
  })



  // action methods, execute the rdd
  // return is returned to executor/application
  val result = filterBy2Rdd.collect();
  println("results ", result.mkString(","));

  val result2 = filterBy2Rdd.collect();
  println("results2 ", result2.mkString(","));








}
