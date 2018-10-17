package com.gl

import com.gl.FileUtils
import org.apache.spark.sql.{Dataset, SparkSession}
import java.sql.Timestamp
import java.sql.Timestamp

import scala.io.StdIn
import org.apache.spark.sql.Encoders
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.expressions.scalalang.typed.{avg => typedAvg, count => typedCount, sum => typedSum}
import org.apache.spark.sql.functions.sum


case class Birthdays(id: Int,
                     state:String,
                     year: Int,
                     month: Int,
                     day :Int,
                     date: Timestamp,
                     wday: String,
                     var births: Int)

object ProcessBirthData {


  def createSparkSession() = {
    val spark = SparkSession
      .builder
      .master("local[4]")
      .appName("DS01Basics")
      .config("spark.sql.crossJoin.enabled", "true")

      .getOrCreate()

    spark
  }


  def readBirthDays(spark: SparkSession, filePath:String) = {

    import spark.sqlContext.implicits._

    val birthdaysDS = spark.read
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv(filePath)
      .as[Birthdays]

    birthdaysDS.printSchema()
    birthdaysDS.show(100)
    birthdaysDS
  }

  def processBirthday(spark: SparkSession, birthdaysDS:Dataset[Birthdays]) = {
    import spark.sqlContext.implicits._

    birthdaysDS.filter(_.births > 100).show()
    println("Birthdays partitions ", birthdaysDS.rdd.getNumPartitions)

    val filterByState = birthdaysDS.filter(bd => bd.state == "OH");
//
//    filterByState.map (bd => {
//      if (bd.births < 450) { // fixing dirty data
//        bd.births = bd.births + 2
//      }
//      bd
//    });


    println("filterByState partitions ", filterByState.rdd.getNumPartitions)

    val filterByGT500 = filterByState.filter(bd => bd.births > 500)

    filterByGT500.show()
    println("filterByGT500 partitions ", filterByGT500.rdd.getNumPartitions)

    val birthdaysDSByState = birthdaysDS.select( "state","births")
       .repartition(50,'state)
      .groupBy($"state")
      .agg(sum("births"));



    birthdaysDSByState.rdd.cache() // MEMORY

    birthdaysDSByState.rdd.persist() // MEMORY

    println("birthdaysDSByState partitions ", birthdaysDSByState.rdd.getNumPartitions)

    birthdaysDSByState.printSchema()
    birthdaysDSByState.show()

    val results = birthdaysDSByState.rdd.glom().collect()
    println("Total partition result Size ", results.size);
    for (r <- results) {
      println("Partition's Data ", r.mkString(","))
    }


  }


  def main(args: Array[String]) {

    val spark = createSparkSession()
    spark.sparkContext.setLogLevel("WARN")
    import spark.sqlContext.implicits._


    ///data/birthdays.csv

    //FileUtils.getInputPath("birthdays.csv")


    val birthDaysDS = readBirthDays(spark, FileUtils.getInputPath("birthdays.csv"))

    processBirthday(spark, birthDaysDS)


  }

}
