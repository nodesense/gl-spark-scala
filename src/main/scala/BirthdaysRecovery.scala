package com.gl

import com.gl.FileUtils
import org.apache.spark.sql.{Dataset, Encoders, RecovererDataFrame, SparkSession}
import java.sql.Timestamp
import java.sql.Timestamp

import scala.io.StdIn
import org.apache.spark.{Recoverer, SparkConf, SparkContext}
import org.apache.spark.sql.expressions.scalalang.typed.{avg => typedAvg, count => typedCount, sum => typedSum}
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types.StructType


object ProcessBirthDataRecovery {


  def createSparkSession() = {
    val spark = SparkSession
      .builder
      .master("local[4]")
      .appName("DS01Basics")
      .config("spark.sql.crossJoin.enabled", "true")

      .getOrCreate()


    import spark.sqlContext.implicits._

    spark
  }

  var schema: StructType = _;

  // loading data from data source
  def state1(spark: SparkSession, filePath:String) = {
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


    val birthdaysDSCheckPointed = birthdaysDS.checkpoint();

    println("birthdaysDSCheckPointed broken lineage")

    birthdaysDSCheckPointed.show()
    //println(birthdaysDSCheckPointed.queryExecution.toRdd.toDebugString)

    // Save the schema as it is going to use to reconstruct nums dataset from a RDD
    //val schema = birthdaysDS.schema

    this.schema = birthdaysDS.schema;

    import org.apache.spark.sql.execution.LogicalRDD
    val logicalRDD = birthdaysDSCheckPointed.queryExecution.optimizedPlan.asInstanceOf[LogicalRDD]
    val checkpointFiles = logicalRDD.rdd.getCheckpointFile.get

    println("Checkpoint files ", checkpointFiles)



    checkpointFiles
  }

  def state2(spark: SparkSession, checkpointFiles: String) = {

    import org.apache.spark.sql.catalyst.InternalRow

    val birthdayRddRecovered = Recoverer.recover[InternalRow](spark.sparkContext, checkpointFiles)
    println("recoverred ", birthdayRddRecovered);

    birthdayRddRecovered.foreach( println(_));


    val birthDataDF = RecovererDataFrame.createDataFrame(spark, birthdayRddRecovered, schema)

    println("Recovered data data");
    birthDataDF.show


//
//    val numsRecovered = RecovererDataFrame.createDataFrame(spark, numsRddRecovered, schema)
//    numsRecovered.show
  }

  def processBirthday(spark: SparkSession, birthdaysDS:Dataset[Birthdays]) = {
    import spark.sqlContext.implicits._



  }


  def main(args: Array[String]) {

    val spark = createSparkSession()
    spark.sparkContext.setLogLevel("WARN")
    import spark.sqlContext.implicits._


    spark.sparkContext.setCheckpointDir("/tmp/app1/checkpoints")

    val checkPointFiles = state1(spark, FileUtils.getInputPath("birthdays.csv"))
    state2(spark, checkPointFiles)

  }

}
