package com.gl

import com.gl.{FileUtils}
import org.apache.spark.{SparkContext}
import org.apache.spark.sql.expressions.scalalang.typed.{count => typedCount, sum => typedSum}
import org.apache.spark.sql.{Dataset, SparkSession, RecovererDataFrame}
import org.apache.spark.sql.functions._


import org.apache.spark.Recoverer

import scala.io.StdIn

object DS12_ProcessBirthDataAdvanced {


  def createSparkSession() = {
    val spark = SparkSession
      .builder
      .master("local[*]")
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

    //birthdaysDS.printSchema()
    //birthdaysDS.show(100)
    birthdaysDS
  }

  var schema:Dataset[Any] = _;

  def aggregateState (spark: SparkSession, birthdaysDS:Dataset[Birthdays]) = {
    import spark.sqlContext.implicits._

    val byStateNameDS = birthdaysDS
      // .repartition('state)
      //.select("id","state","year","month","day","date","wday","births")
      .select( "state","births")
      .groupBy($"state")
      .agg(sum("births"))

    val byStateCheckPointed = byStateNameDS.checkpoint();
    println(byStateCheckPointed.queryExecution.toRdd.toDebugString)

    // Save the schema as it is going to use to reconstruct nums dataset from a RDD
    val schema = birthdaysDS.schema

    import org.apache.spark.sql.execution.LogicalRDD
    val logicalRDD = byStateCheckPointed.queryExecution.optimizedPlan.asInstanceOf[LogicalRDD]
    val checkpointFiles = logicalRDD.rdd.getCheckpointFile.get

    println("Checkpoint files ", checkpointFiles)
    checkpointFiles
  }

  def recoverRdd(spark: SparkSession, checkPointFiles: String) = {

    import org.apache.spark.sql.catalyst.InternalRow

    val recoveredRdd = Recoverer.recover[InternalRow](spark.sparkContext, checkPointFiles)
    println("recoverred ", recoveredRdd);

    recoveredRdd
  }

    def processBirthday3(spark: SparkSession, birthdaysDS:Dataset[Birthdays]) = {
    import spark.sqlContext.implicits._

    //https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-checkpointing.html

    // Task: Count number of births by state, total number of records
    val byStateNameDS = birthdaysDS
      // .repartition('state)
      //.select("id","state","year","month","day","date","wday","births")
      .select( "state","births")
      .groupBy($"state")
      .agg(sum("births"))

    // byStateNameDS.cache()

    val numsCheckpointed = birthdaysDS.checkpoint();
    println(numsCheckpointed.queryExecution.toRdd.toDebugString)


    // Save the schema as it is going to use to reconstruct nums dataset from a RDD
    val schema = birthdaysDS.schema

    import org.apache.spark.sql.execution.LogicalRDD
    val logicalRDD = numsCheckpointed.queryExecution.optimizedPlan.asInstanceOf[LogicalRDD]
    val checkpointFiles = logicalRDD.rdd.getCheckpointFile.get

    println("Checkpoint files ", checkpointFiles)

    import org.apache.spark.sql.catalyst.InternalRow

    val numsRddRecovered = Recoverer.recover[InternalRow](spark.sparkContext, checkpointFiles)
    println("recoverred ", numsRddRecovered);

    val numsRecovered = RecovererDataFrame.createDataFrame(spark, numsRddRecovered, schema)
    numsRecovered.show
    //package org.apache.spark



    birthdaysDS

      .repartition(20,'state)
      .rdd
      .mapPartitionsWithIndex((idx, it) => Iterator((idx, it.toList)))
      .toDF("partition_id", "block_ids").show(true)



    byStateNameDS.explain(true)
    println("Total Partitions ", byStateNameDS.rdd.getNumPartitions)
    //
    //    val results = byStateNameDS.rdd.glom().collect()
    //    println("Total partition result Size ", results.size);
    //    for (r <- results) {
    //      println("Partition's Data ", r.mkString(","))
    //    }

    byStateNameDS.show(20)




  }

  def main(args: Array[String]) {

    val spark = createSparkSession()
    spark.sparkContext.setLogLevel("WARN")

    spark.sparkContext.setCheckpointDir("/tmp/checkpoints")
    val checkpointDir = spark.sparkContext.getCheckpointDir.get
    println("Check point dir", checkpointDir)


    val start = System.currentTimeMillis();
    println("Start ", start);
    ///data/birthdays.csv

    //FileUtils.getInputPath("birthdays.csv")


    val birthDaysDS = readBirthDays(spark, FileUtils.getInputPath("birthdays.csv"))

    //processBirthday(spark, birthDaysDS)

    //processBirthday2(spark, birthDaysDS)


    processBirthday3(spark, birthDaysDS)

    val end = System.currentTimeMillis();
    println("End ", end);

    println("Diff ", end - start);


    StdIn.readLine()

  }

}
