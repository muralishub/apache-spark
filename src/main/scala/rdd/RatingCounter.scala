package com.murali.spark

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession





// we have a dateset and find how many each rating exists in that set

object RatingCounter {

  def main(args: Array[String]): Unit = {
//
//    Logger.getLogger("org.apache").setLevel(Level.ERROR)
//    Logger.getLogger("org.sparkproject").setLevel(Level.ERROR)
//
//    // create a spark context using every core of the local machine, named RatingCounter
//    val sc = new SparkContext("local[*]", "RatingCounter")
//
//
//
//    //load each line of the ratings data into an RDD
//    val lines: RDD[String] = sc.textFile("data/ml-100k/u.data")
//
//    // convert each line to a string, split it out by tabs, and extract the third field
//    val ratings: RDD[String] = lines.map(x => x.split("\t")(2))
//
//    // count up how many times each value (rating) occurs
//    val results = ratings.countByValue()
//
//    val sortedResults = results.toSeq.sortBy(_._1)
//
//    sortedResults.foreach(println)


    Logger.getLogger("org").setLevel(Level.ERROR)
    // create spark session instead of spark context
    val spark = SparkSession.builder().appName("RatingCounter").master("local[*]").getOrCreate()

    val sc: SparkContext = spark.sparkContext

    val lines: RDD[String] = sc.textFile("data/ml-100k/u.data")
    val ratings: RDD[String] = lines.map(x => x.split("\t")(2))
    val results = ratings.countByValue()
    val sortedResults = results.toSeq.sortBy(_._1)
    sortedResults.foreach(println)

    // stop SparkSession to release resources
    spark.stop()
  }
}
