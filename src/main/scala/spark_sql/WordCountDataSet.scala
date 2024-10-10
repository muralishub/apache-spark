package com.murali.spark
package spark_sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{explode, lower}

object WordCountDataSet {


  case class Book(value: String)


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark = SparkSession.builder().appName("WordCount").master("local[*]").getOrCreate()

    // we  have to name the value of book object to be a value may its common for unstructured data
    import spark.implicits._
    val input = spark.read.text("data/book.txt").as[Book]

  //explode is like flatMap
    val words = input.select(explode(functions.split($"value", "\\W+")).alias("word")).filter($"word" =!= "")


    val lowerCaseWords = words.select(lower($"word").alias("word"))


    val wordCount  = lowerCaseWords.groupBy("word").count()

    val wordCountSorted = wordCount.sort("count")

    //show only shows first 20, to show all rows
    wordCountSorted.show(wordCountSorted.count.toInt)


    //Another way to do it(blending RDD's and dataset)
    val bookRDD = spark.sparkContext.textFile("data/book.txt")
    val wordsRDD = bookRDD.flatMap(x => x.split("\\W+"))
    val wordsDS = wordsRDD.toDS()
    val lowerCaseWordsDS = wordsDS.select(lower($"word").alias("word"))
    val wordCountDS = lowerCaseWordsDS.groupBy("word").count()
    val workdCountDsSoted = wordCountDS.sort("count")
    workdCountDsSoted.show(wordCountSorted.count.toInt)




  }



}
