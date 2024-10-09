package com.murali.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object WordCountBetter {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "WordCount")

    val input = sc.textFile("data/book.txt")

// we are using regex here to filter only by meaningful words like it will eliminate commas and periods
    val words: RDD[String] = input.flatMap(x => x.split("\\W+"))

    val lowerCaseWords = words.map(_.toLowerCase)

    val count = lowerCaseWords.countByValue()

    count.foreach(println)




  }



}
