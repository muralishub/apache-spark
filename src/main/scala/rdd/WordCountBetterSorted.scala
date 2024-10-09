package com.murali.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object WordCountBetterSorted {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "WordCount")

    val input = sc.textFile("data/book.txt")

    // we are using regex here to filter only by meaningful words like it will eliminate commas and periods
    val words: RDD[String] = input.flatMap(x => x.split("\\W+"))

    val lowerCaseWords = words.map(_.toLowerCase)

    //count occurrances of each word
    val wordCounts: RDD[(String, Int)] = lowerCaseWords.map(x => (x, 1)).reduceByKey((x, y) => x + y)

    val sorted = wordCounts.map(x => (x._2, x._1)).sortByKey()


    for(result <- sorted) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }






  }



}
