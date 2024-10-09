package com.murali.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.annotation.unused

object WordCount {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "WordCount")

    val input = sc.textFile("data/book.txt")


    val words: RDD[String] = input.flatMap(x => x.split(" "))

    // countByValue returns how many times each word occurs
    val wordCount: collection.Map[String, Long] = words.countByValue()

    wordCount.foreach(println)




  }


}
