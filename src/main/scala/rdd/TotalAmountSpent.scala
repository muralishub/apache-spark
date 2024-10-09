package com.murali.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object TotalAmountSpent {

  def main(args: Array[String]): Unit = {

    def parse(line: String) = {

      val s = line.split(",")
      val customerId = s(0).toInt
      val amount = s(2).toFloat
      (customerId, amount)
    }

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "TotalAmountSpent")

    val lines = sc.textFile("data/customer-orders.csv")

    val transactions = lines.map(parse)

 //   transactions.foreach(println)

    val result = transactions.reduceByKey((x, y) => (x + y))


    val flipped = result.map(x => (x._2, x._1))

    val sorted = flipped.sortByKey().collect()

    sorted.foreach(println)







  }


}
