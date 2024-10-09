package com.murali.spark

import org.apache.hadoop.shaded.org.checkerframework.checker.units.qual.g
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object FriendsByAge {

  def main(args: Array[String]): Unit = {

// average friends by age
    def parseLine(line: String): (Int, Int) = {
      val fields = line.split(",")
      val age = fields(2).toInt
      val numFriends = fields(3).toInt
      (age, numFriends)
    }




    Logger.getLogger("org").setLevel(Level.ERROR)
    // create spark session instead of spark context
    val spark = SparkSession.builder().appName("FriendsByAge").master("local[*]").getOrCreate()

    val sc: SparkContext = spark.sparkContext

    val lines: RDD[String] = sc.textFile("data/fakefriends-noheader.csv")

    val rdd = lines.map(parseLine)
   // this will hold age and  friends for each age and its not unique

    // from below using mapValues we map each value to (value, 1) result will be (key, (value, 1)
    // this will allow use to count value and total count for that age to get the average
    //when we have (key, (value, 1)) we apply reduceByKey which Merge the values for each key using an associative and commutative reduce function
    //here what we are doing is x and y values are (value, 1), so we add values and 1's with ones we get count and when values with total
    // result will be (key, (valuesTotal, countByKey)) example (34,(1473,6))



    val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, (x._2 + y._2)))

    val averageByAge: RDD[(Int, Int)] = totalsByAge.mapValues(x => x._1/ x._2)

    // collect will return all elements in an Array, cousion collect will load all elements in memory its ok for small numbers
    val results = averageByAge.collect()

    results.sorted.foreach(println)





  }



}
