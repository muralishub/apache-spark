package com.murali.spark
package spark_sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

object SparkSQLDataSet {


  case class Person(id: Int, name: String, age: Int, friends: Int)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkSession: SparkSession = SparkSession.builder().appName("SparkSQL").master("local[*]").getOrCreate()

   import sparkSession.implicits._

    // as[Person] is used to convert dataframe to dataset, we get better type checking at compile time and better performance
    val schemaPeople = sparkSession.read.option("header", "true").option("inferSchema", "true").csv("data/fakefriends.csv").as[Person]
// when we use as[Person] it becomes a dataset instead of dataframe which does better type checking at compile time and better performance


schemaPeople.printSchema()

    //this basically says create database table called people.
schemaPeople.createOrReplaceTempView("people")

    val teenagers = sparkSession.sql("SELECT *  FROM people WHERE age >= 13 AND age <= 19")

    val results = teenagers.collect()

results.foreach(println)

sparkSession.close()

  }

}
