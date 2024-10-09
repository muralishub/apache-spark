package com.murali.spark
package spark_sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DataFramesDataSet {

  case class Person(id: Int, name: String, age: Int, friends: Int)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkSession: SparkSession = SparkSession.builder().appName("SparkSQL").master("local[*]").getOrCreate()

    import sparkSession.implicits._

    val people = sparkSession.read.option("header", "true").option("inferSchema", "true").csv("data/fakefriends.csv").as[Person]
    // when we use as[Person] it becomes a dataset instead of dataframe which does better type checking at compile time and better performance

    people.printSchema()

    println("select name column")
    people.select("name").show()

    println("filter out anyone over 21:")
    people.filter(people("age") < 21).show()

    println("Group by age:")
    people.groupBy("age").count().show()

   println("Make everyone 10 years older:")
    people.select(people("name"), people("age") + 10).show()



    sparkSession.stop()






  }



}
