package com.murali.spark
package spark_sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{avg, round}
import org.apache.spark.sql.{Dataset, SparkSession}


object FakeFriendsDataSet {


  case class Friend(id: Int,name: String, age: Int, friends: Long)


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val session = SparkSession.builder().appName("FakeFriendsDataSet").master("local[*]").getOrCreate()

    import session.implicits._


    val ds: Dataset[Friend] = session.read.option("header", "true").option("inferSchema", "true").csv("data/fakefriends.csv").as[Friend]

    val friendsByAge = ds.select("age", "friends")


    println("Group by age:")
    friendsByAge.groupBy("age").avg("friends").show()

    //sorted
    friendsByAge.groupBy("age").avg("friends").sort("age").show()

    //formated more nicely, we have to use agg aggregate function to use round
    friendsByAge.groupBy("age").agg(round(avg("friends"), 2).alias("friends_avg")).sort("age").show()






  }



}
