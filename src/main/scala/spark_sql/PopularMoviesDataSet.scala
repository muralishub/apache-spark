package com.murali.spark
package spark_sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

object PopularMoviesDataSet {

case class Movie(movieID: Int)


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("PopularMoviesDataSet").master("local[*]").getOrCreate()

    val moviesSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    import spark.implicits._

    val ds = spark.read.option("sep", "\t").schema(moviesSchema).csv("data/ml-100k/u.data").as[Movie]

    val topMovieIds = ds.groupBy("movieID").count().orderBy(desc("count"))

    topMovieIds.show(10)


    spark.stop()

  }



}
