package com.murali.spark
package spark_sql

import com.murali.spark.spark_sql.PopularMoviesDataSet.Movie
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

import scala.io.{Codec, Source}

object PopularMoviesNicerDataSet {


  case class Movie(userID: Int, movieID: Int, rating: Int, timestamp: Long)


  def loadMoviews(): Map[Int, String] = {
    implicit val codec: Codec = Codec("ISO-8859-1")

    var movieNames: Map[Int, String] = Map()

    val bufferedSource = Source.fromFile("data/ml-100k/u.item")

    for (line <- bufferedSource.getLines()) {

      val fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    bufferedSource.close()
    movieNames
  }



  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().appName("PopularMoviesNicerDataSet").master("local[*]").getOrCreate()

    val nameDict = spark.sparkContext.broadcast(loadMoviews())

    val moviesSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    import spark.implicits._

    val movies = spark.read.option("sep", "\t").schema(moviesSchema).csv("data/ml-100k/u.data").as[Movie]

    val movieCounts = movies.groupBy("movieID").count()

    val lookupName: Int => String = (movieID: Int) => nameDict.value(movieID)

    // wrapit up with udf user defined function
    val lookupNameUDF = udf(lookupName)

    val moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(col("movieID")))

    val sortedMoviesWithNames = moviesWithNames.sort("count")

    sortedMoviesWithNames.show(sortedMoviesWithNames.count().toInt, truncate = false)







  }

}
