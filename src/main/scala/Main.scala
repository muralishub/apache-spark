package com.murali.spark

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object Main {
  def main(args: Array[String]): Unit = {

  val spark =  SparkSession.builder()
      .appName("spark-video-course") // name of the application
      .master("local[*]") // * means use all the cores available on this machine
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate() // get or create the spark session


   val df =  spark.read
     .option("header", value = true)
      .option("inferSchema", value = true) // infer the schema of the data instead of strings for all columns type will be inferred
     .csv("data/AAPL.csv") //load data frame from csv file


    df.show() // show the data frame in 20 rows by default

    df.printSchema()

    // get columns of the data frame
    df.select("Date", "Open", "Close").show()
    // another way to get columns of the data frame
    val column = df("Date")
    col("Date")

    // or simply by calling $


    import spark.implicits._
    $"Date"

    df.select(col("Date"), $"Open").show()


    val column2 = df("Open")

    column2.plus(2.0)
    val newColumn = column2 + 2.0

    val columnString = column2.cast(StringType).as("OpenAsString")



   val litColumn = lit(2.0)

   val newColumString = functions.concat(columnString, lit("Hello World"))
   df.select(column2, newColumn, columnString, newColumString).filter(newColumn > 2.0).filter(newColumn > column2).filter(newColumn === column2).show() // 2 equal sign means we are comparing scala objects 3 equals means we are comparing 2 columns




  }
}