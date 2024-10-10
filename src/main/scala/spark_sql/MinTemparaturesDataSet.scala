package com.murali.spark
package spark_sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.round
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}

object MinTemparaturesDataSet {

  case class Temparature(stationID: String, date: Int, measure_type: String, temperature: Float)


  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark = SparkSession.builder().appName("MinTemparatureDataSet").master("local[*]").getOrCreate()

    val temperatureSchema = new StructType()
      .add("stationID", StringType, nullable = true)
      .add("date", IntegerType, nullable = true)
      .add("measure_type", StringType, nullable = true)
      .add("temperature", FloatType, nullable = true)

    import spark.implicits._

    val ds = spark.read
      .schema(temperatureSchema).csv("data/1800.csv").as[Temparature]
// filter TMIN
    val minTemps = ds.filter($"measure_type" === "TMIN")
//select only station and Temp

    val stationTemps = minTemps.select("stationID", "temperature")
   // stationTemps.show()

   val minTempsByStation =  stationTemps.groupBy("stationID").min("temperature")
// to convert colum to fahrenheit and sort the dataset
  val minTemparaturesByStationF = minTempsByStation.withColumn("temperature", round($"min(temperature)" * 0.1f * (9.0f / 5.0f) + 32.0f, 2))
    .select("stationID", "temperature").sort("temperature")

 //collect format and print

    val results = minTemparaturesByStationF.collect()


    for(result <- results) {
      val station = result(0)
      val temp = result(1).asInstanceOf[Float]
      val formattedTemp = f"$temp%.2f F"
      println(s"$station minimum temperature: $formattedTemp")
    }






  }



}
