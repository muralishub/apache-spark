package com.murali.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.math.min

object MinTemperatures {

  def main(args: Array[String]): Unit = {


    def parse(line: String) = {
     val fields = line.split(",")
     val stationId = fields(0)
     val entryType = fields(2)
     val temp = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
      (stationId, entryType, temp)

    }






    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MinTemperatures")

    val lines  = sc.textFile("data/1800.csv")

    // convert to stationId, entryType and temperature

    val parsedLines = lines.map(parse)

    // only get TMin

    val filtered = parsedLines.filter(_._2 == "TMIN")

    // stationId and temp we are interested in

    val stationTemps: RDD[(String, Float)] = filtered.map(x => (x._1, x._3))

    //reduce by stationId retaining the minimum temperature found
    val minTempsByStation = stationTemps.reduceByKey((x, y) => min(x, y))


    val results = minTempsByStation.collect()


    for(result <- results) {
      val station = result._1
      val temp = result._2
      val formattedTemp = f"$temp%.2f F"
      println(s"$station minimum temparature: $formattedTemp")



    }



  }

}
