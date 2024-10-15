package com.murali.spark
package spark_sql


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, pi}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object MostPopularSuperheroDataSet {

  case class SuperHeroNames(id: Int, name: String)
  case class SuperHero(value: String)

  def main(args: Array[String]): Unit = {


  val logger = Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("MostPopularSuperheroDataSet").master("local[*]").getOrCreate()

  val superHeroNamesSchema = new StructType()
    .add("id", IntegerType, nullable = true)
    .add("name", StringType, nullable = true)

 import spark.implicits._

 val names = spark.read.schema(superHeroNamesSchema).option("sep", " ").csv("data/Marvel-names.txt").as[SuperHeroNames]

 val lines = spark.read.text("data/Marvel-graph.txt").as[SuperHero]



 val connections = lines.withColumn("id", functions.split(col("value"), " ")(0)).withColumn("connections", functions.size(functions.split(col("value"), " ")) -1).groupBy("id").agg(functions.sum("connections").alias("connections"))



 val mostPopular = connections.sort($"connections".desc).first()

 val mostPopularName = names.filter($"id" === mostPopular(0)).select("name").first()

    println(s"${mostPopularName(0)} is the most popular superhero with ${mostPopular(1)} co-appreances.")





  }



}
