package com.murali.spark
package spark_sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.round
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StructType}

object TotalAmountSpentDataSet {

  case class CustomerOrders(cust_id: Int, item_id: Int, amount_spent: Double)


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("TotalAmountSpent").master("local[*]").getOrCreate()

    val customerOrderSchema = new StructType()
      .add("cust_id", IntegerType, nullable = true)
      .add("item_id", IntegerType, nullable = true)
      .add("amount_spent", DoubleType, nullable = true)


    import spark.implicits._

    val customerDS = spark.read.schema(customerOrderSchema).csv("data/customer-orders.csv").as[CustomerOrders]

    // I only want customer and amountSpent

    val totalByCustomer = customerDS.select("cust_id", "amount_spent")

    // groupby

    val eachSpent = totalByCustomer.groupBy("cust_id").agg(round(functions.sum("amount_spent"), 2).alias("total_spent"))

    val totalAmountSpentDataSetSorted = eachSpent.sort("total_spent")

totalAmountSpentDataSetSorted.show(totalByCustomer.count().toInt)

  }



}
