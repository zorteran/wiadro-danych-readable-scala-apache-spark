package pl.wiadrodanych.demo

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object UglyApp {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("MyAwesomeApp")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val groceries = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("some-data.csv")

    val fruits = groceries.filter($"type" === "fruit")

    val normalizedFruits = fruits.withColumn("normalized_name", lower($"name"))

    val sumOfFruits = normalizedFruits
      .groupBy("normalized_name")
      .agg(
        sum(($"quantity")).as("sum")
      )

    sumOfFruits.show()
  }
}
