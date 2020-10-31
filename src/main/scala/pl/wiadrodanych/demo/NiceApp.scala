package pl.wiadrodanych.demo

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import pl.wiadrodanych.demo.base.SparkJob
import pl.wiadrodanych.demo.base.SparkJob.spark.implicits._

object NiceApp {
  val spark = SparkJob.spark

  def main(args: Array[String]) = {
    val groceries: DataFrame = getGroceries
    val fruits: Dataset[Row] = filterFruits(groceries)
    val normalizedFruits: DataFrame = addNormalizedNameColumn(fruits)
    val sumOfFruits: DataFrame = sumByNormalizedName(normalizedFruits)
    sumOfFruits.show()
  }

  private def sumByNormalizedName(normalizedFruits: DataFrame) = {
    val sumOfFruits = normalizedFruits
      .groupBy("normalized_name")
      .agg(
        sum(($"quantity")).as("sum")
      )
    sumOfFruits
  }

  private def addNormalizedNameColumn(fruits: Dataset[Row]) = {
    val normalizedFruits = fruits.withColumn("normalized_name", lower($"name"))
    normalizedFruits
  }

  private def filterFruits(groceries: DataFrame) = {
    val fruits = groceries.filter($"type" === "fruit")
    fruits
  }

  private def getGroceries: DataFrame = {
    val groceries = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("some-data.csv")
    groceries
  }


}
