package pl.wiadrodanych.demo

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSessionOverdoseApp {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("MyAwesomeApp")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val groceries: DataFrame = getGroceries(spark)
    val fruits: Dataset[Row] = filterFruits(groceries, spark)
    val normalizedFruits: DataFrame = withNormalizedName(fruits, spark)
    val sumOfFruits: DataFrame = sumByNormalizedName(normalizedFruits, spark)

    sumOfFruits.show
  }

  private def sumByNormalizedName(normalizedFruits: DataFrame, spark: SparkSession) = {
    import spark.implicits._
    val sumOfFruits = normalizedFruits
      .groupBy("normalized_name")
      .agg(
        sum(($"quantity")).as("sum")
      )
    sumOfFruits
  }

  private def withNormalizedName(fruits: Dataset[Row], spark: SparkSession) = {
    import spark.implicits._
    val normalizedFruits = fruits.withColumn("normalized_name", lower($"name"))
    normalizedFruits
  }

  private def filterFruits(groceries: DataFrame, spark: SparkSession) = {
    import spark.implicits._
    val fruits = groceries.filter($"type" === "fruit")
    fruits
  }

  private def getGroceries(spark: SparkSession): DataFrame  = {
    val groceries = spark.read
      .option("inferSchema","true")
      .option("header","true")
      .csv("some-data.csv")
    groceries
  }
}
