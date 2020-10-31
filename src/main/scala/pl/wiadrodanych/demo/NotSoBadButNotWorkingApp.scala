//package pl.wiadrodanych.demo
//
//import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
//
//import org.apache.spark.sql._
//import org.apache.spark.sql.functions._
//
//object NotSoBadButNotWorkingApp {
//
//  def main(args: Array[String]) {
//    val spark = SparkSession
//      .builder
//      .appName("MyAwesomeApp")
//      .master("local[*]")
//      .getOrCreate()
//
//    import spark.implicits._
//
//    val groceries: DataFrame = getGroceries
//    val fruits: Dataset[Row] = filterFruits(groceries)
//    val normalizedFruits: DataFrame = withNormalizedName(fruits)
//    val sumOfFruits: DataFrame = sumByNormalizedName(normalizedFruits)
//
//    sumOfFruits.show()
//  }
//
//  private def sumByNormalizedName(normalizedFruits: DataFrame) = {
//    val sumOfFruits = normalizedFruits
//      .groupBy("normalized_name")
//      .agg(
//        sum(($"quantity")).as("sum")
//      )
//    sumOfFruits
//  }
//
//  private def withNormalizedName(fruits: Dataset[Row]) = {
//    val normalizedFruits = fruits.withColumn("normalized_name", lower($"name"))
//    normalizedFruits
//  }
//
//  private def filterFruits(groceries: DataFrame) = {
//    val fruits = groceries.filter($"type" === "fruit")
//    fruits
//  }
//
//  private def getGroceries: DataFrame = {
//
//  val groceries = spark.read
//    .option("inferSchema","true")
//    .option("header","true")
//    .csv("some-data.csv")
//    groceries
//  }
//}
