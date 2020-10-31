package pl.wiadrodanych.demo

import org.apache.spark.sql.DataFrame
import pl.wiadrodanych.demo.NiceApp.spark
import pl.wiadrodanych.demo.extensions.GroceryDataFrameExtensions._

object CoolApp {
  def main(args: Array[String]) = {
    val result = getGroceries
      .filterFruits
      .addNormalizedNameColumn
      .sumByNormalizedName

    result.show
  }

  private def getGroceries: DataFrame = {
    val groceries = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("some-data.csv")
    groceries
  }
}
