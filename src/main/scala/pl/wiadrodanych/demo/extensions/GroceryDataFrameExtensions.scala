package pl.wiadrodanych.demo.extensions

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import pl.wiadrodanych.demo.base.SparkJob.spark.implicits._

object GroceryDataFrameExtensions {

  implicit class RichDataFrame(df: DataFrame) {

    def sumByNormalizedName: DataFrame = {
      val sumOfFruits = df
        .groupBy("normalized_name")
        .agg(
          sum(($"quantity")).as("sum")
        )
      sumOfFruits
    }

    def addNormalizedNameColumn: DataFrame = {
      val normalizedFruits = df.withColumn("normalized_name", lower($"name"))
      normalizedFruits
    }

    def filterFruits: DataFrame = {
      val fruits = df.filter($"type" === "fruit")
      fruits
    }
  }

}
