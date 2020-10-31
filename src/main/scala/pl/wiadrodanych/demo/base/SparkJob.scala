package pl.wiadrodanych.demo.base

import org.apache.spark.sql.SparkSession

trait SparkJob {
  val spark: SparkSession = SparkSession
    .builder
    .appName("SomeApp")
    .master("local[*]")
    .getOrCreate()
}

object SparkJob extends SparkJob {}