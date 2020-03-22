package com.imooc.spark

import org.apache.spark.sql.SparkSession

object SparkSessionApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSessionApp ").master("local").getOrCreate()
    val people = spark.read.format("json").load("file:///usr/local/spark/examples/src/main/resources/people.json")
    people.show()
    spark.stop()
  }
}
