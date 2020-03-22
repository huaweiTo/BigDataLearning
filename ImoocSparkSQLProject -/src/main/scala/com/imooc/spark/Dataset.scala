package com.imooc.spark

import org.apache.spark.sql.SparkSession

/*dataframe的相关操作*/
object Dataset {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameApp").master("local[2]").getOrCreate()
    val peopleDF = spark.read.format("json").load("file:///home/hadoop/桌面/Spark编程代码练习/people.json")
    peopleDF.printSchema()
    peopleDF.show
    peopleDF.select(peopleDF("name"),(peopleDF("age")+20).as("fake name")).show()
    peopleDF.filter(peopleDF.col("age")>40).show()
    peopleDF.groupBy("age").count().show()
    spark.stop()
  }

}
