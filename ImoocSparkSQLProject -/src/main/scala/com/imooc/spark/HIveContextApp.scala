package com.imooc.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext

/*
*HIveContext的使用
 */
object HIveContextApp {
  def main (args:Array[String]): Unit ={
    val sparkConf = new SparkConf().setAppName("HIveContextApp").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    hiveContext.table("my").show
    sc.stop()
  }
}
