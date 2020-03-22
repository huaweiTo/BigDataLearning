package com.imooc.log

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType

/*使用SPARK完成我们的数据清洗操作
* */
object SparkStatCleanJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatCleanJob ").master("local[2]").getOrCreate()
    val accessRDD = spark.sparkContext.textFile("file:///home/hadoop/桌面/Spark编程代码练习/慕课网日志实战/output/part-00000")
    //  accessRDD.take(30).foreach(println)
    //RDD => DF
    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)), AccessConvertUtil.struct)

    //  accessDF.printSchema()
    //  accessDF.show(100,false)
    //coalesce()指定分区数
    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite)
      .partitionBy("day")
      .save("/home/hadoop/桌面/Spark编程代码练习/慕课网日志实战/clean")
    spark.stop()
  }
}

