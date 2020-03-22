package com.imooc.log

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

//TopN统计Spark作业
object TopNStatJOb {
  def main(args: Array[String]) = {
    val day = "20161110"
    val path = "/home/hadoop/桌面/Spark编程代码练习/慕课网日志实战/clean"
    //由于我们设置的day字段StringTYpe类型会被系统自动读取为integer类型，所以避免此发生，下面语句加上.config
    val spark = SparkSession.builder().master("local[2]").appName("TopNStatJOb")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .getOrCreate()
    val accessDF = spark.read.format("parquet").load(path)
    accessDF.printSchema()
    accessDF.show(false)
    StatDAO.deleteData(day)
    //最受欢迎的topN课程
    videoAccessTopNStat(spark, accessDF,day:String)

    //按照地市进行统计TopN课程
    cityAccessTopNStat(spark, accessDF,day:String)
    //按照流量进行统计TopN课程
    videoTrafficsTopStat(spark, accessDF,day:String)
    spark.stop()
  }

  def videoTrafficsTopStat(spark: SparkSession, accessDF: DataFrame,day:String): Unit = {

    import spark.implicits._
    val top4DF = accessDF.filter($"day" === day && $"cmsType" === "video")
      .groupBy("day", "cmsId").agg(sum("traffic").as("traffics")).orderBy($"traffics".desc)
    //.show(false)

    try {
      top4DF.foreachPartition(partitionOfRecords => { //partitionOfRecords 这个名字是随便取的
        //所有数据首先要转换为实体类DayVideoAccessStat(自己创建的)，所以先创建一个list来存放他
        val list = new ListBuffer[DayVideoTrafficsStat]
        //然后对每个分区的每一条记录进行获取
        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val traffics = info.getAs[Long]("traffics")
          //取出来了，接着构建到list里面去
          list.append(DayVideoTrafficsStat(day, cmsId, traffics))
        })
        //调用list写进去
        StatDAO.insertDayVideoTrafficsAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } //没有需要释放的，所以不用finnally

  }

  def cityAccessTopNStat(spark: SparkSession, accessDF: DataFrame,day:String): Unit = {

    import spark.implicits._
    val cityAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video")
      .groupBy("day", "city", "cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)

    //Winodws函数在spark SQL中的使用
    val top3DF = cityAccessTopNDF.select(cityAccessTopNDF("day"),
      cityAccessTopNDF("city"),
      cityAccessTopNDF("cmsId"),
      cityAccessTopNDF("times"),
      row_number().over(Window.partitionBy(cityAccessTopNDF("city")).orderBy(cityAccessTopNDF("times").desc)).as("times_rank")
    ).filter("times_rank <=3") //.show(false)
    try {
      top3DF.foreachPartition(partitionOfRecords => { //partitionOfRecords 这个名字是随便取的
        //所有数据首先要转换为实体类DayVideoAccessStat(自己创建的)，所以先创建一个list来存放他
        val list = new ListBuffer[DayCityVideoAccessStat]
        //然后对每个分区的每一条记录进行获取
        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val city = info.getAs[String]("city")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")
          val timesRank = info.getAs[Int]("times_rank")
          //取出来了，接着构建到list里面去
          list.append(DayCityVideoAccessStat(day, cmsId, city, times, timesRank))
        })
        //调用list写进去
        StatDAO.insertDayCityVideoAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } //没有需要释放的，所以不用finnally

  }

  //    videoAccessTopNDF.sho
  def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame,day:String): Unit = {
    //    //第一个是DataFrame方式
            import spark.implicits._
            val videoAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video")
              .groupBy("day","cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)
    //    //    videoAccessTopNDF.show(false)

    //第二个是SQL方式
//    accessDF.createOrReplaceTempView("access_logs")
//    val videoAccessTopNDF = spark.sql("select day, cmsId ,count(1) as times from access_logs" +
//      " where day = '20161110' and cmsType = 'video'" +
//      " group by day, cmsId" +
//      " order by times desc")
//    videoAccessTopNDF.show(false)
    /*将统计结果写入MYSQL中*/
    //一下步骤以后这是这种写法，第一次不会，以后就明白了
    try {
      videoAccessTopNDF.foreachPartition(partitionOfRecords => { //partitionOfRecords 这个名字是随便取的
        //所有数据首先要转换为实体类DayVideoAccessStat(自己创建的)，所以先创建一个list来存放他
        val list = new ListBuffer[DayVIdeoAccessStat]
        //然后对每个分区的每一条记录进行获取
        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")
          //取出来了，接着构建到list里面去
          list.append(DayVIdeoAccessStat(day, cmsId, times))
        })
        //调用list写进去
        StatDAO.insertDayVideoAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } //没有需要释放的，所以不用finnally

  }
}
