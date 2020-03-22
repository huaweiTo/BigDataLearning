package com.imooc.spark

import org.apache.spark.sql.SparkSession

/*第一步清洗 ： 抽取我们所需要的指定列的数据*/
object SparkStatFormatJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatFormatJob ").master("local")getOrCreate()
    val access = spark.sparkContext.textFile("/home/hadoop/桌面/Spark编程代码练习/慕课网日志实战/10000_access.log")//读出来的是rdd
    //    access.take(10).foreach(println)
    access.map(line=>{
      val splits = line.split(" ")
      val ip = splits(0)
      val time = splits(3)+" "+splits(4)//原始日志的第3,4个字段拼接起来就是完整的访问时间.格式挺难看需要解析一下
      val url = splits(11).replaceAll("\"","")
      val traffic = splits(9)
      //      (ip,DateUtils.parse1(time),url,traffic)
      DateUtils.parse1(time)+"\t"+url+"\t"+traffic+"\t"+ip
      //    }).take(10).foreach(println)
    }).saveAsTextFile("file:///home/hadoop/桌面/Spark编程代码练习/慕课网日志实战/output/")
    spark.stop()
  }

}
