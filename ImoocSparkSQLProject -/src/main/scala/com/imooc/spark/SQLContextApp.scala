package com.imooc.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * SQLContext的使用
 */
object SQLContextApp {
  def main(args: Array[String]): Unit = {
    val path = args(0) //第0个参数
    //1 创建相应的Context
    val sparkConf = new SparkConf()
    //在测试或者生产中，AppName和Master我们是通过你脚本进行指定的
//    sparkConf.setAppName("SQLContextApp").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)//1.x版本的写法

    //2 相关处理:jason
   val people =  sqlContext.read.format("json").load(path )
    people.printSchema()
    people.show()
    //3 关闭资源
    sc.stop()
  }

}
