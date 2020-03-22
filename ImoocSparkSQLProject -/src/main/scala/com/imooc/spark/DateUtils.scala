package com.imooc.spark

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/*日期时间解析工具类
* SimpleDateFormat是线程不安全的，会出现时间1970
* 所以用FastDateFormat
* */
object DateUtils {
  //10/Nov/2016:00:01:01 +0800, 日志里面的日期时间是这样的
//  val YYYYMMDDHHMM_TIME_FORMAT = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH) //输入文件日期格式
  val YYYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH) //输入文件日期格式
  //FastDateFormat不用new只需要getInstance
//  val TARGET_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") //目标文件日期格式
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss") //目标文件日期格式
  def parse1(time: String) = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  def getTime(time: String) = {
    try {
      YYYYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1, time.lastIndexOf("]"))).getTime
      //substring() 方法返回的子串包括 开始 处的字符，但不包括 结束 处的字符
    } catch {
      case e: Exception => {
        0l
      } //输出一个0，long类型
    }
  }


  def main(args: Array[String]): Unit = {
    println(parse1("[10/Nov/2016:00:01:01 +0800]"))
  }
}
