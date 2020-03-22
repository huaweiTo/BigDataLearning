package com.imooc.log

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

/**
 * 各个维度统计的DAO操作 /
 */

object StatDAO {
  /**
   * 批量保存DayVideoTrafficsStat到数据库
   * @param list
   */
  def insertDayVideoTrafficsAccessTopN(list: ListBuffer[DayVideoTrafficsStat]) = { //有可能多条，所以用集合
    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = MySQLUtils.getConnection()
      connection.setAutoCommit(false) //默认是自动，我们为了批量处理，设置为手动
      val sql = "insert into day_video_traffics_topn_stat values (?,?,?)"
      //使用PreparedStatement:是Statement的子接口,可以传入带占位符的SQL语句，提供了补充占位符变量的方法
      //可以看到将sql作为参数传入了，就不需要我们在费力拼写了。?占位符
      //可以调用PreparedStatement的setXxx(int index,Object val)设置占位符的值，其中index的值从1开始
      pstmt = connection.prepareStatement(sql)
      for (ele <- list) { //List里的数据遍历一遍放入数据库
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.traffics)
        pstmt.addBatch() //添加到批处理里面
      }
      pstmt.executeBatch() //执行批量处理
      connection.commit() //手动提交
    } catch {
      case e: Exception => e.printStackTrace()
    }
    finally {
      MySQLUtils.release(connection, pstmt)
    }
  }
  /**
   * 批量保存DayVIdeoAccessStat到数据库
   */
  def insertDayVideoAccessTopN(list: ListBuffer[DayVIdeoAccessStat]) = { //有可能多条，所以用集合
    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = MySQLUtils.getConnection()
      connection.setAutoCommit(false) //默认是自动，我们为了批量处理，设置为手动
      val sql = "insert into day_video_access_topn_stat values (?,?,?)"
      //使用PreparedStatement:是Statement的子接口,可以传入带占位符的SQL语句，提供了补充占位符变量的方法
      //可以看到将sql作为参数传入了，就不需要我们在费力拼写了。?占位符
      //可以调用PreparedStatement的setXxx(int index,Object val)设置占位符的值，其中index的值从1开始
      pstmt = connection.prepareStatement(sql)
      for (ele <- list) { //List里的数据遍历一遍放入数据库
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.times)
        pstmt.addBatch() //添加到批处理里面
      }
      pstmt.executeBatch() //执行批量处理
      connection.commit() //手动提交
    } catch {
      case e: Exception => e.printStackTrace()
    }
    finally {
      MySQLUtils.release(connection, pstmt)
    }
  }

  /**
   * 批量保存DayCityVideoAccessStat到数据库
   * @param list
   */
  def insertDayCityVideoAccessTopN(list: ListBuffer[DayCityVideoAccessStat]) = { //有可能多条，所以用集合
    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = MySQLUtils.getConnection()
      connection.setAutoCommit(false) //默认是自动，我们为了批量处理，设置为手动
      val sql = "insert into day_video_city_access_topn_stat values (?,?,?,?,?)"
      //使用PreparedStatement:是Statement的子接口,可以传入带占位符的SQL语句，提供了补充占位符变量的方法
      //可以看到将sql作为参数传入了，就不需要我们在费力拼写了。?占位符
      //可以调用PreparedStatement的setXxx(int index,Object val)设置占位符的值，其中index的值从1开始
      pstmt = connection.prepareStatement(sql)
      for (ele <- list) { //List里的数据遍历一遍放入数据库
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setString(3, ele.city)
        pstmt.setLong(4, ele.times)
        pstmt.setLong(5, ele.timesRank)
        pstmt.addBatch() //添加到批处理里面
      }
      pstmt.executeBatch() //执行批量处理
      connection.commit() //手动提交
    } catch {
      case e: Exception => e.printStackTrace()
    }
    finally {
      MySQLUtils.release(connection, pstmt)
    }
  }

  def deleteData(day:String)={
    val tables = Array("day_video_access_topn_stat","day_video_city_access_topn_stat","day_video_traffics_topn_stat")
    var connection:Connection = null
    var pstmt:PreparedStatement= null
    try{
      connection=MySQLUtils.getConnection()
      for(table <- tables){

        val sql = s"delete from $table where day =?"
        pstmt = connection.prepareStatement(sql)
        pstmt.setString(1,day)
        pstmt.executeUpdate()
      }
    }catch {case e:Exception => e.printStackTrace()}finally {MySQLUtils.release(connection,pstmt)}
  }
}
