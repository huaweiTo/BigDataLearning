package com.imooc.log

import java.sql.{Connection, DriverManager, PreparedStatement}

/*MySQL操作工具类*/
object MySQLUtils {

  def getConnection()={
    DriverManager.getConnection("jdbc:mysql://localhost:3306/imooc_project?user=hive&password=hive&useSSL=false")
    //报错,把mysqlconnect的jar包放到IDEAjavajvm中去
//    DriverManager.getConnection("jdbc:mysql://localhost:3306/imooc_project?useUnicode=true&characterEncoding=utf8",
//      "root","hadoop")
  }

  /**
   * 释放数据库链接资源
   * @param connection
   * @param pstmt
   */
  def release(connection:Connection,pstmt:PreparedStatement)={
  try{
    if(pstmt != null){
      pstmt.close()
    }

  }catch{case e:Exception => e.printStackTrace()}finally{
    if (connection != null){
      connection.close()
    }

  }
}
  def main(args: Array[String])  {
    println(getConnection())
  }
}
