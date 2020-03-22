package com.imooc.spark

import java.sql.DriverManager

/*通过JDBC的方式访问*/
object SparkSQLThriftServerApp {
  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
   val conn =  DriverManager.getConnection("jdbc:hive2://localhost:10000","hadoop","")
    val pstmt = conn.prepareStatement("select * from my")
    val rs = pstmt.executeQuery()
    while (rs.next()){
     println("my : "+rs.getString("context"))
    }
    rs.close()
    pstmt.close()
    conn.close()
  }
}

