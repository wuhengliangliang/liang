package log
import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

/**
  * 各个维度统计的Dao操作
  */
object StatDao2 {
  /**
    * 批量保存到DayCityVideoAccessStat数据库中
    *
    */

  /**
    * 批量保存DayVideoAccessStat到数据库
    */
  def insertDayCityVideoAccessTopN(list: ListBuffer[DayCityVideoAccessStat]): Unit = {

    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MysqlUtils.getConnection()

      connection.setAutoCommit(false) //设置手动提交

      val sql = "insert into day_video_city_access_topn_stat(day,cms_id,city,times,times_rank) values(?,?,?,?,?)"
      pstmt = connection.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setString(3,ele.city)
        pstmt.setLong(4, ele.times)
        pstmt.setInt(5,ele.timesRank)

        pstmt.addBatch()
      }

      pstmt.executeBatch() // 执行批量处理
      connection.commit() //手工提交
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MysqlUtils.release(connection,pstmt)
    }
  }
}