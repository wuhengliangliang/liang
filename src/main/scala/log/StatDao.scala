package log

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

/**
  * 各个维度统计的Dao操作
  */
object StatDao {
  /**
    * 批量保存到DayVideoAccessStat数据库中
    *
    */

  /**
    * 批量保存DayVideoAccessStat到数据库
    */
  def insertDayVideoAccessTopN(list: ListBuffer[DayVideoAccessStat]): Unit = {

    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MysqlUtils.getConnection()

      connection.setAutoCommit(false) //设置手动提交

      val sql = "insert into day_video_access_topn_stat(day,cms_id,times) values (?,?,?) "
      pstmt = connection.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.times)

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
