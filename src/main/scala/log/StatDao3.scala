package log

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

/**
  * 各个维度统计的Dao操作
  */
object StatDao3 {
  /**
    * 批量保存到DayVideoAccessStat数据库中
    *
    */

  /**
    * 批量保存DayVideoAccessStat到数据库
    */
  def insertDayVideoAccessTopN(list: ListBuffer[DayVideoTrafficsStat]): Unit = {

    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MysqlUtils.getConnection()

      connection.setAutoCommit(false) //设置手动提交

      val sql = "insert into day_video_traffics_topn_stat(day,cms_id,traffics) values (?,?,?)"
      pstmt = connection.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.traffics)

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
  /**
    * 删除指定日期的数据
    */
  def deleteData(day: String): Unit = {

    val tables = Array(
      "day_video_access_topn_stat"
//      "day_video_city_access_topn_stat",
//      "day_video_traffics_topn_stat"
      )

    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {

      connection = MysqlUtils.getConnection()
      for (table <- tables) {
        val deleteSQL = s"delete from $table where day=?"
        pstmt = connection.prepareStatement(deleteSQL)
        pstmt.setString(1, day)
        pstmt.executeUpdate()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
     MysqlUtils.release(connection, pstmt)
    }

  }




}
