package com.skrein.dao

import com.skrein.model.{CategoryTop, Top10CategorySession}
import com.skrein.util.DBUtils
import org.apache.commons.logging.{Log, LogFactory}

import scala.collection.mutable.ArrayBuffer

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/12/17 14:52
 * @since :1.8
 *
 */
object Top10CategorySessionDao {

  val logger: Log = LogFactory.getLog(Top10CategorySessionDao.getClass)

  def insertTop10Sessions(cateSessions: ArrayBuffer[Top10CategorySession]) = {
    val conn = DBUtils.getConnection()
    val sql = "INSERT INTO t_top10_cate_session(task_id,category_id,session_id,click_cnt)\nVALUES (?,?,?,?)"
    val pstm = conn.prepareStatement(sql)
    logger.info(s"Insert into insertSessions sql: ${sql}")
    conn.setAutoCommit(false)
    for (i <- cateSessions.indices) {
      val cateSession = cateSessions(i)
      pstm.setInt(1, cateSession.taskId)
      pstm.setInt(2, cateSession.categoryId)
      pstm.setString(3, cateSession.sessionId)
      pstm.setLong(4, cateSession.clickCnt)
      pstm.addBatch()
    }
    pstm.executeBatch()
    pstm.clearBatch()
    conn.commit()
    pstm.close()
    DBUtils.releaseConnection(conn)
  }

}
