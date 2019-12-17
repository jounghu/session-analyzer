package com.skrein.dao

import com.skrein.model.{PartAggInfo, StepIntervalPercent}
import com.skrein.util.DBUtils
import org.apache.commons.logging.{Log, LogFactory}

import scala.collection.mutable.ArrayBuffer

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/12/17 11:13
 * @since :1.8
 *
 */
object SessionExtractDao {

  val logger: Log = LogFactory.getLog(SessionExtractDao.getClass)

  def insertExtractSessions(sessions: ArrayBuffer[PartAggInfo],taskId:Int) = {
    val conn = DBUtils.getConnection()
    val sql = "INSERT INTO t_session_extract(task_id,session_id,start_time,search_keywords,click_category_ids) \nVALUES (?,?,?,?,?)"
    val pstm = conn.prepareStatement(sql)
    logger.info(s"Insert into t_step_interval sql: ${sql}")

    conn.setAutoCommit(false)

    for(i <- sessions.indices){
      val session = sessions(i)
      pstm.setInt(1,taskId.toInt)
      pstm.setString(2,session.sessionId)
      pstm.setString(3,session.startTime)
      pstm.setString(4,session.searchKeywords)
      pstm.setString(5,session.clickCategoryIds)
      pstm.addBatch()
    }
    pstm.executeBatch()
    pstm.clearBatch()
    conn.commit()
    pstm.close()
    DBUtils.releaseConnection(conn)
  }
}
