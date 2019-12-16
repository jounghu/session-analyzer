package com.skrein.dao

import com.skrein.model.{CostIntervalPercent, StepIntervalPercent}
import com.skrein.util.DBUtils
import org.apache.commons.logging.{Log, LogFactory}

object SessionIntervalDao {
  val logger: Log = LogFactory.getLog(SessionIntervalDao.getClass)

  def insertStepInterval(stePercent: StepIntervalPercent) = {
    val conn = DBUtils.getConnection()
    val sql = "INSERT INTO t_step_interval (task_id,step_1_3, step_4_6, step_7_9, step_10_30, step_30_60, step_60) VALUES(?,?,?,?,?,?,?);"
    val pstm = conn.prepareStatement(sql)
    logger.info(s"Insert into t_step_interval sql: ${sql}")
    pstm.setInt(1,stePercent.taskId)
    pstm.setDouble(2,stePercent.step_1_3_per)
    pstm.setDouble(3,stePercent.step_4_6_per)
    pstm.setDouble(4,stePercent.step_7_9_per)
    pstm.setDouble(5,stePercent.step_10_30_per)
    pstm.setDouble(6,stePercent.step_30_60_per)
    pstm.setDouble(7,stePercent.step_60_per)
    pstm.execute()
    pstm.close()
    DBUtils.releaseConnection(conn)
  }


  def insertCostInterval(costPercent: CostIntervalPercent) = {
    val conn = DBUtils.getConnection()
    val sql = "INSERT INTO t_cost_interval(task_id,cost_1_3,cost_4_6,cost_7_9,cost_10_30,cost_30_60,cost_1m_3m,cost_3m_10m,cost_10m_30m)\nVALUES (?,?,?,?,?,?,?,?,?);"
    val pstm = conn.prepareStatement(sql)
    logger.info(s"Insert into t_cost_interval sql: ${sql}")
    pstm.setInt(1,costPercent.taskId)
    pstm.setDouble(2,costPercent.cost_1_3_per)
    pstm.setDouble(3,costPercent.cost_4_6_per)
    pstm.setDouble(4,costPercent.cost_7_9_per)
    pstm.setDouble(5,costPercent.cost_10_30_per)
    pstm.setDouble(6,costPercent.cost_30_60_per)
    pstm.setDouble(7,costPercent.cost_1m_3m_per)
    pstm.setDouble(8,costPercent.cost_3m_10m_per)
    pstm.setDouble(9,costPercent.cost_10m_30m_per)
    pstm.execute()
    pstm.close()
    DBUtils.releaseConnection(conn)
  }


}
