package com.skrein.dao

import com.skrein.model.CategoryTop
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
object Top10CategoryDao {

  val logger: Log = LogFactory.getLog(Top10CategoryDao.getClass)

  def insertCategories(categories: ArrayBuffer[CategoryTop]) = {
    val conn = DBUtils.getConnection()
    val sql = "INSERT INTO t_top10_cate(task_id,cate_id,click_cnt,pay_cnt,order_cnt)\nVALUES (?,?,?,?,?)"
    val pstm = conn.prepareStatement(sql)
    logger.info(s"Insert into t_top10_cate sql: ${sql}")
    conn.setAutoCommit(false)
    for (i <- categories.indices) {
      val cate = categories(i)
      pstm.setInt(1, cate.taskId)
      pstm.setInt(2, cate.cateId)
      pstm.setLong(3, cate.clickCnt)
      pstm.setLong(4, cate.payCnt)
      pstm.setLong(5, cate.orderCnt)
      pstm.addBatch()
    }
    pstm.executeBatch()
    pstm.clearBatch()
    conn.commit()
    pstm.close()
    DBUtils.releaseConnection(conn)
  }

}
