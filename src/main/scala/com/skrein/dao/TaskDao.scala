package com.skrein.dao

import java.sql.ResultSet

import com.skrein.model.Task
import com.skrein.util.DBUtils
import org.apache.commons.logging.{Log, LogFactory}


/**
 *
 *
 * @author :hujiansong  
 * @date :2019/12/9 16:33
 * @since :1.8
 *
 */
object TaskDao {
  val logger: Log = LogFactory.getLog(TaskDao.getClass)

  def findTaskById(taskId: Int): Task = {
    //  构造Task SQL
    val sql = s"SELECT * FROM t_task WHERE id = ${taskId}"
    val dbHelper = new DBUtils
    var resultSet: ResultSet = null
    try {
      logger.info(s"exec sql ${sql}")
      resultSet = dbHelper.executeSQL(sql)
      while (resultSet.next()) {
        return Task(
          resultSet.getInt(1),
          resultSet.getString(2),
          resultSet.getString(3),
          resultSet.getDate(4),
          resultSet.getDate(5)
        )
      }
    }
    finally {
      if (resultSet!=null){
        resultSet.close()
      }
    }
    null
  }

}
