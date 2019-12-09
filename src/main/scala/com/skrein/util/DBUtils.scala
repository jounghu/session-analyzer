package com.skrein.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.concurrent.{BlockingQueue, LinkedBlockingDeque}

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/12/9 16:42
 * @since :1.8
 *
 */
class DBUtils {
  private var connection: Connection = _

  private var pstm: PreparedStatement = _

  private var resultSet: ResultSet = _

  def executeSQL(sql: String): ResultSet = {
    try {
      connection = DBUtils.getConnection()
      pstm = connection.prepareStatement(sql)
      resultSet = pstm.executeQuery()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      DBUtils.releaseConnection(connection)
    }
    resultSet
  }

}

object DBUtils {
  val url = AppConf.getString(AppConstants.MYSQL_URL)
  val user = AppConf.getString(AppConstants.MYSQL_USER)
  val password = AppConf.getString(AppConstants.MYSQL_PASSWORD)
  val maxConnectionSize = AppConf.getInt(AppConstants.MYSQL_CONNECTION_POOL_SIZE)

  Class.forName("com.mysql.jdbc.Driver")

  val pool: BlockingQueue[Connection] = new LinkedBlockingDeque[Connection]()

  for (i <- 1 to maxConnectionSize) {
    pool.put(DriverManager.getConnection(url, user, password))
  }

  def getConnection(): Connection = {
    pool.take()
  }

  def releaseConnection(connection: Connection): Unit = {
    pool.put(connection)
  }

  def cleanAll() = {
    val thread = new Thread(new CloseRunnable)
    thread.setDaemon(true)
    thread.start()
  }


  class CloseRunnable extends Runnable {
    override def run(): Unit = {
      while (pool.size() > 0) {
        try {
          pool.take().close()
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val db = new DBUtils()
    val resultSet = db.executeSQL("SELECT * FROM t_task")
    while (resultSet.next()){
      print(resultSet.getString("task_name"))
    }
    resultSet.close()

  }

}
