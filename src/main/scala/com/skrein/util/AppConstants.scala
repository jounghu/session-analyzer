package com.skrein.util

object AppConstants {
  val appName = AppConf.getString("appName")
  val master = AppConf.getString("appMaster")

  val local = AppConf.getBoolean("local")

  // mysql config
  val MYSQL_URL = "mysql.url";
  val MYSQL_USER = "mysql.user";
  val MYSQL_PASSWORD = "mysql.password";
  val MYSQL_CONNECTION_POOL_SIZE = "mysql.connection.pool.size"

  // task param config
  val SESSION_FILTER_START_DATE = "start_date"
  val SESSION_FILTER_END_DATE = "end_date"

}
