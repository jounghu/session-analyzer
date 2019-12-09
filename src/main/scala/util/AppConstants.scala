package util

object AppConstants {
  val appName = AppConf.getString("appName")
  val master = AppConf.getString("appMaster")

  val local = AppConf.getBoolean("local")


}
