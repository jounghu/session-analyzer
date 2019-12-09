import mock.DataMock
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import util.AppConstants

object App {

  def getSQLContext(sparkContext: SparkContext): SQLContext = {
    if (AppConstants.local) {
      // local SQLContext
      new SQLContext(sparkContext)
    } else {
      // hiveSQLContext
      new HiveContext(sparkContext)
    }
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName(AppConstants.appName)
      .setMaster(AppConstants.master)
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = getSQLContext(sparkContext)

    DataMock.mockSession(sqlContext)
    DataMock.mockUser(sqlContext)


  }
}
