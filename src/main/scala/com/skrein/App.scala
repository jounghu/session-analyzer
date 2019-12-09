package com.skrein

import com.skrein.dao.TaskDao
import com.skrein.mock.DataMock
import com.skrein.model.PartAggInfo
import com.skrein.util.{AppConstants, DateUtil, JsonParse, StringUtils}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/12/9 15:39
 * @since :1.8
 *
 */
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


  def combineSession(sessionDF: DataFrame, userDF: DataFrame) = {
    val userRdd = userDF.rdd.map(user => {
      val userId = user.getAs("userId").asInstanceOf[Int].toLong
      (userId, user)
    })

    val aggRdd = sessionDF.rdd.map(row => {
      (row.getAs[String]("sessionId"), row)
    }).groupByKey()
      .map { case (sessionId, rows) => {
        val searchKeywordList = ArrayBuffer[String]()
        val clickCategoryIdList = ArrayBuffer[String]()
        var startTime: String = null
        var endTime: String = null
        var stepLength: Long = 0
        var userId: Long = -1
        for (row <- rows) {
          val searchKeyword = row.getAs[String]("searchKeyword")
          val clickCategoryId = row.getAs[Long]("clickCategoryId").toString
          if (!StringUtils.isEmpty(searchKeyword) && !searchKeywordList.contains(searchKeyword)) {
            searchKeywordList += searchKeyword
          }

          if (userId == -1) {
            userId = row.getAs("userId").asInstanceOf[Long]
          }

          if (!StringUtils.isEmpty(clickCategoryId) && !clickCategoryIdList.contains(clickCategoryId)) {
            clickCategoryIdList += clickCategoryId
          }

          // 计算开始时间和结束时间
          val actionTime = row.getAs("actionTime").asInstanceOf[String]
          if (StringUtils.isEmpty(startTime)) {
            startTime = actionTime
          }

          if (StringUtils.isEmpty(endTime)) {
            endTime = actionTime
          }

          if (DateUtil.before(startTime, actionTime)) {
            startTime = actionTime
          }

          if (DateUtil.after(endTime, actionTime)) {
            endTime = actionTime
          }

          // 步长增加1
          stepLength += 1

        }
        val searchKeywordString = StringUtils.join(searchKeywordList)
        val clickCategoryIdString = StringUtils.join(clickCategoryIdList)
        // 计算session访问时长
        val visitCostTime = DateUtil.cost(startTime, endTime)

        // 课中老师采用字符串，拼接，我感觉实在是太不符合企业要求了
        // 我这里的实现采用，case class
        val partAgg = PartAggInfo(sessionId, searchKeywordString, clickCategoryIdString, visitCostTime, stepLength)
        (userId, partAgg)
      }
      }
      .join(userRdd)
      .map {
        case (userId, (pageAggInfo, userRow)) =>
          pageAggInfo.age = userRow.getAs("age").asInstanceOf[Int]
          pageAggInfo.professional = userRow.getAs("profession").asInstanceOf[String]
          pageAggInfo.city = userRow.getAs("city").asInstanceOf[String]
          pageAggInfo.sex = userRow.getAs("sex").asInstanceOf[String]
          (pageAggInfo.sessionId, pageAggInfo)
      }

    aggRdd.foreachPartition(p => {
      p.foreach(println)
    })
  }

  def filter(taskParam: String, sessionDF: DataFrame): DataFrame = {
    val json = new JsonParse(taskParam)
    val startDate = json.getField(AppConstants.SESSION_FILTER_START_DATE)
    val endDate = json.getField(AppConstants.SESSION_FILTER_END_DATE)
    sessionDF.filter(sessionDF("date").between(startDate, endDate))
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName(AppConstants.appName)
      .setMaster(AppConstants.master)


    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = getSQLContext(sparkContext)

    val sessionDF = DataMock.mockSession(sqlContext)
    val userDF = DataMock.mockUser(sqlContext)


    val taskId = args(0).toInt
    if (taskId == 0) {
      throw new RuntimeException("taskId not be 0")
    }

    val task = TaskDao.findTaskById(taskId)

    val filterSession = filter(task.taskParam, sessionDF)
    combineSession(filterSession, userDF)

  }
}
