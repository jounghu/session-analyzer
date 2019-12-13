package com.skrein

import com.skrein.core.{CostAccumulator, StepAccumulator}
import com.skrein.dao.TaskDao
import com.skrein.mock.DataMock
import com.skrein.model.{CostInterval, PartAggInfo, StepInterval}
import com.skrein.util.{AppConstants, DateUtil, JsonParse, StringUtils}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

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


  def aggCostTime(visitCostTime: Long, costAgg: Accumulator[CostInterval]) = {
    val costInterval: CostInterval =
      if (visitCostTime >= 1 && visitCostTime <= 3) {
        new CostInterval(cost_1_3 = 1)
      } else if (visitCostTime >= 4 && visitCostTime <= 6) {
        new CostInterval(cost_4_6 = 1)
      }
      else if (visitCostTime >= 7 && visitCostTime <= 9) {
        new CostInterval(cost_7_9 = 1)
      }
      else if (visitCostTime >= 10 && visitCostTime <= 30) {
        new CostInterval(cost_10_30 = 1)
      }
      else if (visitCostTime >= 31 && visitCostTime <= 60) {
        new CostInterval(cost_30_60 = 1)
      }
      else if (visitCostTime >= 61 && visitCostTime <= 60 * 3) {
        new CostInterval(cost_1m_3m = 1)
      }
      else if (visitCostTime >= 181 && visitCostTime <= 600) {
        new CostInterval(cost_3m_10m = 1)
      } else if (visitCostTime >= 601 && visitCostTime <= 30 * 60) {
        new CostInterval(cost_10m_30m = 1)
      } else {
        new CostInterval(cost_4_6 = 1)
      }
    costAgg.add(costInterval)
  }

  def aggStepLength(stepLength: Long, stepAgg: Accumulator[StepInterval]) = {
    val stepInterval: StepInterval =
      if (stepLength >= 1 && stepLength <= 3) {
        new StepInterval(step_1_3 = 1)
      } else if (stepLength >= 4 && stepLength <= 6) {
        new StepInterval(step_4_6 = 1)
      } else if (stepLength >= 7 && stepLength <= 9) {
        new StepInterval(step_7_9 = 1)
      } else if (stepLength >= 10 && stepLength <= 30) {
        new StepInterval(step_10_30 = 1)
      } else if (stepLength >= 31 && stepLength <= 60) {
        new StepInterval(step_30_60 = 1)
      } else {
        new StepInterval(step_60 = 1)
      }
    stepAgg.add(stepInterval)
  }

  def combineSession(sessionDF: DataFrame, userDF: DataFrame, costAgg: Accumulator[CostInterval], stepAgg: Accumulator[StepInterval]) = {
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

        // TASK: 计算访问步长放入累加器中
        aggCostTime(visitCostTime, costAgg)

        aggStepLength(stepLength, stepAgg)

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

    aggRdd.take(10).foreach(println)

    println( stepAgg.value.toString)
    println(costAgg.value.toString)
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
    val costAgg = sparkContext.accumulator(new CostInterval())(new CostAccumulator)
    val stepAgg = sparkContext.accumulator(new StepInterval())(new StepAccumulator)


    val taskId = args(0).toInt
    if (taskId == 0) {
      throw new RuntimeException("taskId not be 0")
    }

    val task = TaskDao.findTaskById(taskId)
    val filterSession = filter(task.taskParam, sessionDF)
    combineSession(filterSession, userDF, costAgg, stepAgg)


  }
}
