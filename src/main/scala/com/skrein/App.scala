package com.skrein

import java.util.Random

import com.skrein.core.{CostAccumulator, StepAccumulator}
import com.skrein.dao._
import com.skrein.mock.DataMock
import com.skrein.model._
import com.skrein.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

import scala.collection.mutable
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


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName(AppConstants.appName)
      .setMaster(AppConstants.master)


    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = getSQLContext(sparkContext)

    // mock 数据
    val sessionDF = DataMock.mockSession(sqlContext)
    val userDF = DataMock.mockUser(sqlContext)

    // 自定义累加器
    val costAgg = sparkContext.accumulator(new CostInterval())(new CostAccumulator)
    val stepAgg = sparkContext.accumulator(new StepInterval())(new StepAccumulator)

    val taskId = args(0).toInt
    if (taskId == 0) {
      throw new RuntimeException("taskId not be 0")
    }

    val task = TaskDao.findTaskById(taskId)

    // 过滤session detail
    val filterSession = filter(task.taskParam, sessionDF).cache()

    // 初步聚合点击数据
    val aggRdd = combineSession(filterSession, userDF, costAgg, stepAgg).cache()

    // 随机抽取Session
    randomExtractSession(aggRdd, 10, taskId)

    // 实现categoryId, 点击、下单、支付 二次排序
    // 查找Top10 品类
    val top10Cates = top10Category(aggRdd, filterSession, taskId)

    // top10品类下的点击top10
    top10CategorySessionClick(top10Cates, aggRdd, taskId)

    // 存储步长百分比
    saveStepInterval(stepAgg, taskId)

    // 存储花费时间百分比
    saveCostInterval(costAgg, taskId)

  }

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
    }).groupByKey(4)
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
            searchKeywordList.append(searchKeyword)
          }

          if (userId == -1) {
            userId = row.getAs("userId").asInstanceOf[Long]
          }

          if (!StringUtils.isEmpty(clickCategoryId) && !clickCategoryIdList.contains(clickCategoryId)) {
            clickCategoryIdList.append(clickCategoryId)
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
        val partAgg = PartAggInfo(sessionId, searchKeywordString, clickCategoryIdString, visitCostTime, stepLength, startTime = startTime)
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
    aggRdd
  }

  def filter(taskParam: String, sessionDF: DataFrame): DataFrame = {
    val json = new JsonParse(taskParam)
    val startDate = json.getField(AppConstants.SESSION_FILTER_START_DATE)
    val endDate = json.getField(AppConstants.SESSION_FILTER_END_DATE)
    sessionDF.filter(sessionDF("date").between(startDate, endDate))
  }

  /**
   * 随机抽取Session算法
   *
   * @param aggRdd
   */
  def randomExtractSession(aggRdd: RDD[(String, PartAggInfo)], sessionNum: Long, taskId: Int) = {

    val dayHourSession = aggRdd.map {
      case (sessionId, pageAggInfo) => {
        val startTime = pageAggInfo.startTime
        val dateTimeHour = DateUtil.format(startTime)
        (dateTimeHour, pageAggInfo)
      }
    }.cache()

    // 随机抽取session算法
    val dateHourSessionCount: scala.collection.Map[String, Long] = dayHourSession.countByKey()


    val dayHourMap = mutable.Map[String, mutable.Map[String, Int]]()
    dateHourSessionCount.foreach(t => {
      val dayHour = t._1.split("_")
      val day = dayHour(0)
      val hour = dayHour(1)
      val hourMap = dayHourMap.getOrElse(day, mutable.Map[String, Int]())
      hourMap.put(hour, t._2.toInt)
      dayHourMap.put(day, hourMap)
    })


    val allDaySize = dayHourMap.size
    val perDaySize = sessionNum / allDaySize


    val random = new Random()
    val dayHourMapIndex: mutable.Map[String, ArrayBuffer[Int]] = mutable.Map()
    for ((k, v: mutable.Map[String, Int]) <- dayHourMap) {
      val total = v.values.sum

      // 遍历
      for ((hour, count: Int) <- v) {
        val perHourRate = count.toDouble / total.toDouble * perDaySize
        val perHourSize = if (perHourRate < 1) 1 else perHourRate
        val randomSessionIndex = ArrayBuffer[Int]()
        for (i <- 0 until perHourSize.toInt) {
          var item = random.nextInt(count)
          while (randomSessionIndex.contains(item)) {
            item = random.nextInt(count)
          }
          randomSessionIndex.append(item)
        }
        dayHourMapIndex.put(k + "_" + hour, randomSessionIndex)
      }
    }


    val aggInfos = dayHourSession.groupByKey(5).flatMap {
      t => {
        val dayHourList: ArrayBuffer[Int] = dayHourMapIndex(t._1)

        val sessionAggList = ArrayBuffer[PartAggInfo]()
        val sessionIter = t._2.iterator
        var index = 0
        while (sessionIter.hasNext) {
          val sessionAgg = sessionIter.next()
          if (dayHourList.contains(index)) {
            sessionAggList.append(sessionAgg)
          }
          index = index + 1
        }
        sessionAggList
      }
    }.collect()

    aggInfos.foreach(println)

    SessionExtractDao.insertExtractSessions(aggInfos.to[ArrayBuffer], taskId)

  }

  /**
   * 将累加器包含的值存入MySQL -> `t_step_interval`
   *
   * @param stepAgg
   */
  def saveStepInterval(stepAgg: Accumulator[StepInterval], taskId: Int): Unit = {
    val stepInterval: StepInterval = stepAgg.value
    val count = stepInterval.count()
    val stePercent = StepIntervalPercent(taskId,
      NumberUtil.twoPrecision(stepInterval.step_1_3, count),
      NumberUtil.twoPrecision(stepInterval.step_4_6, count),
      NumberUtil.twoPrecision(stepInterval.step_7_9, count),
      NumberUtil.twoPrecision(stepInterval.step_10_30, count),
      NumberUtil.twoPrecision(stepInterval.step_30_60, count),
      NumberUtil.twoPrecision(stepInterval.step_60, count)
    )
    SessionIntervalDao.insertStepInterval(stePercent)
  }

  def saveCostInterval(costAgg: Accumulator[CostInterval], taskId: Int): Unit = {
    val costInterval = costAgg.value
    val count = costInterval.count()
    val costIntervalPercent = CostIntervalPercent(taskId,
      NumberUtil.twoPrecision(costInterval.cost_1_3, count),
      NumberUtil.twoPrecision(costInterval.cost_4_6, count),
      NumberUtil.twoPrecision(costInterval.cost_7_9, count),
      NumberUtil.twoPrecision(costInterval.cost_10_30, count),
      NumberUtil.twoPrecision(costInterval.cost_30_60, count),
      NumberUtil.twoPrecision(costInterval.cost_1m_3m, count),
      NumberUtil.twoPrecision(costInterval.cost_3m_10m, count),
      NumberUtil.twoPrecision(costInterval.cost_10m_30m, count)
    )

    SessionIntervalDao.insertCostInterval(costIntervalPercent)


  }

  def top10Category(aggRdd: RDD[(String, PartAggInfo)], filterSession: DataFrame, taskId: Int) = {

    val cateSortRdd = filterSession.rdd.map(row => {
      val sessionId: String = row.getAs("sessionId")
      (sessionId, row)
    }).join(aggRdd, 10)
      .flatMap(t => {
        val row = t._2._1
        val partAggInfo = t._2._2
        val clickCategoryIds = partAggInfo.clickCategoryIds
        val payCategoryIds = row.getAs[String]("payCategoryIds")
        val orderCategoryIds = row.getAs[String]("orderCategoryIds")

        val cateActionList = ArrayBuffer[(String, CateSortKey)]()
        if (clickCategoryIds != null) {
          val clickCates = StringUtils.split(clickCategoryIds)
          for (item <- clickCates) {
            if (item.toInt != 0) {
              cateActionList.append((item, new CateSortKey(1, 0, 0)))
            }
          }
        }

        if (payCategoryIds != null) {
          val clickCates = StringUtils.split(payCategoryIds)
          for (item <- clickCates) {
            if (item.toInt != 0) {
              cateActionList.append((item, new CateSortKey(0, 1, 0)))
            }
          }
        }


        if (orderCategoryIds != null) {
          val clickCates = StringUtils.split(orderCategoryIds)
          for (item <- clickCates) {
            if (item.toInt != 0) {
              cateActionList.append((item, new CateSortKey(0, 0, 1)))
            }
          }
        }
        cateActionList
      }).groupByKey(10)
      .map(t => {
        val iter = t._2.iterator
        var clickCnt = 0L
        var payCnt = 0L
        var orderCnt = 0L
        while (iter.hasNext) {
          val cate: CateSortKey = iter.next()
          clickCnt = clickCnt + cate.orderCnt
          payCnt = payCnt + cate.payCnt
          orderCnt = orderCnt + cate.orderCnt
        }

        (new CateSortKey(clickCnt, payCnt, orderCnt), t._1)
      }).sortByKey(false).map(t => (t._2, t._1))

    val top10Cate = cateSortRdd.take(10)

    val topCates = top10Cate.map(t => {
      val cnts = t._2
      CategoryTop(t._1.toInt, taskId, cnts.clickCnt, cnts.payCnt, cnts.orderCnt)
    }).to[ArrayBuffer]

    Top10CategoryDao.insertCategories(topCates)
    topCates
  }


  def top10CategorySessionClick(top10Cates: ArrayBuffer[CategoryTop], aggRdd: RDD[(String, PartAggInfo)], taskId: Int) = {
    val topSession = aggRdd.flatMap(t => {
      val cateIds = t._2.clickCategoryIds

      val cateGroups = StringUtils.split(cateIds)
      val cateSessionCnt = ArrayBuffer[(String, (String, Long))]()

      val cateCnt = mutable.Map[String, Int]()

      for (item <- cateGroups) {
        val value = cateCnt.getOrElse(item, 0)
        cateCnt += item -> (value + 1)
      }

      for ((k, v) <- cateCnt) {
        cateSessionCnt += Tuple2(k, Tuple2(t._1, v))
      }

      cateSessionCnt
    }).filter(t => top10Cates.map(_.cateId).contains(t._1.toInt))
      .groupByKey(10) // 按照商品分组
      .map {
        case (cateId, sessionCnts) => {
          val arr = sessionCnts.toArray.sortBy(r => r._2).take(10)
          (cateId, arr)
        }
      }

    val clt = topSession.collect()

    val cateSessionTop10 = ArrayBuffer[Top10CategorySession]()

    for (elem <- clt) {
      for (arr <- elem._2) {
        val top10Session = Top10CategorySession(taskId, elem._1.toInt, arr._1, arr._2)
        cateSessionTop10 += top10Session
      }
    }

    Top10CategorySessionDao.insertTop10Sessions(cateSessionTop10)

  }


}
