package com.skrein.util

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/12/9 14:08
 * @since :1.8
 *
 */
object DateUtil {


  val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  val datetimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  val datetimeHourFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH")

  /**
   * 比较date是否在start和end中间
   * pattern yyyy-MM-dd
   *
   * @param start 开始时间
   * @param end   结束时间
   * @param date  比较的时间
   */
  def in(start: String, end: String, date: String) = {
    val startDate = LocalDate.parse(start, dateFormat)
    val endDate = LocalDate.parse(end, dateFormat)
    val curDate = LocalDate.parse(date, dateFormat)

    val a = curDate.isBefore(endDate) || curDate.isEqual(endDate)
    val b = curDate.isAfter(startDate) || curDate.isEqual(startDate)
    a || b
  }

  def before(startDateTime: String, curDateTime: String): Boolean = {
    val start = LocalDateTime.parse(startDateTime, datetimeFormat)
    val cur = LocalDateTime.parse(curDateTime, datetimeFormat)
    cur.isBefore(start)
  }

  def after(endTime: String, actionTime: String): Boolean = {
    val end = LocalDateTime.parse(endTime, datetimeFormat)
    val action = LocalDateTime.parse(actionTime, datetimeFormat)
    action.isAfter(end)
  }

  /**
   * format startTime
   * yyyy-MM-dd HH:mm:ss -> yyyy-MM-dd_HH
   * @param startTime
   * @return
   */
  def format(startTime: String): String = {
    LocalDateTime.parse(startTime,datetimeFormat).format(datetimeHourFormat)
  }

  /**
   * 计算访问时长
   *
   * @param startTime
   * @param endTime
   * @return endTimeSecone - startTimeSecond
   */
  def cost(startTime: String, endTime: String): Long = {
    val start = getInstant(startTime).getEpochSecond
    val end = getInstant(endTime).getEpochSecond
    end - start
  }

  private def parseDateTime(dateTime: String): LocalDateTime = {
    LocalDateTime.parse(dateTime, datetimeFormat)
  }

  private def getInstant(datetime: String): Instant = {
    parseDateTime(datetime).atZone(ZoneId.systemDefault()).toInstant
  }

  def main(args: Array[String]): Unit = {
    val b = DateUtil.in("2018-11-11", "2018-11-13", "2018-11-11")
    print(b)
  }

}
