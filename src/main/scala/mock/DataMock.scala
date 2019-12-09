package mock

import java.time.LocalDate
import java.util
import java.util.{Random, UUID}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DataTypes

import scala.collection.mutable.ArrayBuffer

/**
 * data mock app
 */
case class Session(
                    date: String, userId: Long, sessionId: String, pageId: Long,
                    actionTime: String, searchKeyword: String,
                    clickCategoryId: Long = 0L, clickProductId: Long = 0L,
                    orderCategoryIds: String, orderProductIds: String,
                    payCategoryIds: String, payProductIds: String
                  )

case class User(
                 userId: Int, userName: String, name: String, age: Int,
                 profession: String, city: String, sex: String
               )

object DataMock {

  def mockUser(sqlContext: SQLContext): Unit = {
    val sexes = Array("male", "female")
    val random = new Random
    val userList: ArrayBuffer[User] = ArrayBuffer()
    for (i <- 1 to 100) {
      val userId = i
      val userName = s"user${i}"
      val name = s"name${i}"
      val age = random.nextInt(60);
      val professional = s"professional${random.nextInt(100)}"
      val city = "city" + random.nextInt(100)
      val sex = sexes(random.nextInt(sexes.size))
      userList += User(userId, userName, name, age, professional, city, sex)
    }
    val rdd = sqlContext.sparkContext.parallelize(userList)
    import sqlContext.implicits._
    val userDF = rdd.toDF()
    userDF.registerTempTable("user_info")

    val user1 = sqlContext.sql("select * from user_info limit 1")
    user1.show()
  }

  def mockSession(sqlContext: SQLContext) = {
    val searchKeywords = Array("火锅", "蛋糕", "重庆辣子鸡", "重庆小面",
      "甲普甲普", "辛辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉")
    val actions = Array("search", "click", "order", "pay")
    val random = new Random()
    val sessionList = ArrayBuffer[Session]()
    val today = LocalDate.now().toString

    for (i <- 1 to 100) {
      // user
      val userId = random.nextInt(100)

      for (j <- 1 to 10) {
        // session
        val sessionId = UUID.randomUUID().toString.replace("-", "")
        val baseTime = today + " " + random.nextInt(24)

        for (k <- 1 to 100) {
          val pageId = random.nextInt(10)
          val actionTime = baseTime + ":" + random.nextInt(60) + ":" + random.nextInt(60)
          var searchKeyword: String = null
          var clickCategoryId: Long = 0L
          var clickProductId: Long = 0L
          var orderCategoryIds: String = null
          var orderProductIds: String = null
          var payCategoryIds: String = null
          var payProductIds: String = null

          val action = actions(random.nextInt(4))
          action match {
            case "search" => {
              searchKeyword = searchKeywords(random.nextInt(searchKeywords.size))
            }
            case "click" => {
              clickCategoryId = random.nextInt(100).toLong
              clickProductId = random.nextInt(100).toLong
            }
            case "order" => {
              orderCategoryIds = random.nextInt(100).toString
              orderProductIds = random.nextInt(100).toString
            }
            case "pay" => {
              payCategoryIds = random.nextInt(100).toString
              payProductIds = random.nextInt(100).toString
            }
            case _ => {
              throw new RuntimeException(s"Unsupport action ${action}")
            }
          }

          val session = Session(
            today, userId,
            sessionId, pageId,
            actionTime, searchKeyword,
            clickCategoryId, clickProductId,
            orderCategoryIds, orderProductIds,
            payCategoryIds, payProductIds
          )
          sessionList += session
        }
      }
    }
    val rdd = sqlContext.sparkContext.parallelize(sessionList)
    import sqlContext.implicits._
    val df = rdd.toDF()
    df.registerTempTable("user_action")

    val result = sqlContext.sql("select * from user_action limit 1")
    result.show()
  }
}
