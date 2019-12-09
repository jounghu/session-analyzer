package com.skrein.util

import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/12/9 17:20
 * @since :1.8
 *
 */
class JsonParse(jsonStr: String) {

  val jObject: JValue = parse(jsonStr)


  def getField(key: String) = {
    (jObject \ key).values
  }

}

