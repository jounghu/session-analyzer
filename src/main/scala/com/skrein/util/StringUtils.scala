package com.skrein.util

import com.google.common.base.Joiner

import scala.collection.JavaConverters._
import scala.collection.mutable._

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/12/9 14:29
 * @since :1.8
 *
 */
object StringUtils {

  def isEmpty(str: Object) = {
    str == null || "" == str
  }

  def join(array: ArrayBuffer[String]): String = {
    if (array == null || array.size == 0) {
      return null
    }
    Joiner.on("|").join(array.asJava)
  }

  def split(str:String)={
    str.split("\\|").toBuffer
  }

  /**
   * 小时和分钟秒拼接0
   *
   * @param str
   * @return
   */
  def append0(str: String): String = {
    if (str.length == 1) {
      return "0" + str
    }
    str
  }

}
