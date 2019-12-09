package com.skrein.core

import org.apache.spark.AccumulatorParam

import scala.collection.mutable.HashMap

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/12/9 17:09
 * @since :1.8
 *
 */
class SessionAccumulator extends AccumulatorParam[HashMap[String, Long]] {
  override def addInPlace(r1: HashMap[String, Long], r2: HashMap[String, Long]): HashMap[String, Long] = {
    for (key <- r1.keySet) {
      val acc = r1.getOrElse(key, 0L) + r2.getOrElse(key, 0L)
      r1.put(key, acc)
    }
    r1
  }

  override def zero(initialValue: HashMap[String, Long]): HashMap[String, Long] = {
    initialValue
  }
}
