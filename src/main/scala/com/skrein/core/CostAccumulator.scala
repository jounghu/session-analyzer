package com.skrein.core

import com.skrein.model.CostInterval
import org.apache.spark.AccumulatorParam


/**
 *
 *
 * @author :hujiansong  
 * @date :2019/12/9 17:09
 * @since :1.8
 *
 */
class CostAccumulator extends AccumulatorParam[CostInterval] {
  override def addInPlace(r1: CostInterval, r2: CostInterval): CostInterval = {
    r1 + r2
    r1
  }

  override def zero(initialValue: CostInterval): CostInterval = {
    initialValue
  }
}
