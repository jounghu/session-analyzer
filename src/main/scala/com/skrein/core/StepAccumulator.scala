package com.skrein.core

import com.skrein.model.{CostInterval, StepInterval}
import org.apache.spark.AccumulatorParam


/**
 *
 *
 * @author :hujiansong  
 * @date :2019/12/9 17:09
 * @since :1.8
 *
 */
class StepAccumulator extends AccumulatorParam[StepInterval] {
  override def addInPlace(r1: StepInterval, r2: StepInterval): StepInterval = {
    r1 + r2
    r1
  }

  override def zero(initialValue: StepInterval): StepInterval = {
    initialValue
  }
}
