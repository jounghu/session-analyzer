package com.skrein.util

object NumberUtil {

  def twoPrecision(n: Long, cnt: Long): Double = {
    if(cnt==0){
      return 0.0
    }
    BigDecimal(n.toDouble / cnt.toDouble).setScale(2, BigDecimal.RoundingMode.FLOOR).toDouble
  }

}
