package com.skrein.model

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/12/16 18:06
 * @since :1.8
 *
 */

class CateSortKey(val clickCnt: Long, val payCnt: Long, val orderCnt: Long) extends Ordered[CateSortKey] with Serializable {



  /**
   * 首先比较click，然后比较payCnt, 然后比较OrderCnt
   *
   * @param that
   * @return
   */
  override def compare(that: CateSortKey): Int = {
    val clickCompare = clickCnt.compareTo(that.clickCnt)
    if (clickCompare == 0) {
      val payCompare =  payCnt.compareTo(that.payCnt)
      if (payCompare == 0) {
        orderCnt.compareTo(that.orderCnt)
      } else {
        payCompare
      }
    } else {
      clickCompare
    }
  }


  override def toString = s"CateSortKey(_clickCnt=${clickCnt}, _payCnt=${payCnt}, _orderCnt=${orderCnt})"
}

