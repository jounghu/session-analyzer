package com.skrein.model

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/12/16 18:06
 * @since :1.8
 *
 */

class CateSortKey(clickCnt: Long, payCnt: Long, orderCnt: Long) extends Ordered[CateSortKey] {


  /**
   * 首先比较click，然后比较payCnt, 然后比较OrderCnt
   *
   * @param that
   * @return
   */
  override def compare(that: CateSortKey): Int = {

    -1

  }
}
