package com.skrein.model

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/12/10 10:55
 * @since :1.8
 *
 */

/**
 * 计算访问深度
 *
 * @param step_1_3
 * @param step_4_6
 * @param step_7_9
 * @param step_10_30
 * @param step_30_60
 * @param step_60
 */
class StepInterval(
                    var step_1_3: Long = 0,
                    var step_4_6: Long = 0,
                    var step_7_9: Long = 0,
                    var step_10_30: Long = 0,
                    var step_30_60: Long = 0,
                    var step_60: Long = 0
                  )extends Serializable {


  def +(stepInterval: StepInterval) = {
    this.step_1_3 += stepInterval.step_1_3
    this.step_4_6 += stepInterval.step_4_6
    this.step_7_9 += stepInterval.step_7_9
    this.step_10_30 += stepInterval.step_10_30
    this.step_30_60 += stepInterval.step_30_60
    this.step_60 += stepInterval.step_60

  }

  override def toString: String =
    "step_1_3=" + this.step_1_3 + "\n" +
      "step_4_6=" + this.step_4_6 + "\n" +
      "step_7_9=" + this.step_7_9 + "\n" +
      "step_10_30=" + this.step_10_30 + "\n" +
      "step_30_60=" + this.step_30_60 + "\n" +
      "step_30_60=" + this.step_30_60 + "\n" +
      "step_60=" + this.step_60 + "\n"

}


/**
 * 花费时长
 *
 * @param cost_1_3
 * @param cost_4_6
 * @param cost_7_9
 * @param cost_10_30
 * @param cost_30_60
 * @param cost_1m_3m
 * @param cost_3m_10m
 * @param cost_10m_30m
 */
class CostInterval(var cost_1_3: Long = 0,
                   var cost_4_6: Long = 0,
                   var cost_7_9: Long = 0,
                   var cost_10_30: Long = 0,
                   var cost_30_60: Long = 0,
                   var cost_1m_3m: Long = 0,
                   var cost_3m_10m: Long = 0,
                   var cost_10m_30m: Long = 0
                  ) extends Serializable {



  def +(costInterval: CostInterval):Unit = {
    this.cost_10m_30m += costInterval.cost_10m_30m
    this.cost_3m_10m += costInterval.cost_3m_10m
    this.cost_1m_3m += costInterval.cost_1m_3m
    this.cost_30_60 += costInterval.cost_30_60
    this.cost_10_30 += costInterval.cost_10_30
    this.cost_7_9 += costInterval.cost_7_9
    this.cost_4_6 += costInterval.cost_4_6
    this.cost_1_3 += costInterval.cost_1_3
  }

  override def toString: String =
    "cost_10m_30m=" + this.cost_10m_30m + "\n" +
      "cost_3m_10m=" + this.cost_3m_10m + "\n" +
      "cost_1m_3m=" + this.cost_1m_3m + "\n" +
      "cost_30_60=" + this.cost_30_60 + "\n" +
      "cost_10_30=" + this.cost_10_30 + "\n" +
      "cost_7_9=" + this.cost_7_9 + "\n" +
      "cost_4_6=" + this.cost_4_6 + "\n" +
      "cost_1_3=" + this.cost_1_3 + "\n"
}




