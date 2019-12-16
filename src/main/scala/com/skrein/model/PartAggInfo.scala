package com.skrein.model

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/12/9 14:59
 * @since :1.8
 *
 */
case class PartAggInfo(
                        sessionId: String = null, searchKeywords: String = null,
                        clickCategoryIds: String = null, visitCost: Long = 0L, stepLength: Long = 0L,
                        var age: Int = 0, var professional: String = null, var city: String = null, var sex: String = null,
                        startTime: String = null
                      )