package com.bupt.framework.controller

import com.bupt.framework.service.WordCountService

/**
 * @author gml
 * @date 2021/7/30 21:38
 * @version 1.0
 * @param
 * @return
 */
class WordCountController {

    private val wordCountService = new WordCountService()

    //调度
    def dispatch() = {
        val array: Array[(String, Int)] = wordCountService.dataAnysis()
        array.foreach(println)
    }


}
