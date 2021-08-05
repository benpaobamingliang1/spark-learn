package com.bupt.framework.application

import com.bupt.framework.common.TApplication
import com.bupt.framework.controller.WordCountController

/**
 * @author gml
 * @date 2021/7/30 21:37
 * @version 1.0
 * @param
 * @return
 */
object WordCountApplication extends App with TApplication {

    // 启动应用程序
    start() {
        val controller = new WordCountController()
        controller.dispatch()
    }

}
