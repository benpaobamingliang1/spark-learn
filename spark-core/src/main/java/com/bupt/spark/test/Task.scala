package com.bupt.spark.test

/**
 * @author gml
 * @date 2021/7/27 15:29
 * @version 1.0
 * @param
 * @return
 */
class Task extends Serializable {

    val list = List(1, 2, 3, 4)

    //计算
    def compute = {
        list.map(logic)
    }

    val logic = (numL: Int) => {
        numL * 2
    }
}
