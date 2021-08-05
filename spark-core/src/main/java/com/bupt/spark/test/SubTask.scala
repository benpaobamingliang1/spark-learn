package com.bupt.spark.test

/**
 * @author gml
 * @date 2021/7/27 15:53
 * @version 1.0
 * @param
 * @return
 */
class SubTask extends Serializable {

    var list: List[Int] = _
    var logic: (Int) => Int = _

    //计算
    def compute = {
        list.map(logic)
    }

}
