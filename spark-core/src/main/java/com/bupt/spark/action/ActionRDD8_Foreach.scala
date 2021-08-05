package com.bupt.spark.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author gml
 * @date 2021/7/29 11:02
 * @version 1.0
 * @param
 * @return
 */
object ActionRDD8_Foreach {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        var user = new User()
        rdd.foreach(
            num => {
                println("age = " + (user.age + num))
            }
        )
        sc.stop()
    }

    class User extends Serializable {
        var age: Int = 30
    }

}
