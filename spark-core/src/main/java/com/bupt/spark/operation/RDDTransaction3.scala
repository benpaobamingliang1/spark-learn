package com.bupt.spark.operation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author gml
 * @date 2021/7/27 21:37
 * @version 1.0
 * @param
 * @return
 */
object RDDTransaction3 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        //TODO 计算
        var rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

        def mapFunction(num: Int) = {
            num * 2
        }

        val mapRDD: RDD[Int] = rdd.map(
            num => {
                println("num = " + num)
                num
            }
        )
        val mapRDD1: RDD[Int] = mapRDD.map(
            num => {
                println("+++++++++++ " + num)
                num
            }
        )
        mapRDD1.collect()
        sc.stop()
    }

}
