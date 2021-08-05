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
object RDDTransaction10 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        //TODO 计算
        val rdd = sc.makeRDD((List(1, 2, 3, 4, 5, 6)), 2)
        val glomRDD: RDD[Array[Int]] = rdd.glom()
        val maxRDD: RDD[Int] = glomRDD.map {
            array => {
                array.max
            }
        }

        println(maxRDD.collect().sum)
        sc.stop()
    }

}
