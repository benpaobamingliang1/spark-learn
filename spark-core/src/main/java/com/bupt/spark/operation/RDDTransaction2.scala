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
object RDDTransaction2 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        //TODO 计算
        var rdd = sc.textFile("data/apache.log")

        val mapRDD: RDD[String] = rdd.map(
            line => {
                val strings: Array[String] = line.split(" ")
                strings(6)
            }
        )
        mapRDD.collect().foreach(println)
        sc.stop()
    }

}
