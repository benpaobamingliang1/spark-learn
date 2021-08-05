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
object ActionRDD4_CountByKey {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

        val map: collection.Map[Int, Long]
        = rdd.countByValue()

        println(map)

        val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3)))

        println(rdd1.countByKey())

    }

}
