package com.bupt.spark.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author gml
 * @date 2021/7/29 22:27
 * @version 1.0
 * @param
 * @return
 */
object SaveLoad2 {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.textFile("output")
        println(rdd.collect().mkString(","))

        val rdd1: RDD[(String, Int)] = sc.objectFile[(String, Int)]("output1")
        println(rdd1.collect().mkString(","))

        val rdd2: RDD[(String, Int)] = sc.sequenceFile[String, Int]("output2")
        println(rdd2.collect().mkString(","))
        sc.stop()
    }

}
