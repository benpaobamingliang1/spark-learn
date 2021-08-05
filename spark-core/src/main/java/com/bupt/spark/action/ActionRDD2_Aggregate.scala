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
object ActionRDD2_Aggregate {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))


        println(rdd.aggregate(0)(_ + _, _ + _))


    }

}
