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
object RDDTransaction7 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        //TODO 计算
        val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4)))
        val flatMap: RDD[Int] = rdd.flatMap(
            list => {
                list
            }
        )
        flatMap.collect().foreach(println)
        sc.stop()
    }

}
