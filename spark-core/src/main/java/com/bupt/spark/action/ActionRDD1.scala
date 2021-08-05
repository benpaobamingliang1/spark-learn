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
object ActionRDD1 {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))


        val i: Int = rdd.reduce(_ + _)
        println(i)
        val ints: Array[Int] = rdd.collect()
        ints.foreach(println)
        println(ints.mkString(","))

        println(rdd.count())
        println(rdd.first())
        println(rdd.take(2).foreach(println))

        val rdd1: RDD[Int] = sc.makeRDD(List(1, 3, 2, 4))

        rdd1.takeOrdered(3).foreach(println)
    }

}
