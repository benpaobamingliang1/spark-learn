package com.bupt.spark.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author gml
 * @date 2021/7/29 20:55
 * @version 1.0
 * @param
 * @return
 */
object RDDPersist {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.makeRDD(List(("Hello Spark"), ("Hello scala")))

        val strings: RDD[String] = rdd.flatMap(_.split(" "))

        val wordOne: RDD[(String, Int)] = strings.map((_, 1))

        val wordCount: RDD[(String, Int)] = wordOne.reduceByKey(_ + _)

        wordCount.collect().foreach(println)
        println("+++++++++++++++++++++++++")

        val rdd1: RDD[String] = sc.makeRDD(List(("Hello Spark"), ("Hello scala")))

        val strings1: RDD[String] = rdd1.flatMap(_.split(" "))

        val wordOne1: RDD[(String, Int)] = strings1.map((_, 1))

        val wordCount1: RDD[(String, Iterable[Int])] = wordOne1.groupByKey()

        wordCount1.collect().foreach(println)


        sc.stop()
    }
}
