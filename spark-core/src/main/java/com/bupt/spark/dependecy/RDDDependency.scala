package com.bupt.spark.dependecy

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author gml
 * @date 2021/7/29 20:17
 * @version 1.0
 * @param
 * @return
 */
object RDDDependency {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.textFile("data/1.txt")
        println(rdd.toDebugString)
        println("++++++++++++++++")
        val strings: RDD[String] = rdd.flatMap(_.split(" "))
        println(strings.toDebugString)
        println("++++++++++++++++")
        val wordOne: RDD[(String, Int)] = strings.map((_, 1))
        println(wordOne.toDebugString)
        println("++++++++++++++++")
        val wordCount: RDD[(String, Int)] = wordOne.reduceByKey(_ + _)
        println(wordCount.toDebugString)
        println("++++++++++++++++")
        val tuples: Array[(String, Int)] = wordCount.collect()
        tuples.foreach(println)

        sc.stop()
    }

}
