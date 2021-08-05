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
object RDDPersist4 {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        sc.setCheckpointDir("cp")
        val rdd: RDD[String] = sc.makeRDD(List(("Hello Spark"), ("Hello scala")))

        val strings: RDD[String] = rdd.flatMap(_.split(" "))

        val wordOne: RDD[(String, Int)] = strings.map((_, 1))

        wordOne.checkpoint()

        //wordOne.persist(StorageLevel.DISK_ONLY)
        val wordCount: RDD[(String, Int)] = wordOne.reduceByKey(_ + _)
        //相当于重新执行
        wordCount.collect().foreach(println)
        println("+++++++++++++++++++++++++")
        val wordCount1: RDD[(String, Iterable[Int])] = wordOne.groupByKey()

        wordCount1.collect().foreach(println)

        sc.stop()
    }
}
