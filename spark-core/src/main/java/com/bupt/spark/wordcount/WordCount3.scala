package com.bupt.spark.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author gml
 * @date 2021/7/26 11:52
 * @version 1.0
 * @param
 * @return
 */
object WordCount3 {

    def main(args: Array[String]): Unit = {

        //Application
        //TODO 建立依赖关系
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)
        //TODO 执行业务
        val line: RDD[String] = sc.textFile("data")
        //分组
        val words: RDD[String] = line.flatMap {
            _.split(" ")
        }
        //转换
        val wordToOne: RDD[(String, Int)] = words.map((_, 1))

        val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)

        val array: Array[(String, Int)] = wordToCount.collect()
        array.foreach(println)
        sc.stop()
    }

}
