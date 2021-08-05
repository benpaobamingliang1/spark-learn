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
object WordCount2 {

    def main(args: Array[String]): Unit = {

        //Application
        //TODO 建立依赖关系
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)
        //TODO 执行业务
        val line: RDD[String] = sc.textFile("data")
        //分组
        val words: RDD[String] = line.flatMap(_.split(" "))
        //转换
        val wordToOne: RDD[(String, Int)] = words.map {
            word => (word, 1)
        }

        val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy { t => t._1 }

        //转换为数量
        val wordToCount = wordGroup.map {
            case (word, list) => {
                list.reduce(
                    (t1, t2) => {
                        (t1._1, t1._2 + t2._2)
                    }
                )
            }
        }

        val array: Array[(String, Int)] = wordToCount.collect()
        array.foreach(println)
        //TODO 关闭连接
        sc.stop()

        val string = "Hello world"
        val strings: Array[String] = string.split(" ")
        val test: Array[(String, Int)] = strings.map(
            word => (word, 1)
        )
        test.foreach(println)
    }

}
