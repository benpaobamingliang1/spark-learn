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
object RDDTransaction32_Summary {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)
        val datas: RDD[String] = sc.textFile("data/agent.log")
        val summaryRDD: RDD[((String, String), Int)] = datas.map {
            line => {
                val strings: Array[String] = line.split(" ")
                ((strings(1), strings(4)), 1)
            }
        }
        val reduceRDD: RDD[((String, String), Int)] = summaryRDD.reduceByKey(_ + _)

        val summaryRDD1: RDD[(String, (String, Int))] = reduceRDD.map(
            t => {
                (t._1._1, (t._1._2, t._2))
            }
        )

        val groupByKey: RDD[(String, Iterable[(String, Int)])] = summaryRDD1.groupByKey()
        groupByKey.map {
            t => {
                t._2.toList.sortBy(d => d._2)(Ordering.Int.reverse).take(3)
            }
        }.collect().foreach(println)
        sc.stop()
    }

}
