package com.bupt.Project.SparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayOps

/**
 * @author gml
 * @date 2021/7/30 10:57
 * @version 1.0
 * @param
 * @return
 */
object Requeirment {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.textFile(
            "data/user_visit_action.txt")

        /**
         * 1.统计品类的点击数量
         * 2.统计品类的下单数量
         * 3.统计品类的支付数量
         * 4.将品类进行分类，并且取前十名
         */

        val realizeRDD: RDD[String] = rdd.filter(
            t => {
                val strings: Array[String] = t.split("_")
                strings(6) != "-1"
            }
        )
        val clickRDD: RDD[(String, Int)] = realizeRDD.map(
            t => {
                val strings: Array[String] = t.split("_")
                ((strings(6)), 1)
            }
        ).reduceByKey(_ + _)


        val orderRDD: RDD[String] = rdd.filter(
            t => {
                val strings: Array[String] = t.split("_")
                strings(8) != "null"
            }
        )
        val orderCountRDD: RDD[(String, Int)] = orderRDD.flatMap(
            t => {
                val strings: Array[String] = t.split("_")
                val data = strings(8)
                val id: ArrayOps.ofRef[String] = data.split(",")
                id.map((_, 1))
            }
        ).reduceByKey(_ + _)


        val payRDD: RDD[String] = rdd.filter(
            t => {
                val strings: Array[String] = t.split("_")
                strings(10) != "null"
            }
        )
        val payCountRDD: RDD[(String, Int)] = payRDD.flatMap(
            t => {
                val strings: Array[String] = t.split("_")
                val data = strings(10)
                val id: ArrayOps.ofRef[String] = data.split(",")
                id.map((_, 1))
            }
        ).reduceByKey(_ + _)

        val coGroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickRDD.cogroup(orderCountRDD, payCountRDD)

        val anysisRDD: RDD[(String, (Int, Int, Int))] = coGroupRDD.mapValues({
            t => {
                var clickCount = 0
                if (t._1.iterator.hasNext) {
                    clickCount = t._1.iterator.next()
                }
                var orderCount = 0
                if (t._2.iterator.hasNext) {
                    orderCount = t._2.iterator.next()
                }
                var payCount = 0
                if (t._3.iterator.hasNext) {
                    payCount = t._3.iterator.next()
                }
                (clickCount, orderCount, payCount)
            }
        })
        val resultRDD: Array[(String, (Int, Int, Int))] = anysisRDD.sortBy(_._2, false).take(10)

        resultRDD.foreach(println)

    }

}
