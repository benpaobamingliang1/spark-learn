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
object Requeirment_1_1 {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.textFile(
            "data/user_visit_action.txt")

        rdd.cache()
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

        val rdd1 = clickRDD.map(
            t => {
                (t._1, (t._2, 0, 0))
            }
        )
        val rdd2 = orderCountRDD.map(
            t => {
                (t._1, (0, t._2, 0))
            }
        )
        val rdd3 = payCountRDD.map(
            t => {
                (t._1, (0, 0, t._2))
            }
        )
        val sourceRDD: RDD[(String, (Int, Int, Int))] = rdd1.union(rdd2).union(rdd3)

        val anysisRDD = sourceRDD.reduceByKey(
            (t1, t2) => {
                (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
            }
        )
        val resultRDD: Array[(String, (Int, Int, Int))] = anysisRDD.sortBy(_._2, false).take(10)

        resultRDD.foreach(println)

    }

}
