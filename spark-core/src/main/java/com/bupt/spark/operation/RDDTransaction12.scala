package com.bupt.spark.operation

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author gml
 * @date 2021/7/27 21:37
 * @version 1.0
 * @param
 * @return
 */
object RDDTransaction12 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        //TODO 计算
        val rdd: RDD[String] = sc.textFile("data/apache.log")

        val timeRDD: RDD[(Int, Iterable[(Int, Int)])] = rdd.map {
            line => {
                val strings: Array[String] = line.split(" ")
                val time: String = strings(3)
                //                time.substring()
                val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
                val date: Date = sdf.parse(time)
                val hour: Int = date.getHours
                (hour, 1)
            }
        }.groupBy(hour => hour._1)
        val timeRDD1: RDD[(Int, Int)] = timeRDD.map {
            time => {
                (time._1, time._2.size)
            }
        }.sortBy(t => t._1)
        timeRDD1.collect().foreach(println)

        sc.stop()
    }

}
