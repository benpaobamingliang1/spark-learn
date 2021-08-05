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
object RDDTransaction14 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        //TODO 计算
        val rdd: RDD[String] = sc.textFile("data/apache.log")
        rdd.filter(
            line => {
                val datas: Array[String] = line.split(" ")
                val time: String = datas(3)

                time.startsWith("17/05/2015")
            }
        ).collect().foreach(println)

        sc.stop()
    }

}
