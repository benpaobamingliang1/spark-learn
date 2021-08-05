package com.bupt.sparkacc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author gml
 * @date 2021/7/29 22:42
 * @version 1.0
 * @param
 * @return
 */
object ACC02_Problem {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        val sum1: LongAccumulator = sc.longAccumulator("sum")

        val value: RDD[Int] = rdd.map(
            num => {
                sum1.add(num)
                num
            }
        )
        value.collect()
        println(sum1.value)

        sc.stop()

    }
}
