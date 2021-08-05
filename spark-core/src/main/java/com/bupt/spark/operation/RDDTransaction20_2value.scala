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
object RDDTransaction20_2value {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        //TODO 计算
        val rdd1 = sc.makeRDD(List(4, 5, 6))
        val rdd2 = sc.makeRDD(List(5, 6, 7))
        //TODO 交集
        val rdd3: RDD[Int] = rdd1.intersection(rdd2)
        println(rdd3.collect().mkString(","))
        //TODO 并集
        val rdd4: RDD[Int] = rdd1.union(rdd2)
        println(rdd4.collect().mkString(","))
        //TODO 差集
        val rdd5: RDD[Int] = rdd1.subtract(rdd2)
        println(rdd5.collect().mkString(","))
        //TODO 交集
        val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
        println(rdd6.collect().mkString(","))
        sc.stop()
    }

}
