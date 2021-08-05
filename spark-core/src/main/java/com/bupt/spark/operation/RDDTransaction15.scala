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
object RDDTransaction15 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        //TODO 计算
        val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 0))
        // 第一个参数：抽取的数据是否放回，false：不放回
        // 第二个参数：抽取的几率，范围在[0,1]之间,0：全不取；1：全取；
        // 第三个参数：随机数种子
        val sampleRDD: RDD[Int] = rdd.sample(
            false,
            0.6,
            1
        )
        sampleRDD.collect().foreach(println)
        sc.stop()
    }

}
