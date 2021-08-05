package com.bupt.spark.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author gml
 * @date 2021/7/29 11:02
 * @version 1.0
 * @param
 * @return
 */
object ActionRDD6_Save {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3)), 2)

        // 保存成 Text 文件
        rdd.saveAsTextFile("output")
        // 序列化成对象保存到文件
        rdd.saveAsObjectFile("output1")
        // 保存成 Sequencefile 文件
        rdd.saveAsSequenceFile("output2")
        sc.stop()

    }

}
