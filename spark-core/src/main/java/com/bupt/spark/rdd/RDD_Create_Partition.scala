package com.bupt.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author gml
 * @date 2021/7/27 18:24
 * @version 1.0
 * @param
 * @return
 */
object RDD_Create_Partition {

    def main(args: Array[String]): Unit = {
        //TODO 准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sparkContext = new SparkContext(sparkConf)
        //TODO 创建RDD
        val rdd: RDD[Int] = sparkContext.makeRDD(
            List(1, 2, 3, 4)
        )
        rdd.saveAsTextFile("output")
        rdd.collect().foreach(println)
        //TODO 关闭资源
        sparkContext.stop()
    }

}
