package com.bupt.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author gml
 * @date 2021/7/27 17:36
 * @version 1.0
 * @param
 * @return
 */
object RDD_Create2 {

    def main(args: Array[String]): Unit = {
        //TODO 准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sparkContext = new SparkContext(sparkConf)
        //TODO 创建RDD
        //文件中创建RDD，转换为处理的数据流
        //       val rdd: RDD[String] = sparkContext.textFile("data/1.txt")
        val rdd: RDD[(String, String)] = sparkContext.wholeTextFiles("data")

        rdd.collect().foreach(println)
        //TODO 关闭资源
        sparkContext.stop()
    }

}
