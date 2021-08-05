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
object RDD_Create {

    def main(args: Array[String]): Unit = {
        //TODO 准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sparkContext = new SparkContext(sparkConf)
        //TODO 创建RDD
        //从内存中创建RDD，转换为处理的数据流
        val seq: Seq[Int] = Seq[Int](1, 2, 3, 4)
        //并行
        //val rdd: RDD[Int] = sparkContext.parallelize(seq)
        val rdd: RDD[Int] = sparkContext.makeRDD(seq)
        rdd.collect().foreach(println)
        //TODO 关闭资源
        sparkContext.stop()
    }

}
