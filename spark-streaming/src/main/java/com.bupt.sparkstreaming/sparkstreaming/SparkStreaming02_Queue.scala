package com.bupt.sparkstreaming.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author gml
 * @date 2021/8/2 11:31
 * @version 1.0
 * @param
 * @return
 */
object SparkStreaming02_Queue {

    def main(args: Array[String]): Unit = {
        //TODO 创建环境对象
        /**
         * 需要传递两个参数
         */
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        //第二个参数就是周期
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        //TODO 逻辑处理
        /**
         * 获取端口数据
         */
        val queue = new mutable.Queue[RDD[Int]]()
        val inputStream: InputDStream[Int] = ssc.queueStream(queue, oneAtATime = false)
        //5.处理队列中的 RDD 数据
        val mappedStream = inputStream.map((_, 1))
        val reducedStream = mappedStream.reduceByKey(_ + _)
        //6.打印结果
        reducedStream.print()

        ssc.start()
        //TODO 关闭环境

        //8.循环创建并向 RDD 队列中放入 RDD
        for (i <- 1 to 5) {
            queue += ssc.sparkContext.makeRDD(1 to 300, 10)
            Thread.sleep(2000)
        }
        ssc.awaitTermination()
        //ssc.stop()
    }

}
