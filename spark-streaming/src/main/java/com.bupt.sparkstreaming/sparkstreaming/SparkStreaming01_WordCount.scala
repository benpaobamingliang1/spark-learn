package com.bupt.sparkstreaming.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author gml
 * @date 2021/8/2 11:31
 * @version 1.0
 * @param
 * @return
 */
object SparkStreaming01_WordCount {

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
        val lines: ReceiverInputDStream[String]
        = ssc.socketTextStream("localhost", 9999)

        val words: DStream[String] = lines.flatMap(_.split(" "))

        val wordToOne: DStream[(String, Int)] = words.map((_, 1))

        val wordToCount: DStream[(String, Int)] = wordToOne.reduceByKey(_ + _)

        wordToCount.print()
        ssc.start()
        //TODO 关闭环境
        ssc.awaitTermination()
        //ssc.stop()
    }

}
