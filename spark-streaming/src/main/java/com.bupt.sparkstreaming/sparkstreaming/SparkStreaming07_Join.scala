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
object SparkStreaming07_Join {

    def main(args: Array[String]): Unit = {
        //TODO 创建环境对象

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        //第二个参数就是周期
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        //TODO 逻辑处理
        val datas1: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
        val wordToOne: DStream[(String, Int)] = datas1.map((_, 1))

        //窗口的范围应该是采集周期的整数倍
        val windowDS: DStream[(String, Int)] = wordToOne.window(Seconds(6), Seconds(6))
        val result: DStream[(String, Int)] = windowDS.reduceByKey(_ + _)

        result.print()


        result.print()


        ssc.start()
        //TODO 关闭环境
        ssc.awaitTermination()
        //ssc.stop()
    }


}
