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
object SparkStreaming05_State {

    def main(args: Array[String]): Unit = {
        //TODO 创建环境对象

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        //第二个参数就是周期
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        ssc.checkpoint("cp")
        //TODO 逻辑处理
        val datas: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

        /**
         * val result: DStream[(String, Int)] =
         *             datas.map((_, 1)).reduceByKey(
         * _+_)
         */
        //两个参数，一个key,一个value
        val result: DStream[(String, Int)] =
        datas.map((_, 1)).updateStateByKey(
            (seq: Seq[Int], opt: Option[Int]) => {
                val newCount = opt.getOrElse(0) + seq.sum
                Option(newCount)
            }
        )

        result.print()
        ssc.start()
        //TODO 关闭环境
        ssc.awaitTermination()
        //ssc.stop()
    }


}
