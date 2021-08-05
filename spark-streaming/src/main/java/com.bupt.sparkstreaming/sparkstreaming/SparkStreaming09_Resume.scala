package com.bupt.sparkstreaming.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author gml
 * @date 2021/8/2 11:31
 * @version 1.0
 * @param
 * @return
 */
object SparkStreaming09_Resume {

    def main(args: Array[String]): Unit = {
        //TODO 创建环境对象
        val ssc: StreamingContext = StreamingContext.getActiveOrCreate("cp", ()
        => {
            val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
            //第二个参数就是周期
            val ssc = new StreamingContext(sparkConf, Seconds(3))

            //TODO 逻辑处理
            val datas1: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
            datas1.map((_, 1)).print()
            ssc
        })

        ssc.checkpoint("cp")

        ssc.start()
        ssc.awaitTermination()

        //ssc.stop()
    }


}
