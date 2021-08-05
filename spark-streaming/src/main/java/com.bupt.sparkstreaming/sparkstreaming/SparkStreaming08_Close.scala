package com.bupt.sparkstreaming.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

/**
 * @author gml
 * @date 2021/8/2 11:31
 * @version 1.0
 * @param
 * @return
 */
object SparkStreaming08_Close {

    def main(args: Array[String]): Unit = {
        //TODO 创建环境对象

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        //第二个参数就是周期
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        //TODO 逻辑处理
        val datas1: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
        datas1.map((_, 1)).print()

        ssc.start()
        //TODO 关闭环境

        new Thread(new Runnable {
            override def run(): Unit = {
                //                while (true){
                //                    if(true){
                //                        val state: StreamingContextState = ssc.getState()
                //                        if (state == StreamingContextState.ACTIVE){
                //                            ssc.stop(true, true)
                //                        }
                //                    }
                //                    Thread.sleep(5000)
                //
                //                }
                Thread.sleep(5000)
                val state: StreamingContextState = ssc.getState()
                if (state == StreamingContextState.ACTIVE) {
                    ssc.stop(true, true)
                }

            }
        }).start()
        ssc.awaitTermination()

        //ssc.stop()
    }


}
