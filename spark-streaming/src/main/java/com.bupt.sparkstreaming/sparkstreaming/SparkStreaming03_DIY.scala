package com.bupt.sparkstreaming.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

/**
 * @author gml
 * @date 2021/8/2 11:31
 * @version 1.0
 * @param
 * @return
 */
object SparkStreaming03_DIY {

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

        val messageID: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver)
        messageID.print()
        ssc.start()
        //TODO 关闭环境
        ssc.awaitTermination()
        //ssc.stop()
    }

    class MyReceiver extends Receiver[String](
        StorageLevel.MEMORY_ONLY) {
        private var flag = true

        override def onStart(): Unit = {
            new Thread(
                new Runnable {
                    override def run(): Unit = {
                        while (flag) {

                            val message = "采集的数据：" + new Random().nextInt(10).toString
                            store(message)
                            Thread.sleep(500)
                        }
                    }
                }
            ).start()
        }

        override def onStop(): Unit = {
            flag = false
        }
    }

}
