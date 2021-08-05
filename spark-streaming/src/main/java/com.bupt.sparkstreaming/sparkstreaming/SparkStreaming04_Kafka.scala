package com.bupt.sparkstreaming.sparkstreaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author gml
 * @date 2021/8/2 11:31
 * @version 1.0
 * @param
 * @return
 */
object SparkStreaming04_Kafka {

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
        //3.定义 Kafka 参数
        val kafkaPara: Map[String, Object] = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ->
                    "hadoop102:9092,hadoop102:9092,hadoop102:9092",
            ConsumerConfig.GROUP_ID_CONFIG -> "bupt",
            "key.deserializer" ->
                    "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" ->
                    "org.apache.kafka.common.serialization.StringDeserializer"
        )

        val kafkaDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Set("bupt"), kafkaPara)
        )

        kafkaDS.map(_.value()).print()

        ssc.start()
        //TODO 关闭环境
        ssc.awaitTermination()
        //ssc.stop()
    }


}
