package com.bupt.project

import java.text.SimpleDateFormat

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming12_request2 {

    def main(args: Array[String]): Unit = {

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
                    "hadoop102:9092,hadoop103:9092,hadoop104:9092",
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

        val adClickData = kafkaDS.map(
            data => {
                val line: String = data.value()
                val strings: Array[String] = line.split(" ")
                AdClickData(strings(0), strings(1)
                    , strings(2), strings(3), strings(4))
            }
        )

        val reduceDS: DStream[((String, String, String, String), Int)] = adClickData.map(
            data => {
                val sdf = new SimpleDateFormat("yyyy-MM-dd")
                val day = sdf.format(new java.util.Date(data.ts.toLong))
                val area = data.area
                val city = data.city
                val ad = data.ad
                ((day, area, city, ad), 1)
            }
        ).reduceByKey(_ + _)
        reduceDS.print()
        //        reduceDS.foreachRDD(
        //            rdd =>{
        //                rdd.foreachPartition(
        //                    iter =>{
        //                        val conn = JDBCUtil.getConnection
        //                        val pstat = conn.prepareStatement(
        //                            """
        //                              | insert into area_city_ad_count ( dt, area, city, adid, count )
        //                              | values ( ?, ?, ?, ?, ? )
        //                              | on DUPLICATE KEY
        //                              | UPDATE count = count + ?
        //                            """.stripMargin)
        //                        iter.foreach{
        //                            case((day, area, city, ad), sum) =>{
        //                                pstat.setString(1,day )
        //                                pstat.setString(2,area )
        //                                pstat.setString(3, city)
        //                                pstat.setString(4, ad)
        //                                pstat.setInt(5, sum)
        //                                pstat.setInt(6,sum )
        //                                pstat.executeUpdate()
        //                            }
        //                        }
        //                        pstat.close()
        //                        conn.close()
        //                    }
        //                )
        //            }
        //        )

        ssc.start()
        ssc.awaitTermination()
    }

    // 广告点击数据
    case class AdClickData(ts: String, area: String, city: String, user: String, ad: String)

}
