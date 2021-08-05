package com.bupt.project

import java.sql.ResultSet
import java.text.SimpleDateFormat

import com.bupt.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
 * @author gml
 * @date 2021/8/2 11:31
 * @version 1.0
 * @param
 * @return
 */
object SparkStreaming11_KafkaConsumer {

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
        //TODO 周期性获取黑名单
        val ds = adClickData.transform(
            rdd => {
                // TODO 通过JDBC周期性获取黑名单数据
                val blackList = ListBuffer[String]()

                val conn = JDBCUtil.getConnection
                val pstat = conn.prepareStatement("select userid from black_list")

                val rs: ResultSet = pstat.executeQuery()
                while (rs.next()) {
                    blackList.append(rs.getString(1))
                }

                rs.close()
                pstat.close()
                conn.close()

                // TODO 判断点击用户是否在黑名单中
                val filterRDD = rdd.filter(
                    data => {
                        !blackList.contains(data.user)
                    }
                )

                // TODO 如果用户不在黑名单中，那么进行统计数量（每个采集周期）
                filterRDD.map(
                    data => {
                        val sdf = new SimpleDateFormat("yyyy-MM-dd")
                        val day = sdf.format(new java.util.Date(data.ts.toLong))
                        val user = data.user
                        val ad = data.ad

                        ((day, user, ad), 1) // (word, count)
                    }
                ).reduceByKey(_ + _)
            }
        )

        ds.foreachRDD(
            rdd => {
                rdd.foreach {
                    case ((day, user, ad), count) => {
                        println(s"${day} ${user} ${ad} ${count}")
                        if (count >= 30) {
                            // TODO 如果统计数量超过点击阈值(30)，那么将用户拉入到黑名单
                            val conn = JDBCUtil.getConnection
                            val pstat = conn.prepareStatement(
                                """
                                  |insert into black_list (userid) values (?)
                                  |on DUPLICATE KEY
                                  |UPDATE userid = ?
                                """.stripMargin)
                            pstat.setString(1, user)
                            pstat.setString(2, user)
                            pstat.executeUpdate()
                            pstat.close()
                            conn.close()
                        } else {
                            // TODO 如果没有超过阈值，那么需要将当天的广告点击数量进行更新。
                            val conn = JDBCUtil.getConnection
                            val pstat = conn.prepareStatement(
                                """
                                  | select
                                  |     *
                                  | from user_ad_count
                                  | where dt = ? and userid = ? and adid = ?
                                """.stripMargin)

                            pstat.setString(1, day)
                            pstat.setString(2, user)
                            pstat.setString(3, ad)
                            val rs = pstat.executeQuery()
                            // 查询统计表数据
                            if (rs.next()) {
                                // 如果存在数据，那么更新
                                val pstat1 = conn.prepareStatement(
                                    """
                                      | update user_ad_count
                                      | set count = count + ?
                                      | where dt = ? and userid = ? and adid = ?
                                    """.stripMargin)
                                pstat1.setInt(1, count)
                                pstat1.setString(2, day)
                                pstat1.setString(3, user)
                                pstat1.setString(4, ad)
                                pstat1.executeUpdate()
                                pstat1.close()
                                // TODO 判断更新后的点击数据是否超过阈值，如果超过，那么将用户拉入到黑名单。
                                val pstat2 = conn.prepareStatement(
                                    """
                                      |select
                                      |    *
                                      |from user_ad_count
                                      |where dt = ? and userid = ? and adid = ? and count >= 30
                                    """.stripMargin)
                                pstat2.setString(1, day)
                                pstat2.setString(2, user)
                                pstat2.setString(3, ad)
                                val rs2 = pstat2.executeQuery()
                                if (rs2.next()) {
                                    val pstat3 = conn.prepareStatement(
                                        """
                                          |insert into black_list (userid) values (?)
                                          |on DUPLICATE KEY
                                          |UPDATE userid = ?
                                        """.stripMargin)
                                    pstat3.setString(1, user)
                                    pstat3.setString(2, user)
                                    pstat3.executeUpdate()
                                    pstat3.close()
                                }

                                rs2.close()
                                pstat2.close()
                            } else {
                                // 如果不存在数据，那么新增
                                val pstat1 = conn.prepareStatement(
                                    """
                                      | insert into user_ad_count ( dt, userid, adid, count ) values ( ?, ?, ?, ? )
                                    """.stripMargin)

                                pstat1.setString(1, day)
                                pstat1.setString(2, user)
                                pstat1.setString(3, ad)
                                pstat1.setInt(4, count)
                                pstat1.executeUpdate()
                                pstat1.close()
                            }

                            rs.close()
                            pstat.close()
                            conn.close()
                        }
                    }
                }
            }
        )

        ssc.start()
        //TODO 关闭环境
        ssc.awaitTermination()
        //ssc.stop()
    }

    case class AdClickData(ts: String, area: String, city: String, user: String, ad: String)

}
