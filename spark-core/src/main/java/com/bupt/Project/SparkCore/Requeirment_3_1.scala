package com.bupt.Project.SparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author gml
 * @date 2021/7/30 10:57
 * @version 1.0
 * @param
 * @return
 */
object Requeirment_3_1 {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.textFile(
            "data/user_visit_action.txt")
        rdd.cache()

        val sumRDD: RDD[UserVisitAction] = rdd.map(
            t => {
                val strings: Array[String] = t.split("_")
                UserVisitAction(
                    strings(0),
                    strings(1).toLong,
                    strings(2),
                    strings(3).toLong,
                    strings(4),
                    strings(5),
                    strings(6).toLong,
                    strings(7).toLong,
                    strings(8),
                    strings(9),
                    strings(10),
                    strings(11),
                    strings(12).toLong
                )
            }
        )
        sumRDD.cache()
        //分母
        var ids = List[Long](1, 2, 3, 4, 5, 6, 7)

        val trueFenZI: List[(Long, Long)] = ids.zip(ids.tail)
        val fenMuCountMap: Map[Long, Long] = sumRDD.filter(
            t => {
                ids.init.contains(t.page_id)
            }
        ).map(
            t => {
                (t.page_id, 1L)
            }
        ).reduceByKey(_ + _).collect().toMap

        //计算分子
        val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = sumRDD.groupBy(t => t.session_id)

        val mvRDD: RDD[(String, List[((Long, Long), Int)])] = sessionRDD.mapValues(
            t => {
                val sortList: List[UserVisitAction] = t.toList.sortBy(_.action_time)

                val flowIds: List[Long] = sortList.map(
                    t => {
                        t.page_id
                    }
                )
                val pageflowIds: List[(Long, Long)] = flowIds.zip(flowIds.tail)

                //将不合法的页面去除
                pageflowIds.filter(
                    t => {
                        trueFenZI.contains(t)
                    }
                ).map(
                    t => {
                        (t, 1)
                    }
                )
            }
        )

        val flatRDD: RDD[((Long, Long), Int)] = mvRDD.map(_._2).flatMap(list => list)

        val fenZiRDD: RDD[((Long, Long), Int)] = flatRDD.reduceByKey(_ + _)

        fenZiRDD.foreach(
            t => {
                val fenMu: Long = fenMuCountMap.getOrElse(t._1._1, 0L)
                println(s"页面${t._1._1}跳转到${t._1._2}单跳转换率为" + t._2.toDouble / fenMu)
            }
        )

        sc.stop()
    }

    //用户访问动作表
    case class UserVisitAction(
                                      date: String, //用户点击行为的日期
                                      user_id: Long, //用户的 ID
                                      session_id: String, //Session 的 ID
                                      page_id: Long, //某个页面的 ID
                                      action_time: String, //动作的时间点
                                      search_keyword: String, //用户搜索的关键词
                                      click_category_id: Long, //某一个商品品类的 ID
                                      click_product_id: Long, //某一个商品的 ID
                                      order_category_ids: String, //一次订单中所有品类的 ID 集合
                                      order_product_ids: String, //一次订单中所有商品的 ID 集合
                                      pay_category_ids: String, //一次支付中所有品类的 ID 集合
                                      pay_product_ids: String, //一次支付中所有商品的 ID 集合
                                      city_id: Long
                              ) //城市 id
}
