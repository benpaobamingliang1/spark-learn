package com.bupt.Project.SparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayOps

/**
 * @author gml
 * @date 2021/7/30 10:57
 * @version 1.0
 * @param
 * @return
 */
object Requeirment_1_2 {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.textFile(
            "data/user_visit_action.txt")

        rdd.cache()

        /**
         * 1.统计品类的点击数量
         * 2.统计品类的下单数量
         * 3.统计品类的支付数量
         * 4.将品类进行分类，并且取前十名
         */
        //将相同的品类ID的数据进行分组聚合
        val flatMapRDD: RDD[(String, (Int, Int, Int))] = rdd.flatMap(
            t => {
                val strings: ArrayOps.ofRef[String] = t.split("_")
                if (strings(6) != "-1") {
                    List((strings(6), (1, 0, 0)))
                } else if (strings(8) != "null") {
                    val id: Array[String] = strings(8).split(",")
                    id.map(
                        t => {
                            (t, (0, 1, 0))
                        }
                    )
                } else if (strings(10) != "null") {
                    val id: Array[String] = strings(10).split(",")
                    id.map(
                        t => {
                            (t, (0, 0, 1))
                        }
                    )
                } else {
                    Nil
                }

            }
        )
        val anysisRDD: RDD[(String, (Int, Int, Int))] = flatMapRDD.reduceByKey(
            (t1, t2) => {
                (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
            }
        )


        val resultRDD: Array[(String, (Int, Int, Int))] = anysisRDD.sortBy(_._2, false).take(10)

        resultRDD.foreach(println)

    }

}
