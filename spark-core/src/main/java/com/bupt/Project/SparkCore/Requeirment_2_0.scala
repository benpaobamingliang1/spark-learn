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
object Requeirment_2_0 {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.textFile(
            "data/user_visit_action.txt")
        rdd.cache()
        val top10Ids: Array[String] = top10Category(rdd)
        val filterRDD: RDD[String] = rdd.filter(
            t => {
                val strings: Array[String] = t.split("_")
                if (strings(6) != "-1") {
                    top10Ids.contains(strings(6))
                } else {
                    false
                }
            }
        )
        val reduceRDD: RDD[((String, String), Int)] = filterRDD.map(
            t => {
                val strings: Array[String] = t.split("_")
                ((strings(6), strings(2)), 1)
            }
        ).reduceByKey(_ + _)

        val mapRDD: RDD[(String, (String, Int))] = reduceRDD.map(
            t => {
                (t._1._1, (t._1._2, t._2))
            }
        )

        //相同品类排序
        val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()

        //进行点击量的统计
        val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
            t => {
                t.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
            }
        )
        resultRDD.foreach(println)

        sc.stop()

    }

    def top10Category(rdd: RDD[String]) = {
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


        anysisRDD.sortBy(_._2, false).take(10).map(_._1)

    }

}
