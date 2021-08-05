package com.bupt.sparksroadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author gml
 * @date 2021/7/29 23:34
 * @version 1.0
 * @param
 * @return
 */
object RDDBroadCast {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        //TODO 计算
        val rdd = sc.makeRDD(List(("a", 1), ("a", 3), ("b", 2), ("c", 3)))
        //        val rdd1 = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)))
        //
        //        val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd)
        //        joinRDD.collect().foreach(println)
        val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))

        val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)
        val mapRDD: RDD[(String, (Int, Int))] = rdd.map {
            t => {
                val i: Int = bc.value.getOrElse(t._1, 0)
                (t._1, (t._2, i))
            }
        }
        mapRDD.collect().foreach(println)
        sc.stop()
    }
}
