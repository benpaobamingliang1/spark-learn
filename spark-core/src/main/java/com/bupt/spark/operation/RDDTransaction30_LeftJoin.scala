package com.bupt.spark.operation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author gml
 * @date 2021/7/27 21:37
 * @version 1.0
 * @param
 * @return
 */
object RDDTransaction30_LeftJoin {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        //TODO 计算
        val rdd = sc.makeRDD(List(("a", 1), ("b", 2)))
        val rdd1 = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)))

        val leftJoin: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd)

        leftJoin.collect().foreach(println)


        val rightJoin: RDD[(String, (Option[Int], Int))] = rdd1.rightOuterJoin(rdd)
        rightJoin.collect().foreach(println)
        sc.stop()
    }

}
