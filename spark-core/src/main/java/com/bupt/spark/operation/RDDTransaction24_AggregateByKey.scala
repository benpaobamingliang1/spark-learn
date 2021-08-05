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
object RDDTransaction24_AggregateByKey {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        //TODO 计算
        val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))

        val aggRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(
            (x, y) => {
                math.max(x, y)
            },
            (x, y) => {
                x + y
            }
        )

        aggRDD.collect().foreach(println)
        sc.stop()
    }

}
