package com.bupt.spark.operation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author gml
 * @date 2021/7/27 21:37
 * @version 1.0
 * @param
 * @return
 */
object RDDTransaction28_ByKey {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        //TODO 计算
        val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))

        rdd.reduceByKey(_ + _)
        rdd.aggregateByKey(0)(_ + _, _ + _)
        rdd.foldByKey(0)(_ + _)
        rdd.combineByKey(v => v, (x: Int, y: Int) => x + y, (x: Int, y: Int) => x + y)
        //.collect().foreach(println)
        sc.stop()
    }

}
