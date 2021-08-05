package com.bupt.spark.operation

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author gml
 * @date 2021/7/27 21:37
 * @version 1.0
 * @param
 * @return
 */
object RDDTransaction21_PartitionBy {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        //TODO 计算
        val rdd = sc.makeRDD(List(3, 4, 5, 6))
        val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))
        //PairRDDFunctions
        mapRDD.partitionBy(new HashPartitioner(2))
                .saveAsTextFile("output")
        sc.stop()
    }

}
