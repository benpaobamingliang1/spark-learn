package com.bupt.spark.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @author gml
 * @date 2021/7/29 21:27
 * @version 1.0
 * @param
 * @return
 */
object RDDPartitoner1 {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)


        var rdd = sc.makeRDD(List(
            ("nba", "NAB nihao"),
            ("cba", "NAB nihao"),
            ("wnba", "NAB nihao"),
            ("nba", "WNAB nihao"),
        ), 3)


        val partRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)

        partRDD.saveAsTextFile("output")


        sc.stop()
    }

    class MyPartitioner extends Partitioner {

        //分区的数量
        override def numPartitions: Int = 3

        //返回数据所在的分区索引
        override def getPartition(key: Any): Int = {
            key match {
                case "nba" => {
                    0
                }
                case "cba" => {
                    1
                }
                case "wnba" => {
                    2
                }
            }

        }

    }

}
