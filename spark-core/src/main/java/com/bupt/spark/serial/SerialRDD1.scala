package com.bupt.spark.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author gml
 * @date 2021/7/29 19:40
 * @version 1.0
 * @param
 * @return
 */
object SerialRDD1 {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)


        val rdd: RDD[String] = sc.makeRDD(Array("Hello Spark", "Hello Hive", "Hello Test"))

        val search = new Search("H")

        search.getMatchedRDD1(rdd).collect().foreach(println)
        sc.stop()


    }

    class Search(query: String) extends Serializable {
        def isMatch(s: String) = {
            s.contains(query)
        }

        def getMatchedRDD1(rdd: RDD[String]) = {
            rdd.filter(isMatch)
        }

        def getMatchedRDD2(rdd: RDD[String]) = {
            val q = query
            rdd.filter(_.contains(q))
        }
    }


}
