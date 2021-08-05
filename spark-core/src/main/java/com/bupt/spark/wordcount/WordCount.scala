package com.bupt.spark.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author gml
 * @date 2021/7/26 11:52
 * @version 1.0
 * @param
 * @return
 */
object WordCount {

    def main(args: Array[String]): Unit = {

        //Application
        //TODO 建立依赖关系
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)
        //TODO 执行业务
        val line: RDD[String] = sc.textFile("data")
        //分组
        val words: RDD[String] = line.flatMap(_.split(" "))
        //转换
        val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
        //转换为数量
        val wordToCount: RDD[(String, Int)] = wordGroup.map {
            case (word, list) => {
                (word, list.size)
            }
        }

        val array: Array[(String, Int)] = wordToCount.collect()
        array.foreach(println)


        //TODO 关闭连接
        sc.stop()
        //        val myMap: Map[String, String] = Map("key1" -> "value")
        //        val value1: Option[String] = myMap.get("key1")
        //        val value2: Option[String] = myMap.get("key2")
        //
        //        println(value1) // Some("value1")
        //        println(value2) // None
        //        val sites = Map("runoob" -> "http://www.runoob.com",
        //            "baidu" -> "http://www.baidu.com",
        //            "taobao" -> "http://www.taobao.com")
        //
        //        sites.keys.foreach{i =>
        //            print( "Key = " + i )
        //            println(" Value = " + sites(i) )}

    }

}
