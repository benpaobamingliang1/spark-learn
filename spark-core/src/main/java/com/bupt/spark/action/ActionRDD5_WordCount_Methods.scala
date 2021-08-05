package com.bupt.spark.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author gml
 * @date 2021/7/29 11:02
 * @version 1.0
 * @param
 * @return
 */
object ActionRDD5_WordCount_Methods {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        def wordCount1(sc: SparkContext) = {
            val rdd: RDD[String] = sc.makeRDD(List(("Hello Spark"), ("Hello scala")))
            val strings: RDD[String] = rdd.flatMap(_.split(" "))
            val group: RDD[(String, Iterable[String])] = strings.groupBy(word => word)
            val wordCount: RDD[(String, Int)] = group.map(t => (t._1, t._2.size))
        }

        def wordCount2(sc: SparkContext) = {
            val rdd: RDD[String] = sc.makeRDD(List(("Hello Spark"), ("Hello scala")))
            val strings: RDD[String] = rdd.flatMap(_.split(" "))
            val wordOne: RDD[(String, Int)] = strings.map((_, 1))
            val group: RDD[(String, Iterable[Int])] = wordOne.groupByKey()
            val wordCount: RDD[(String, Int)] = group.map(t => (t._1, t._2.size))
        }

        def wordCount3(sc: SparkContext) = {
            val rdd: RDD[String] = sc.makeRDD(List(("Hello Spark"), ("Hello scala")))
            val strings: RDD[String] = rdd.flatMap(_.split(" "))
            val wordOne: RDD[(String, Int)] = strings.map((_, 1))
            val wordCount: RDD[(String, Int)] = wordOne.reduceByKey(_ + _)
        }

        def wordCount4(sc: SparkContext) = {
            val rdd: RDD[String] = sc.makeRDD(List(("Hello Spark"), ("Hello scala")))
            val strings: RDD[String] = rdd.flatMap(_.split(" "))
            val wordOne: RDD[(String, Int)] = strings.map((_, 1))
            val wordCount: RDD[(String, Int)] = wordOne.aggregateByKey(0)(_ + _, _ + _)
        }

        def wordCount5(sc: SparkContext) = {
            val rdd: RDD[String] = sc.makeRDD(List(("Hello Spark"), ("Hello scala")))
            val strings: RDD[String] = rdd.flatMap(_.split(" "))
            val wordOne: RDD[(String, Int)] = strings.map((_, 1))
            val wordCount: RDD[(String, Int)] = wordOne.foldByKey(0)(_ + _)
        }

        def wordCount6(sc: SparkContext) = {
            val rdd: RDD[String] = sc.makeRDD(List(("Hello Spark"), ("Hello scala")))
            val strings: RDD[String] = rdd.flatMap(_.split(" "))
            val wordOne: RDD[(String, Int)] = strings.map((_, 1))
            val wordCount: RDD[(String, Int)] = wordOne.combineByKey(
                v => v,
                (x: Int, y) => {
                    x + y
                },
                (x: Int, y: Int) => {
                    x + y
                }

            )
        }

        def wordCount7(sc: SparkContext) = {
            val rdd: RDD[String] = sc.makeRDD(List(("Hello Spark"), ("Hello scala")))
            val strings: RDD[String] = rdd.flatMap(_.split(" "))
            val wordOne: RDD[(String, Int)] = strings.map((_, 1))
            val wordCount: collection.Map[String, Long] = wordOne.countByKey()
        }

        def wordCount8(sc: SparkContext) = {
            val rdd: RDD[String] = sc.makeRDD(List(("Hello Spark"), ("Hello scala")))
            val strings: RDD[String] = rdd.flatMap(_.split(" "))
            val wordCount: collection.Map[String, Long] = strings.countByValue()
        }

        def wordCount9(sc: SparkContext) = {
            val rdd: RDD[String] = sc.makeRDD(List(("Hello Spark"), ("Hello scala")))
            val strings: RDD[String] = rdd.flatMap(_.split(" "))
            val wordOne: RDD[Map[String, Long]] = strings.map(
                string => {
                    Map[String, Long]((string, 1))
                }
            )
            println(wordOne.reduce(
                (map1, map2) => {
                    map2.foreach {
                        case (word, count) => {
                            var newCount = map1.getOrElse(word, 0L) + count
                            map1.updated(word, newCount)
                        }

                    }
                    map1
                }
            ))
        }

        wordCount9(sc)
    }

}
