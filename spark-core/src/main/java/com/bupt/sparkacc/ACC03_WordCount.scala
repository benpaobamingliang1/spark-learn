package com.bupt.sparkacc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author gml
 * @date 2021/7/29 22:42
 * @version 1.0
 * @param
 * @return
 */
object ACC03_WordCount {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List("Hello", "Spark", "HIve"))

        val myAcc = new MyAccumulator()
        sc.register(myAcc, "wordCount")

        rdd.foreach(
            word => {
                myAcc.add(word)
            }
        )

        println(myAcc.value)
        sc.stop()

    }

    /**
     * 1.继承AccumulatorV2 定义泛型
     * IN
     * OUT
     */
    class MyAccumulator extends AccumulatorV2[String,
            mutable.Map[String, Long]] {

        private var myMap = mutable.Map[String, Long]()

        override def isZero: Boolean = {
            myMap.isEmpty
        }

        override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {

            new MyAccumulator()
        }

        override def reset(): Unit = {
            myMap.clear()
        }

        override def add(word: String): Unit = {
            val newCount = myMap.getOrElse(word, 0L) + 1
            myMap.update(word, newCount)
        }


        override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
            val map1 = this.myMap
            val map2 = other.value

            map2.foreach {
                case (word, count) => {
                    val newCount = map1.getOrElse(word, 0L) + 1
                    map1.update(word, newCount)
                }
            }


        }

        override def value: mutable.Map[String, Long] = {
            myMap
        }
    }

}
