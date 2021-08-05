package com.bupt.Project.SparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayOps

/**
 * @author gml
 * @date 2021/7/30 10:57
 * @version 1.0
 * @param
 * @return
 */
object Requeirment_1_3 {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.textFile(
            "data/user_visit_action.txt")

        rdd.cache()

        /**
         * 1.统计品类的点击数量
         * 2.统计品类的下单数量
         * 3.统计品类的支付数量
         * 4.将品类进行分类，并且取前十名
         */
        //将相同的品类ID的数据进行分组聚合

        val acc = new HotCategoryAccumulator()

        sc.register(acc, "HotCategory")
        val accMapRDD = rdd.foreach(
            t => {
                val strings: ArrayOps.ofRef[String] = t.split("_")
                if (strings(6) != "-1") {
                    acc.add(strings(6), "click")
                } else if (strings(8) != "null") {
                    val id: Array[String] = strings(8).split(",")
                    id.foreach(
                        t => {
                            acc.add(t, "order")
                        }
                    )
                } else if (strings(10) != "null") {
                    val id: Array[String] = strings(10).split(",")
                    id.foreach(
                        t => {
                            acc.add(t, "pay")
                        }
                    )
                }

            }
        )
        val accVal: mutable.Map[String, HotCategory] = acc.value

        val categories: mutable.Iterable[HotCategory] = accVal.map(_._2)


        val sort = categories.toList.sortWith(
            (left, right) => {
                if (left.clickCnt > right.clickCnt) {
                    true
                } else if (left.clickCnt == right.clickCnt) {
                    if (left.orderCnt > right.orderCnt) {
                        true
                    } else if (left.orderCnt == right.orderCnt) {
                        left.payCnt > right.payCnt
                    }
                    else {
                        false
                    }
                } else {
                    false
                }
            }
        )
        sort.take(10).foreach(println)

        sc.stop()
    }

    case class HotCategory(cid: String, var clickCnt: Int, var orderCnt: Int, var payCnt: Int)

    class HotCategoryAccumulator extends AccumulatorV2[(String, String),
            mutable.Map[String, HotCategory]] {

        private val hcMap = mutable.Map[String, HotCategory]()

        override def isZero: Boolean = {
            hcMap.isEmpty
        }

        override def copy(): AccumulatorV2[(String, String),
                mutable.Map[String, HotCategory]] = {
            new HotCategoryAccumulator()
        }

        override def reset(): Unit = {
            hcMap.clear()
        }

        override def add(v: (String, String)): Unit = {
            val cid: String = v._1
            val actionType: String = v._2
            val category: HotCategory = hcMap.getOrElse(cid,
                HotCategory(cid, 0, 0, 0))
            if (actionType == "click") {
                category.clickCnt += 1
            }
            else if (actionType == "order") {
                category.orderCnt += 1
            } else if (actionType == "pay") {
                category.payCnt += 1
            }
            hcMap.update(cid, category)
        }

        override def merge(other: AccumulatorV2[(String, String)
                , mutable.Map[String, HotCategory]]): Unit = {
            val map1: mutable.Map[String, HotCategory] = this.hcMap
            val map2: mutable.Map[String, HotCategory] = other.value
            map2.foreach({
                case (cid, hc) => {
                    val category: HotCategory = hcMap.getOrElse(cid,
                        HotCategory(cid, 0, 0, 0))
                    category.clickCnt += hc.clickCnt
                    category.orderCnt += hc.orderCnt
                    category.payCnt += hc.payCnt
                    map1.update(cid, category)
                }

            })
        }

        override def value: mutable.Map[String, HotCategory] = {
            hcMap
        }
    }

}
