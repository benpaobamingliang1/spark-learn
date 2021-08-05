package com.bupt.spark.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author gml
 * @date 2021/7/29 20:55
 * @version 1.0
 * @param
 * @return
 */
object RDDPersist5_ALL {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sc = new SparkContext(sparkConf)

        /**
         * 1）Cache 缓存只是将数据保存起来，不切断血缘依赖。Checkpoint 检查点切断血缘依赖。
         * 2）Cache 缓存的数据通常存储在磁盘、内存等地方，可靠性低。Checkpoint 的数据通常存
         * 储在 HDFS 等容错、高可用的文件系统，可靠性高。
         * 3）建议对 checkpoint()的 RDD 使用 Cache 缓存，这样 checkpoint 的 job 只需从 Cache 缓存
         * 中读取数据即可，否则需要再从头计算一次 RDD。
         */
        sc.setCheckpointDir("cp")
        val rdd: RDD[String] = sc.makeRDD(List(("Hello Spark"), ("Hello scala")))

        val strings: RDD[String] = rdd.flatMap(_.split(" "))

        val wordOne: RDD[(String, Int)] = strings.map(
            word => {
                println("===========")
                (word, 1)
            }
        )

        wordOne.cache()
        wordOne.checkpoint()

        //wordOne.persist(StorageLevel.DISK_ONLY)
        val wordCount: RDD[(String, Int)] = wordOne.reduceByKey(_ + _)
        //相当于重新执行
        wordCount.collect().foreach(println)
        println("+++++++++++++++++++++++++")
        val wordCount1: RDD[(String, Iterable[Int])] = wordOne.groupByKey()

        wordCount1.collect().foreach(println)

        sc.stop()
    }
}
