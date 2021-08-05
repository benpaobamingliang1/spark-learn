package com.bupt.framework.service

import com.bupt.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

/**
 * @author gml
 * @date 2021/7/30 21:38
 * @version 1.0
 * @param
 * @return
 */
class WordCountService {

    private val wordCountDao = new WordCountDao()

    def dataAnysis() = {
        //TODO 执行业务
        val line: RDD[String] = wordCountDao.readFile("data/1.txt")
        //分组
        val words: RDD[String] = line.flatMap {
            _.split(" ")
        }
        //转换
        val wordToOne: RDD[(String, Int)] = words.map((_, 1))

        val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)

        wordToCount.collect()
    }
}
