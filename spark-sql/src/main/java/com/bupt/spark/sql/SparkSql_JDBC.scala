package com.bupt.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @author gml
 * @date 2021/8/1 11:32
 * @version 1.0
 * @param
 * @return
 */
object SparkSql_JDBC {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        //TODO 执行业务操作

        val df: DataFrame = sparkSession.read
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/wangpan")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("user", "root")
                .option("password", "123456")
                .option("dbtable", "user")
                .load()
        df.show

        //保存
        df.write
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/wangpan")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("user", "root")
                .option("password", "123456")
                .option("dbtable", "user1")
                .mode(SaveMode.Append)
                .save()
        df.show

        //TODO 关闭环境
        sparkSession.close()

    }


}
