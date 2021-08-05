package com.bupt.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author gml
 * @date 2021/8/1 11:32
 * @version 1.0
 * @param
 * @return
 */
object SparkSqlBasic {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        import sparkSession.implicits._
        //TODO 执行业务操作
        //DataFrame
        val df: DataFrame = sparkSession.read.json("data/user.json")

        df.show()
        //1.1.DataFrame =>SQL
        //        df.createOrReplaceTempView("user")
        //        sparkSession.sql("select * from user").show
        //        sparkSession.sql("select age,username from user").show
        //        sparkSession.sql("select avg(age) from user").show

        //1.2.DataFrame =>DSL

        //        df.select("age", "username").show
        //        df.select($"age" + 1).show
        //2.DataSet

        val seq: Seq[Int] = Seq(1, 2, 3, 4)
        val ds: Dataset[Int] = seq.toDS()

        ds.show()
        //RDD <>DataFrame
        val rdd = sparkSession.sparkContext.
                makeRDD(List((1, "zhangsan", 30), (2, "lisi", 20)))
        val dataFrame: DataFrame = rdd.toDF("id", "username", "age")
        val rowRDD: RDD[Row] = dataFrame.rdd
        //DataFrame <>DataSet

        val dataSet: Dataset[User] = dataFrame.as[User]

        val dataFrame1: DataFrame = ds.toDF()

        //RDD <>DataSet
        val dataSet1: Dataset[User] = rdd.map {
            case (id, name, age) => {
                User(id, name, age)
            }
        }.toDS()
        val rdd1: RDD[User] = dataSet1.rdd
        //TODO 关闭环境
        sparkSession.close()

    }

    case class User(id: Int, name: String, age: Int)

}
