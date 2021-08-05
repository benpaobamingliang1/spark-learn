package com.bupt.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @author gml
 * @date 2021/8/1 11:32
 * @version 1.0
 * @param
 * @return
 */
object SparkSqlUDF {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
        val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        //TODO 执行业务操作

        val dataFrame: DataFrame = sparkSession.read.json("data/user.json")

        //dataFrame.createTempView("user")
        //        sparkSession.udf.register("prefixName", (name:String) =>{
        //            "Name:" + name
        //        })
        dataFrame.createTempView("user")
        sparkSession.udf.register("ageAvg", new MyAvgUDAF()
        )

        sparkSession.sql("select ageAvg(age) from user").show

        //TODO 关闭环境
        sparkSession.close()

    }

    case class User(id: Int, name: String, age: Int)

    /**
     * 1.自定义类，计算年林的平均值
     */
    class MyAvgUDAF extends UserDefinedAggregateFunction {
        override def inputSchema: StructType = {
            StructType(
                Array(
                    StructField("age", LongType)
                )
            )
        }

        override def bufferSchema: StructType = {
            StructType(
                Array(
                    StructField("total", LongType),
                    StructField("count", LongType)

                )
            )
        }

        override def dataType: DataType = LongType

        override def deterministic: Boolean = true

        //缓冲区初始化
        override def initialize(buffer: MutableAggregationBuffer): Unit = {
            buffer(0) = 0L
            buffer(1) = 1L
        }

        //根据输入的数值更新缓冲区数据
        override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
            buffer.update(0, buffer.getLong(0) + input.getLong(0))
            buffer.update(1, buffer.getLong(1) + 1)

        }

        override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
            buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
            buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
        }

        override def evaluate(buffer: Row): Any = {
            buffer.getLong(0) / buffer.getLong(1)
        }
    }

}
