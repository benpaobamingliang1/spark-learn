package com.bupt.spark.test

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

/**
 * @author gml
 * @date 2021/7/27 12:26
 * @version 1.0
 * @param
 * @return
 */
object Driver {

    def main(args: Array[String]): Unit = {
        val client = new Socket("localhost", 8888)
        val client2 = new Socket("localhost", 9999)
        val task = new Task()
        val out: OutputStream = client.getOutputStream
        val objectOutPut = new ObjectOutputStream(out)

        val subTask = new SubTask()
        subTask.logic = task.logic
        subTask.list = task.list.take(2)

        objectOutPut.writeObject(subTask)
        objectOutPut.flush()
        objectOutPut.close()
        client.close()


        val out2: OutputStream = client2.getOutputStream
        val objectOutPut2 = new ObjectOutputStream(out2)

        val subTask2 = new SubTask()
        subTask2.logic = task.logic
        subTask2.list = task.list.takeRight(2)

        objectOutPut2.writeObject(subTask2)
        objectOutPut2.flush()
        objectOutPut2.close()
        client2.close()
        println("数据输入完毕")
    }
}
