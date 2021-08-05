package com.bupt.spark.test

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

/**
 * @author gml
 * @date 2021/7/27 12:26
 * @version 1.0
 * @param
 * @return
 */
object Executor {

    def main(args: Array[String]): Unit = {
        //
        val server = new ServerSocket(8888)

        val client: Socket = server.accept()
        val in: InputStream = client.getInputStream
        val ObjIn = new ObjectInputStream(in)
        val task: SubTask = ObjIn.readObject().asInstanceOf[SubTask]
        val compute: List[Int] = task.compute
        println("计算节点1计算的结果" + compute)
        ObjIn.close()
        client.close()
        server.close()
    }

}
