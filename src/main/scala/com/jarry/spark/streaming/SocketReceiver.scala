package com.jarry.spark.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.{ConnectException, Socket}
import java.nio.charset.StandardCharsets

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.slf4j.Logger


/**
  * Created by jarry on 17/1/16.
  */
class SocketReceiver(host:String, port:Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {
    @transient lazy val log = Logger(LoggerFactory.getLogger(this.getClass))

    override def onStart(): Unit = {
        new Thread("Socket Receiver Thread") {
            override def run(): Unit = {receive()}
        }.start()
    }

    override def onStop(): Unit = {

    }

    private def receive(): Unit = {
        var socket:Socket = null
        var line:String = null

        try {
            socket = new Socket(host, port)
            log.info("Connecting to " + host + ":" + port)
            val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))

            while(!this.isStopped() && (line=reader.readLine()) != null) {
                this.store(line)
            }
            reader.close()
            socket.close()

            log.info("Stopped receiving")
            this.restart("Trying to connect again")
        } catch {
//            case e: ConnectException =>
//                restart("Error, connecting to " + host + ":" + port, e)
            case t: Throwable =>
//                restart("Error, receiving data", t)
                  throw new RuntimeException(t)
        }
    }
}
