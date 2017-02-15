package com.jarry.spark.streaming

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.{ConnectException, Socket}
import java.nio.charset.StandardCharsets

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.slf4j.Logger
import com.jarry.spark.util.NextIterator

import scala.util.control.NonFatal


/**
  * Created by jarry on 17/1/16.
  */
class SocketReceiver(host:String, port:Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {
    @transient lazy val log = Logger(LoggerFactory.getLogger(this.getClass))
    private var socket: Socket = _

    override def onStart(): Unit = {
        log.info(s"Connecting to $host:$port")
        try {
            socket = new Socket(host, port)
        } catch {
            case e: ConnectException => {
                restart(s"Restart, Error connect to $host:$port", e)
                log.error(s"Error connect to $host:$port, Restart")
                return
            }
        }

        val t = new Thread("Socket Receiver"){
            setDaemon(true)
            override def run(): Unit = {
                receive()
            }
        }
        t.start()

    }

    override def onStop(): Unit = {
        // in case restart thread close it twice
        synchronized {
            if(socket != null) {
                socket.close()
                socket = null
                log.info(s"Closed socket from $host:$port")
            }
        }
    }

    private def bytesToLines(inputStream: InputStream): Iterator[String] = {
        val dataInputStream = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))

        return new NextIterator[String] {
            override protected def getNext(): String = {
                val nextValue = dataInputStream.readLine()
                if(nextValue == null) {finished = true}
                return nextValue
            }

            override protected def close(): Unit = {
                if(dataInputStream != null) {dataInputStream.close()}
            }
        }
    }

    private def receive(): Unit = {
        try {
            val it = bytesToLines(socket.getInputStream)
            while(!isStopped() && it.hasNext) {
                store(it.next())
            }

            if(!isStopped()) {
                restart("Restart, Socket data stream had no more data")
                log.info("Socket data stream had no more data, Restart")
            } else {
                log.info("Stopped receiving")
            }
        } catch {
            case NonFatal(e) => {
                log.error("Restart, Error receiving data", e)
                restart("Restart, Error receiving data", e)
            }
        } finally {
            onStop()
        }
    }
}
