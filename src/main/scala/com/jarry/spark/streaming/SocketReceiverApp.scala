package com.jarry.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by jarry on 17/1/20.
  */
object SocketReceiverApp {
    def main(args: Array[String]): Unit = {
        val Array(host, port, interval) = args;
        val conf = new SparkConf()
        val ssc = new StreamingContext(conf, Seconds(interval.toInt))

        val lines = ssc.receiverStream(new SocketReceiver(host, port.toInt))
        lines.print()

        ssc.start()
        ssc.awaitTermination()
        ssc.stop()
    }
}
