package com.jarry.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by jarry on 17/1/10.
  */
object NetworkWordCountApp {
    def main(args: Array[String]): Unit = {
        val Array(host, port, interval) = args;
        val conf = new SparkConf()
        val ssc = new StreamingContext(conf, Seconds(interval.toInt))
        val lines = ssc.socketTextStream(host, port.toInt)

        val ws = lines.flatMap(_.split("\\s+"))
                      .map((_, 1))
                      .reduceByKey(_ + _)

        ws.print()

        ssc.start()
        ssc.awaitTermination()
        ssc.stop()
    }
}
