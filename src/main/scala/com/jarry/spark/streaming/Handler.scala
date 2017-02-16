package com.jarry.spark.streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by jarry on 17/2/16.
  */
abstract class Handler(conf: SparkConf, val interval:Int) {
    def handle(ssc: StreamingContext)

    def run(): Unit = {
        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(interval))

        handle(ssc)

        ssc.start()
        ssc.awaitTermination()
        ssc.stop()
    }
}
