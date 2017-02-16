package com.jarry.spark.streaming

import com.jarry.spark.util.Logging
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by jarry on 17/2/15.
  */
class FileSystemStreamApp(val sc:SparkContext, val dir:String, val interval:Int) extends Logging {

    def run(): Unit = {
        val ssc = new StreamingContext(sc, Seconds(interval))
        val lines = ssc.textFileStream(dir)
        lines.print(1024)

        ssc.start()
        ssc.awaitTermination()
        ssc.stop()
    }
}

object FileSystemStreamApp {
    def main(args: Array[String]):Unit = {
        val Array(dir, interval) = args
        val conf = new SparkConf()
        val sc = new SparkContext(conf)

        val app = new FileSystemStreamApp(sc, dir, interval.toInt)
        app.run()
    }
}
