package com.jarry.spark.streaming

import com.jarry.spark.util.Logging
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext

/**
  * Created by jarry on 17/2/15.
  */
class FileSystemStreamApp(conf: SparkConf, dir:String, interval:Int) extends StreamHandler(conf, interval) with Logging {
    def this(conf: SparkConf, dir:String) {
        this(conf, dir, 10)
    }

    override def handle(ssc: StreamingContext): Unit = {
        val lines = ssc.textFileStream(dir)
        val ws = lines.flatMap(_.split("\\s+"))
                      .map((_, 1))
                      .reduceByKey(_ + _)
        ws.print(1024)
    }
}

object FileSystemStreamApp {
    def main(args: Array[String]):Unit = {
        val Array(dir) = args
        val conf = new SparkConf()

        val app = new FileSystemStreamApp(conf, dir)
        app.run()
    }
}
