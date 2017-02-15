package com.jarry.spark.core

import com.jarry.spark.util.Logging
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jarry on 17/2/15.
  */
class FileSystemReadApp(val sc: SparkContext,  path:String) extends Logging {
    def read(): Unit = {
        val lines = sc.textFile(path)
        lines.collect().foreach(println)
    }
}

object FileSystemReadApp {
    def main(args: Array[String]):Unit = {
        val conf = new SparkConf()
        val sc = new SparkContext(conf)
        val Array(path) = args

        val app = new FileSystemReadApp(sc, path)
        app.read()
    }
}
