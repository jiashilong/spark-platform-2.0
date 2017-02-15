package com.jarry.spark.core

import com.jarry.spark.util.Logging
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jarry on 16/12/29.
  */
object SimpleApp extends Logging {
    def main(args: Array[String]):Unit = {
        val logFile = "src/main/resources/data.txt"
        val conf = new SparkConf().setAppName("SimpleApp").setMaster("local")
        val sc = new SparkContext(conf)
        val data = sc.textFile(logFile, 2).cache()
        val as = data.filter(_.contains("a")).count()
        val bs = data.filter(_.contains("b")).count()

        logInfo(s"Lines with a: $as, Lines with b: $bs")

        sc.stop();

    }
}
