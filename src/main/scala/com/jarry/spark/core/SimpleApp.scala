package com.jarry.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jarry on 16/12/29.
  */
object SimpleApp {
    def main(args: Array[String]):Unit = {
        val logFile = args(0);
        val conf = new SparkConf().setAppName("SimpleApp").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val data = sc.textFile(logFile, 2).cache()
        val as = data.filter(_.contains("a")).count()
        val bs = data.filter(_.contains("b")).count()

        println(s"Lines with a: $as, Lines with b: $bs")

        sc.stop();

    }
}
