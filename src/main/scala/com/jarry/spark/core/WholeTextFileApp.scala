package com.jarry.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jarry on 17/2/16.
  */
object WholeTextFileApp {
    def main(args: Array[String]):Unit = {
        val conf = new SparkConf()
        val sc = new SparkContext(conf)

        val data = sc.wholeTextFiles(args(0))
        for(d <- data.collect()) {
            println(d._1 + "<------>" + d._2)
        }

        sc.stop()
    }
}
