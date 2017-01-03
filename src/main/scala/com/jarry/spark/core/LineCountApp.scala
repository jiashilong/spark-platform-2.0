package com.jarry.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jarry on 16/12/30.
  */
object LineCountApp {
    def main(args: Array[String]):Unit = {
        val conf = new SparkConf()
        val sc = new SparkContext(conf)
        val lines = sc.textFile(args(0))
        val pairs = lines.map((_, 1))
        val counts = pairs.reduceByKey(_ + _)
        counts.collect().foreach(println)
        sc.stop()
    }
}
