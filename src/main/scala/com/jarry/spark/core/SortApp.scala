package com.jarry.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jarry on 16/12/30.
  */
object SortApp {
    def main(args: Array[String]):Unit = {
        val conf = new SparkConf()
        val sc = new SparkContext(conf)
        val cs = sc.textFile(args(0))
            .map(_.replaceAll(",", " "))
            .flatMap(_.split("\\s"))
            .filter(w => ! w.isEmpty)
            .map((_, 1))
            .reduceByKey(_ + _)
        val ss = cs.sortBy(_._2, false)

        ss.take(5).foreach(println)
        sc.stop()
    }
}
