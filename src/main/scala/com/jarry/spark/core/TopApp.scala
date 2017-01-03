package com.jarry.spark.core

import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by jarry on 17/1/3.
  */
object TopApp {
    def main(args: Array[String]):Unit = {
        val conf = new SparkConf()
        val ctx = new SparkContext(conf)
        val lines = ctx.textFile("src/main/resources/data.txt")
        val pairs = lines.map(_.split("\\s"))
                    .map(line => (line(0), line(1).toInt))
                    .reduceByKey((a,b) => Math.max(a,b))
                    .sortBy(_._2, false)
        val rs = pairs.take(2)
        rs.foreach(println)
    }
}
