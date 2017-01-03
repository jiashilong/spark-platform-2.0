package com.jarry.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jarry on 17/1/3.
  */
object CogroupApp {
    def main(args: Array[String]):Unit = {
        val names=Array(
            Tuple2(1,"Spark"),
            Tuple2(2,"Hadoop"),
            //Tuple2(3,"Kylin"),
            Tuple2(4,"Flink")
        )
        val types=Array(
            Tuple2(1,"String"),
            Tuple2(2,"int"),
            Tuple2(3,"byte"),
            //Tuple2(4,"bollean"),
            Tuple2(5,"float"),
            Tuple2(1,"34"),
            Tuple2(1,"45"),
            Tuple2(2,"47"),
            Tuple2(3,"75"),
            //Tuple2(4,"95"),
            Tuple2(5,"16"),
            Tuple2(1,"85")
        )

        val conf = new SparkConf()
        val ctx = new SparkContext(conf)
        val distNames = ctx.parallelize(names)
        val distTypes = ctx.parallelize(types)

        val distNameAndTypes = distNames.cogroup(distTypes)

        distNameAndTypes.collect().foreach(println)
        ctx.stop()
    }
}
