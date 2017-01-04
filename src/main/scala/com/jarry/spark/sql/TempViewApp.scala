package com.jarry.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by jarry on 17/1/4.
  */
object TempViewApp {
    def main(args: Array[String]):Unit = {
        val spark = SparkSession.builder().getOrCreate()
        val df = spark.read.json("src/main/resources/people.json")
        //df.registerTempTable("t_people")      //deprecated api
        df.createOrReplaceTempView("people")    //new api from 2.0
        spark.sql("select * from people where age > 21").show()
        //临时视图不能跨session使用
        //spark.newSession().sql("select * from people where age > 21").show()
        spark.stop()
    }
}
