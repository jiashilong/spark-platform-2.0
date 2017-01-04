package com.jarry.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by jarry on 17/1/4.
  */
object GlobalViewApp {
    def main(args: Array[String]):Unit = {
        val spark = SparkSession.builder().getOrCreate()
        val df = spark.read.json("src/main/resources/people.json")
        // Global temporary view is tied to a system preserved database `global_temp`
        // 全局视图注册在global_temp数据库中,使用时必须指定数据库。
        df.createGlobalTempView("v_people")         //new api from 2.10
        spark.sql("select * from global_temp.v_people").show()
        spark.newSession().sql("select * from global_temp.v_people").show()
        spark.close()       //new api from 2.10
    }
}
